/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.plan.gpu;

import org.apache.flink.table.runtime.gpu.GpuKernelSource;
import org.apache.flink.table.runtime.gpu.GpuValueType;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

/**
 * Generates a TornadoVM kernel, as Java source, from a Calc's {@code RexNode} expressions.
 *
 * <p>The generated method is compiled at run time and named to TornadoVM through {@code
 * TaskGraph.task(String, Method, Object...)}. Before that overload existed a kernel had to be a
 * method that already existed at compile time, which forced a fixed catalogue of shapes; this
 * removes that limit.
 *
 * <h2>Storage follows the column, arithmetic does not</h2>
 *
 * <p>Each staged column is held at its declared width — {@code IntArray}, {@code FloatArray} or
 * {@code DoubleArray} — and widened to {@code double} on entry to the loop body, so the expression
 * is evaluated in double whatever it reads. Only the buffer and the transfer follow the column.
 *
 * <p>This used to stage everything as {@code DoubleArray}, justified by {@code INTEGER} and {@code
 * FLOAT} both being exact in a double. They are; the argument was still wrong, because it was about
 * values and the gather is about layout. Reading a four-byte field with {@code getDouble} reads
 * eight bytes: an {@code INT} column holding 42 reached the device as 2.08e-322. A {@code FLOAT}
 * result had the mirror-image problem on the way out, boxing a {@code Double} into a field the row
 * type declares {@code FLOAT}.
 *
 * <p>The width is also worth having on its own: a {@code FLOAT} or {@code INT} column now moves
 * four bytes per row instead of eight, and every workload measured so far is bound by moving its
 * input.
 *
 * <p><b>{@code BIGINT} is deliberately excluded as an expression input.</b> Values beyond 2^53 do
 * not survive the round trip through a double, so admitting it would silently return different
 * answers from the CPU plan for large keys. A {@code BIGINT} column may still be selected: a
 * projected-through column never reaches the device and keeps its exact type.
 *
 * <h2>Filtering does not compact</h2>
 *
 * <p>A condition becomes a 0/1 mask written alongside the projections rather than a compaction.
 * Compacting on the device needs a prefix sum and a scatter — two more kernels and another buffer —
 * whereas the host has to walk the results anyway to build output rows.
 */
public final class GpuKernelGenerator {

    /**
     * Words that are legal Java identifiers but reserved in OpenCL C.
     *
     * <p>TornadoVM translates the generated Java into OpenCL, so a method or variable named after
     * an OpenCL keyword fails late, inside the sketcher, with {@code "Java method name corresponds
     * to an OpenCL Token"}. Generated names are checked against this list rather than left to
     * produce that error.
     */
    private static final Set<String> OPENCL_RESERVED =
            new HashSet<>(
                    Arrays.asList(
                            "kernel",
                            "global",
                            "local",
                            "constant",
                            "private",
                            "read_only",
                            "write_only",
                            "read_write",
                            "uniform",
                            "pipe",
                            "half",
                            "quad",
                            "complex",
                            "imaginary",
                            "generic"));

    private static final String INDENT = "        ";

    /**
     * Mirrors {@code GpuCalcSpec.COMPUTED}. Duplicated rather than imported so the generator has no
     * reason to reach into the runtime module beyond the value object it produces.
     */
    private static final int OUTPUT_COMPUTED = -1;

    private GpuKernelGenerator() {}

    /**
     * Generates a kernel for the given expressions, or empty if any of them cannot be expressed.
     *
     * <p>Returning empty is an ordinary outcome. {@link GpuCostEstimator} decides whether an
     * expression is <em>worth</em> offloading; this decides whether it can be <em>written</em> as a
     * kernel, and the two are independent.
     *
     * @param projection the Calc's projections, already expanded from program form
     * @param condition the Calc's condition, or null
     * @param classNameSuffix appended to the generated class name to keep it unique per query
     */
    public static Optional<GpuKernelSource> generate(
            List<RexNode> projection, @Nullable RexNode condition, String classNameSuffix) {
        return generate(projection, condition, classNameSuffix, 0);
    }

    /**
     * As above, optionally writing every computed column into one buffer.
     *
     * <p>{@code packedStride} rows apart, column-major, so column <em>j</em> of row <em>i</em> is
     * at {@code j * packedStride + i}. Used by aggregate fusion, where a buffer per column would
     * mean a reduction task per column and a graph TornadoVM refuses to build past about a hundred
     * tasks. Zero keeps the per-column arrays the Calc operator expects.
     */
    public static Optional<GpuKernelSource> generate(
            List<RexNode> projection,
            @Nullable RexNode condition,
            String classNameSuffix,
            int packedStride) {

        // Only referenced columns are staged; a projected-through column stays on the host.
        final Map<Integer, String> inputs = new LinkedHashMap<>();
        // Filled by render() as columns are registered, so it stays in step with inputs.
        final Map<Integer, GpuValueType> inputTypes = new LinkedHashMap<>();
        final List<String> computed = new ArrayList<>();
        final List<GpuValueType> outputTypes = new ArrayList<>();
        final int[] layout = new int[projection.size()];

        for (int field = 0; field < projection.size(); field++) {
            RexNode expr = projection.get(field);
            if (expr instanceof RexInputRef) {
                layout[field] = ((RexInputRef) expr).getIndex();
                continue;
            }
            layout[field] = OUTPUT_COMPUTED;
            if (!isDoubleResult(expr)) {
                return Optional.empty();
            }
            String rendered = render(expr, inputs, inputTypes);
            if (rendered == null) {
                return Optional.empty();
            }
            computed.add(rendered);
            outputTypes.add(valueTypeOf(expr.getType().getSqlTypeName()));
        }
        if (computed.isEmpty()) {
            // Nothing to compute; the CPU path already handles pure projection.
            return Optional.empty();
        }

        String renderedCondition = null;
        if (condition != null) {
            renderedCondition = render(condition, inputs, inputTypes);
            if (renderedCondition == null) {
                return Optional.empty();
            }
        }
        if (inputs.isEmpty()) {
            // A kernel with no input column has nothing to size the parallel loop by.
            return Optional.empty();
        }

        String className = "GpuCalcKernel$" + classNameSuffix;
        String methodName = "evaluate";
        if (OPENCL_RESERVED.contains(methodName.toLowerCase(Locale.ROOT))) {
            throw new IllegalStateException(
                    "generated method name collides with an OpenCL keyword");
        }

        int[] inputFields = inputs.keySet().stream().mapToInt(Integer::intValue).toArray();
        GpuValueType[] stagedTypes = new GpuValueType[inputFields.length];
        for (int i = 0; i < inputFields.length; i++) {
            stagedTypes[i] = inputTypes.get(inputFields[i]);
        }
        if (packedStride > 0) {
            for (GpuValueType type : outputTypes) {
                if (type != GpuValueType.DOUBLE) {
                    // One buffer means one element type, and the contraction that reads it
                    // accumulates in double. A narrower column would need its own buffer again.
                    return Optional.empty();
                }
            }
        }
        String source =
                renderClass(
                        className,
                        methodName,
                        inputs,
                        inputTypes,
                        computed,
                        outputTypes,
                        renderedCondition,
                        packedStride);
        return Optional.of(
                new GpuKernelSource(
                        className,
                        methodName,
                        source,
                        inputFields,
                        stagedTypes,
                        outputTypes.toArray(new GpuValueType[0]),
                        renderedCondition != null,
                        layout,
                        packedStride));
    }

    private static String renderClass(
            String className,
            String methodName,
            Map<Integer, String> inputs,
            Map<Integer, GpuValueType> inputTypes,
            List<String> computed,
            List<GpuValueType> outputTypes,
            @Nullable String condition,
            int packedStride) {

        // Only the array classes this kernel actually mentions, so the generated source names
        // nothing it does not use. IntArray is separate: a filter needs it for the mask whether or
        // not any column is an integer.
        Set<String> arrayTypes = new TreeSet<>();
        for (GpuValueType type : inputTypes.values()) {
            arrayTypes.add(type.arrayType());
        }
        if (packedStride > 0) {
            arrayTypes.add(GpuValueType.DOUBLE.arrayType());
        } else {
            for (GpuValueType type : outputTypes) {
                arrayTypes.add(type.arrayType());
            }
        }
        if (condition != null) {
            arrayTypes.add(GpuValueType.INT.arrayType());
        }

        StringBuilder sb = new StringBuilder();
        sb.append("import uk.ac.manchester.tornado.api.annotations.Parallel;\n");
        sb.append("import uk.ac.manchester.tornado.api.math.TornadoMath;\n");
        for (String arrayType : arrayTypes) {
            sb.append("import uk.ac.manchester.tornado.api.types.arrays.")
                    .append(arrayType)
                    .append(";\n");
        }
        sb.append("\n");
        sb.append("public final class ").append(className).append(" {\n\n");
        sb.append("    public static void ").append(methodName).append("(");

        List<String> params = new ArrayList<>();
        for (Map.Entry<Integer, String> input : inputs.entrySet()) {
            params.add(inputTypes.get(input.getKey()).arrayType() + " " + input.getValue() + "_in");
        }
        if (packedStride > 0) {
            params.add("DoubleArray out");
        } else {
            for (int i = 0; i < computed.size(); i++) {
                params.add(outputTypes.get(i).arrayType() + " out" + i);
            }
        }
        if (condition != null) {
            params.add("IntArray mask");
        }
        sb.append(String.join(", ", params)).append(") {\n");

        String first = inputs.values().iterator().next();
        sb.append("        for (@Parallel int i = 0; i < ")
                .append(first)
                .append("_in.getSize(); i++) {\n");
        // Widened to double on read, so the expression below is evaluated exactly as it was when
        // every column was staged as a double. Only the buffer changed.
        for (String var : inputs.values()) {
            sb.append(INDENT)
                    .append("    double ")
                    .append(var)
                    .append(" = ")
                    .append(var)
                    .append("_in.get(i);\n");
        }
        for (int i = 0; i < computed.size(); i++) {
            if (packedStride > 0) {
                // Column-major into one buffer. The stride is a literal rather than a parameter
                // because the batch size is known here, and a constant is one less thing for the
                // device compiler to be given at run time.
                sb.append(INDENT)
                        .append("    out.set(")
                        .append(i * packedStride)
                        .append(" + i, ")
                        .append(computed.get(i))
                        .append(");\n");
                continue;
            }
            // A FLOAT result is narrowed here rather than on the host, so the value that crosses
            // the bus is the one the row type declares and the buffer is half the size.
            boolean narrow = outputTypes.get(i) == GpuValueType.FLOAT;
            sb.append(INDENT)
                    .append("    out")
                    .append(i)
                    .append(".set(i, ")
                    .append(narrow ? "(float) (" : "")
                    .append(computed.get(i))
                    .append(narrow ? ")" : "")
                    .append(");\n");
        }
        if (condition != null) {
            // Written as if/else rather than a ternary: both compile, but this keeps the generated
            // OpenCL readable under -Dtornado.printKernel=True.
            sb.append(INDENT).append("    if (").append(condition).append(") {\n");
            sb.append(INDENT).append("        mask.set(i, 1);\n");
            sb.append(INDENT).append("    } else {\n");
            sb.append(INDENT).append("        mask.set(i, 0);\n");
            sb.append(INDENT).append("    }\n");
        }
        sb.append("        }\n");
        sb.append("    }\n");
        sb.append("}\n");
        return sb.toString();
    }

    /** Renders one expression, registering any column it reads. Null if it cannot be expressed. */
    private static @Nullable String render(
            RexNode node, Map<Integer, String> inputs, Map<Integer, GpuValueType> inputTypes) {
        if (node instanceof RexInputRef) {
            RexInputRef ref = (RexInputRef) node;
            SqlTypeName sqlType = ref.getType().getSqlTypeName();
            if (!isDoubleSafeInput(sqlType)) {
                return null;
            }
            inputTypes.putIfAbsent(ref.getIndex(), valueTypeOf(sqlType));
            return inputs.computeIfAbsent(ref.getIndex(), index -> "c" + index);
        }
        if (node instanceof RexLiteral) {
            return renderLiteral((RexLiteral) node);
        }
        if (!(node instanceof RexCall)) {
            return null;
        }
        RexCall call = (RexCall) node;
        List<String> operands = new ArrayList<>();
        for (RexNode operand : call.getOperands()) {
            String rendered = render(operand, inputs, inputTypes);
            if (rendered == null) {
                return null;
            }
            operands.add(rendered);
        }
        return renderCall(call, operands);
    }

    private static @Nullable String renderCall(RexCall call, List<String> operands) {
        switch (call.getKind()) {
            case PLUS:
                return infix(operands, "+");
            case MINUS:
                return infix(operands, "-");
            case TIMES:
                return infix(operands, "*");
            case DIVIDE:
                return infix(operands, "/");
            case MINUS_PREFIX:
                return operands.size() == 1 ? "(-" + operands.get(0) + ")" : null;
            case GREATER_THAN:
                return infix(operands, ">");
            case GREATER_THAN_OR_EQUAL:
                return infix(operands, ">=");
            case LESS_THAN:
                return infix(operands, "<");
            case LESS_THAN_OR_EQUAL:
                return infix(operands, "<=");
            case EQUALS:
                return infix(operands, "==");
            case NOT_EQUALS:
                return infix(operands, "!=");
            case AND:
                return infix(operands, "&&");
            case OR:
                return infix(operands, "||");
            case NOT:
                return operands.size() == 1 ? "(!" + operands.get(0) + ")" : null;
            case CAST:
                // Every value on the device is already a double, so a widening numeric cast is a
                // no-op. Narrowing casts must raise on overflow and a kernel cannot, so they are
                // refused here as they are in the cost estimator.
                return operands.size() == 1 && isDoubleResult(call) ? operands.get(0) : null;
            default:
                return renderFunction(call, operands);
        }
    }

    /** Maps a named SQL function onto {@code TornadoMath}, which is what compiles to the device. */
    private static @Nullable String renderFunction(RexCall call, List<String> operands) {
        String name = call.getOperator().getName().toUpperCase(Locale.ROOT);

        // LEAST and GREATEST take any number of arguments; TornadoMath.min and max take two. Fold
        // left, so LEAST(a, b, c) becomes min(min(a, b), c). Kept out of the switch below because
        // that one pairs a name with a fixed arity.
        if ("LEAST".equals(name) || "GREATEST".equals(name)) {
            if (operands.size() < 2 || !isDoubleResult(call)) {
                return null;
            }
            String reduce = "LEAST".equals(name) ? "min" : "max";
            String folded = operands.get(0);
            for (int i = 1; i < operands.size(); i++) {
                folded = "TornadoMath." + reduce + "(" + folded + ", " + operands.get(i) + ")";
            }
            return folded;
        }

        String fn;
        int arity = 1;
        switch (name) {
            case "ABS":
                fn = "abs";
                break;
            case "SQRT":
                fn = "sqrt";
                break;
            case "EXP":
                fn = "exp";
                break;
            case "LN":
                fn = "log";
                break;
            case "LOG2":
                fn = "log2";
                break;
            case "SIN":
                fn = "sin";
                break;
            case "COS":
                fn = "cos";
                break;
            case "TAN":
                fn = "tan";
                break;
            case "ASIN":
                fn = "asin";
                break;
            case "ACOS":
                fn = "acos";
                break;
            case "ATAN":
                fn = "atan";
                break;
            case "TANH":
                fn = "tanh";
                break;
            case "FLOOR":
                fn = "floor";
                break;
            case "CEIL":
                fn = "ceil";
                break;
            case "SIGN":
                fn = "signum";
                break;
            case "POWER":
                fn = "pow";
                arity = 2;
                break;
            case "ATAN2":
                fn = "atan2";
                arity = 2;
                break;
            default:
                return null;
        }
        if (operands.size() != arity) {
            return null;
        }
        return "TornadoMath." + fn + "(" + String.join(", ", operands) + ")";
    }

    private static @Nullable String infix(List<String> operands, String op) {
        if (operands.size() < 2) {
            return null;
        }
        // Fully parenthesised: the tree already encodes precedence, so nothing is left to Java's.
        return "(" + String.join(" " + op + " ", operands) + ")";
    }

    private static @Nullable String renderLiteral(RexLiteral literal) {
        if (literal.getType().getSqlTypeName() == SqlTypeName.BOOLEAN) {
            Object value = literal.getValue3();
            return value instanceof Boolean ? value.toString() : null;
        }
        Object value = literal.getValue3();
        double d;
        if (value instanceof BigDecimal) {
            d = ((BigDecimal) value).doubleValue();
        } else if (value instanceof Number) {
            d = ((Number) value).doubleValue();
        } else {
            return null;
        }
        if (!Double.isFinite(d)) {
            return null;
        }
        // Double.toString round-trips exactly, so the constant the kernel sees is the constant the
        // CPU plan folded.
        return Double.toString(d);
    }

    /**
     * Input types that survive a round trip through a double unchanged.
     *
     * <p>{@code BIGINT} is absent on purpose; see the class comment.
     */
    /**
     * The width a column of this type is staged at.
     *
     * <p>Only called for types {@link #isDoubleSafeInput} or {@link #isDoubleResult} has already
     * admitted, so the fall-through is unreachable rather than a default.
     */
    private static GpuValueType valueTypeOf(SqlTypeName type) {
        switch (type) {
            case INTEGER:
                return GpuValueType.INT;
            case FLOAT:
            case REAL:
                return GpuValueType.FLOAT;
            case DOUBLE:
                return GpuValueType.DOUBLE;
            default:
                throw new IllegalStateException("no staging width for " + type);
        }
    }

    private static boolean isDoubleSafeInput(SqlTypeName type) {
        return type == SqlTypeName.DOUBLE
                || type == SqlTypeName.FLOAT
                || type == SqlTypeName.REAL
                || type == SqlTypeName.INTEGER;
    }

    private static boolean isDoubleResult(RexNode node) {
        SqlTypeName type = node.getType().getSqlTypeName();
        return type == SqlTypeName.DOUBLE || type == SqlTypeName.FLOAT || type == SqlTypeName.REAL;
    }
}
