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

package org.apache.flink.table.planner.plan.accelerator;

import org.apache.flink.table.accelerator.AccelCall;
import org.apache.flink.table.accelerator.AccelExpression;
import org.apache.flink.table.accelerator.AccelFunction;
import org.apache.flink.table.accelerator.AccelInputRef;
import org.apache.flink.table.accelerator.AccelLiteral;
import org.apache.flink.table.accelerator.AccelWorkProfile;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Converts the planner's {@code RexNode} trees into accelerator IR.
 *
 * <p>This class is the only place Calcite and the IR meet, and it lives in the planner for that
 * reason: {@code flink-table-common} holds the IR and must not learn about the optimizer.
 *
 * <h2>The mapping is the type check</h2>
 *
 * <p>An operator becomes an {@link AccelFunction} only if it appears in {@link #FUNCTIONS}, which
 * is keyed on {@link SqlKind} and on the identity of Flink's own built-in functions — never on a
 * name string. A catalog function called {@code SQRT} therefore has no mapping and the conversion
 * fails, rather than being rendered as the builtin and quietly returning a different answer.
 *
 * <p>Failure to convert is an ordinary outcome: it means this subtree is not offloadable, which is
 * what an empty {@link Optional} says.
 */
public final class AccelIrBuilder {

    /** Operators recognised by kind. */
    private static final Map<SqlKind, AccelFunction> BY_KIND = new HashMap<>();

    /**
     * Operators recognised by the name of a Flink built-in.
     *
     * <p>Built-ins reach the planner as {@code OTHER_FUNCTION} with no distinguishing kind, so the
     * name is the only handle Calcite offers. The safety this loses is bought back by {@link
     * #isBuiltIn}: a name is accepted only when the operator is a built-in rather than something
     * a user registered under the same name.
     */
    private static final Map<String, AccelFunction> BY_BUILT_IN_NAME = new HashMap<>();

    static {
        BY_KIND.put(SqlKind.PLUS, AccelFunction.PLUS);
        BY_KIND.put(SqlKind.MINUS, AccelFunction.MINUS);
        BY_KIND.put(SqlKind.TIMES, AccelFunction.TIMES);
        BY_KIND.put(SqlKind.DIVIDE, AccelFunction.DIVIDE);
        BY_KIND.put(SqlKind.MINUS_PREFIX, AccelFunction.NEGATE);
        BY_KIND.put(SqlKind.EQUALS, AccelFunction.EQUALS);
        BY_KIND.put(SqlKind.NOT_EQUALS, AccelFunction.NOT_EQUALS);
        BY_KIND.put(SqlKind.LESS_THAN, AccelFunction.LESS_THAN);
        BY_KIND.put(SqlKind.LESS_THAN_OR_EQUAL, AccelFunction.LESS_OR_EQUAL);
        BY_KIND.put(SqlKind.GREATER_THAN, AccelFunction.GREATER_THAN);
        BY_KIND.put(SqlKind.GREATER_THAN_OR_EQUAL, AccelFunction.GREATER_OR_EQUAL);
        BY_KIND.put(SqlKind.AND, AccelFunction.AND);
        BY_KIND.put(SqlKind.OR, AccelFunction.OR);
        BY_KIND.put(SqlKind.NOT, AccelFunction.NOT);
        BY_KIND.put(SqlKind.CAST, AccelFunction.CAST);
        BY_KIND.put(SqlKind.FLOOR, AccelFunction.FLOOR);
        BY_KIND.put(SqlKind.CEIL, AccelFunction.CEIL);
        BY_KIND.put(SqlKind.LEAST, AccelFunction.LEAST);
        BY_KIND.put(SqlKind.GREATEST, AccelFunction.GREATEST);

        BY_BUILT_IN_NAME.put("ABS", AccelFunction.ABS);
        BY_BUILT_IN_NAME.put("SQRT", AccelFunction.SQRT);
        BY_BUILT_IN_NAME.put("EXP", AccelFunction.EXP);
        BY_BUILT_IN_NAME.put("LN", AccelFunction.LN);
        BY_BUILT_IN_NAME.put("LOG2", AccelFunction.LOG2);
        BY_BUILT_IN_NAME.put("POWER", AccelFunction.POWER);
        BY_BUILT_IN_NAME.put("SIN", AccelFunction.SIN);
        BY_BUILT_IN_NAME.put("COS", AccelFunction.COS);
        BY_BUILT_IN_NAME.put("TAN", AccelFunction.TAN);
        BY_BUILT_IN_NAME.put("ASIN", AccelFunction.ASIN);
        BY_BUILT_IN_NAME.put("ACOS", AccelFunction.ACOS);
        BY_BUILT_IN_NAME.put("ATAN", AccelFunction.ATAN);
        BY_BUILT_IN_NAME.put("ATAN2", AccelFunction.ATAN2);
        BY_BUILT_IN_NAME.put("TANH", AccelFunction.TANH);
        BY_BUILT_IN_NAME.put("SIGN", AccelFunction.SIGN);
    }

    private AccelIrBuilder() {}

    /** Converts one expression, or empty when any part of it has no IR representation. */
    public static Optional<AccelExpression> convert(RexNode node) {
        if (node instanceof RexInputRef) {
            RexInputRef ref = (RexInputRef) node;
            return logicalType(node).map(type -> new AccelInputRef(ref.getIndex(), type));
        }
        if (node instanceof RexLiteral) {
            return convertLiteral((RexLiteral) node);
        }
        if (!(node instanceof RexCall)) {
            return Optional.empty();
        }
        RexCall call = (RexCall) node;
        AccelFunction function = functionOf(call);
        if (function == null) {
            return Optional.empty();
        }
        List<AccelExpression> operands = new ArrayList<>(call.getOperands().size());
        for (RexNode operand : call.getOperands()) {
            Optional<AccelExpression> converted = convert(operand);
            if (!converted.isPresent()) {
                return Optional.empty();
            }
            operands.add(converted.get());
        }
        return logicalType(call).map(type -> new AccelCall(function, operands, type));
    }

    /**
     * Whether this call has an IR representation at all.
     *
     * <p>The single admission list. Before this existed the vocabulary was stated three times --
     * in the cost estimator's weight table, in the kernel generator's switch, and implicitly in
     * whatever the runtime could execute -- and nothing kept them in step.
     */
    public static boolean supports(RexCall call) {
        return functionOf(call) != null;
    }

    /** Counts the work a set of expressions performs on each input row. */
    public static AccelWorkProfile profile(
            List<AccelExpression> expressions,
            int inputBytesPerRow,
            int outputBytesPerRow,
            long estimatedRows) {
        int[] counts = new int[AccelFunction.values().length];
        for (AccelExpression expression : expressions) {
            count(expression, counts);
        }
        return new AccelWorkProfile(counts, inputBytesPerRow, outputBytesPerRow, estimatedRows);
    }

    private static void count(AccelExpression expression, int[] counts) {
        if (expression instanceof AccelCall) {
            AccelCall call = (AccelCall) expression;
            counts[call.function().ordinal()]++;
            for (AccelExpression operand : call.operands()) {
                count(operand, counts);
            }
        }
    }

    private static @Nullable AccelFunction functionOf(RexCall call) {
        AccelFunction byKind = BY_KIND.get(call.getKind());
        if (byKind != null) {
            return byKind;
        }
        if (!isBuiltIn(call)) {
            // A user function may share a built-in's name. Refusing here is what stops it being
            // costed and executed as the built-in it is impersonating.
            return null;
        }
        return BY_BUILT_IN_NAME.get(call.getOperator().getName().toUpperCase(java.util.Locale.ROOT));
    }

    /**
     * Whether the call's operator is one Flink or Calcite defines, rather than one a user
     * registered.
     *
     * <p>This is the check that makes a name safe to use. A catalog function reaches the planner
     * as a {@link BridgingSqlFunction} whose definition is <em>not</em> a {@link
     * BuiltInFunctionDefinition}; Flink's own functions arrive the same way but with one that is.
     * Anything straight out of Calcite's operator table is a built-in by construction.
     *
     * <p>Without it, a user function registered as {@code SQRT} would be converted to {@link
     * AccelFunction#SQRT} and executed as the builtin — a wrong answer from a correct-looking
     * query, and review finding A3.
     */
    private static boolean isBuiltIn(RexCall call) {
        SqlOperator operator = call.getOperator();
        if (operator instanceof BridgingSqlFunction) {
            return ((BridgingSqlFunction) operator).getDefinition()
                    instanceof BuiltInFunctionDefinition;
        }
        return operator.getClass().getName().startsWith("org.apache.calcite.sql");
    }

    private static Optional<AccelExpression> convertLiteral(RexLiteral literal) {
        Optional<LogicalType> type = logicalType(literal);
        if (!type.isPresent()) {
            return Optional.empty();
        }
        Object value = literal.getValue3();
        Serializable box;
        if (value == null) {
            box = null;
        } else if (value instanceof BigDecimal) {
            box = ((BigDecimal) value).doubleValue();
        } else if (value instanceof Number) {
            box = ((Number) value).doubleValue();
        } else if (value instanceof Boolean) {
            box = (Boolean) value;
        } else {
            return Optional.empty();
        }
        return Optional.of(new AccelLiteral(box, type.get()));
    }

    private static Optional<LogicalType> logicalType(RexNode node) {
        try {
            return Optional.of(FlinkTypeFactory.toLogicalType(node.getType()));
        } catch (RuntimeException unsupported) {
            // A type the planner can name but Flink's logical type system cannot carry.
            return Optional.empty();
        }
    }
}
