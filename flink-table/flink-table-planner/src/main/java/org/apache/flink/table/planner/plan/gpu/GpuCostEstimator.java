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

import org.apache.flink.table.accelerator.AccelExpression;
import org.apache.flink.table.accelerator.AccelWorkProfile;
import org.apache.flink.table.planner.plan.accelerator.AccelIrBuilder;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;

/**
 * Decides, at plan time, whether an expression can run as a device kernel and how much arithmetic
 * it does per row.
 *
 * <p><b>Nothing is executed.</b> This is a static walk over the {@code RexNode} tree the planner
 * already holds after optimisation, before any {@code Transformation} is built and long before a
 * record is read. Work is <em>counted</em> per operation, never weighted: a weight table is a model
 * of one device, and this one ran inside Flink where it could not be right for every accelerator.
 * A provider applies its own weights to these counts.
 *
 * <p>Type eligibility is necessary and nowhere near sufficient: {@code val * 2.0 + 1.0} is
 * trivially expressible and measures 0.54x against CPU. Deciding that it is not worth moving
 * requires knowing the device, so it happens on the TaskManager rather than here.
 *
 * <h2>Common subexpressions are counted more than once</h2>
 *
 * <p>{@code BatchPhysicalCalc} builds its expressions with {@code RexProgram.expandLocalRef}, which
 * inlines shared subexpressions, so {@code SQRT(x) + SQRT(x)} arrives as two {@code SQRT} nodes and
 * is costed as two. A kernel generator that recognised the common subexpression would do one. The
 * estimate therefore runs slightly high on expression-sharing queries, which biases toward
 * offloading — the less safe direction. It is left uncorrected because the effect is small next to
 * the order of magnitude a provider's own comparison resolves, and because the current kernel library does no
 * CSE either, so the estimate matches what actually executes.
 *
 * <h2>Rejection is the safe direction</h2>
 *
 * <p>Every unrecognised node, operator or type is refused. A false rejection costs a missed
 * speedup; a false acceptance produces either a wrong answer or a regression, and both are worse.
 * Rejection reasons are carried in the {@link RowCost} so they can be surfaced in {@code EXPLAIN}
 * rather than leaving a user wondering why a query did not offload.
 */
public final class GpuCostEstimator implements RexVisitor<RowCost> {

    /**
     * Types the staging buffers and kernels handle. Deliberately narrow.
     *
     * <p>{@code DECIMAL} is {@code DecimalData}, an object; {@code VARCHAR} is variable-length
     * {@code BinaryStringData} backed by a memory segment. Neither has a fixed-width columnar
     * representation the gather can produce, so both are excluded rather than costed.
     */
    private static boolean isSupportedType(RelDataType type) {
        switch (type.getSqlTypeName()) {
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case REAL:
            case BOOLEAN:
                return true;
            default:
                return false;
        }
    }

    /**
     * Costs one expression tree.
     *
     * <p>Delegates to {@link #estimateAll} so a single expression and a list of them count the
     * same way. The visitor below decides eligibility; it deliberately does not count, because
     * counting is the IR's job and two independent counters would drift.
     */
    public static RowCost estimate(RexNode node) {
        return estimateAll(java.util.Collections.singletonList(node), false);
    }

    /**
     * Costs a set of expressions — a Calc's projections plus its condition — as one unit.
     *
     * <p>Each expression's own result type is checked here, because that is what the staging
     * buffers must be able to hold. Interior types are checked as the tree is walked; leaf literals
     * are not (see {@link #visitLiteral}).
     */
    public static RowCost estimateAll(List<? extends RexNode> nodes) {
        return estimateAll(nodes, true);
    }

    private static RowCost estimateAll(List<? extends RexNode> nodes, boolean checkOutputTypes) {
        for (RexNode node : nodes) {
            if (checkOutputTypes && !isSupportedType(node.getType())) {
                return RowCost.ineligible("unsupported output type " + node.getType());
            }
            RowCost admitted = node.accept(new GpuCostEstimator());
            if (!admitted.isEligible()) {
                return admitted;
            }
        }
        // Eligibility is settled above; this second pass counts. Two walks, each with one job:
        // the refusal rules stay readable and the counting cannot silently disagree with what the
        // IR admits, because it is the IR that does the counting.
        List<AccelExpression> ir = new java.util.ArrayList<>(nodes.size());
        for (RexNode node : nodes) {
            java.util.Optional<AccelExpression> converted = AccelIrBuilder.convert(node);
            if (!converted.isPresent()) {
                return RowCost.ineligible(
                        "no accelerator IR representation for " + node);
            }
            ir.add(converted.get());
        }
        AccelWorkProfile work =
                AccelIrBuilder.profile(
                        ir, bytesPerRow(nodes), bytesPerRow(nodes), AccelWorkProfile.UNKNOWN_ROWS);
        return RowCost.of(work);
    }

    /**
     * Width of the columns a set of expressions reads, in bytes per row.
     *
     * <p>Approximate by design: it charges each distinct referenced column once at its declared
     * width. What matters to a provider is the ratio of this to the operation counts — arithmetic
     * intensity — and that ratio is not sensitive to a byte here or there.
     */
    private static int bytesPerRow(List<? extends RexNode> nodes) {
        java.util.Set<Integer> seen = new java.util.HashSet<>();
        int[] bytes = {0};
        for (RexNode node : nodes) {
            node.accept(
                    new org.apache.calcite.rex.RexVisitorImpl<Void>(true) {
                        @Override
                        public Void visitInputRef(RexInputRef ref) {
                            if (seen.add(ref.getIndex())) {
                                bytes[0] += widthOf(ref.getType().getSqlTypeName());
                            }
                            return null;
                        }
                    });
        }
        return bytes[0];
    }

    private static int widthOf(SqlTypeName type) {
        switch (type) {
            case INTEGER:
            case FLOAT:
            case REAL:
                return 4;
            case BOOLEAN:
                return 1;
            default:
                return 8;
        }
    }

    @Override
    public RowCost visitInputRef(RexInputRef ref) {
        // Nullability is deliberately NOT checked here. A bare top-level RexInputRef is a
        // projected-through column: it is staged host-side and never crosses to the device, so its
        // nullability is the pass-through buffer's business (task 0.1a), not the kernel's.
        // Nullable *expression inputs* are refused in visitCall, where the operand walk is.
        return isSupportedType(ref.getType())
                ? RowCost.ZERO
                : RowCost.ineligible("unsupported column type " + ref.getType());
    }

    @Override
    public RowCost visitLiteral(RexLiteral literal) {
        // A literal becomes a kernel scalar argument, so it costs nothing per row.
        //
        // Its own type is deliberately NOT checked. Calcite parses an unsuffixed SQL decimal such
        // as `2.0` or `0.5` as DECIMAL(2,1) even inside an all-DOUBLE expression, so rejecting
        // literals by type refuses `val * 2.0 + 1.0 WHERE val > 0.5` -- which is to say, nearly
        // every query anyone writes. The literal's value is folded into the kernel as a scalar
        // argument of the enclosing call's type, and that call's result type is checked in
        // visitCall, as is each top-level expression's in estimateAll. A DECIMAL literal reaching
        // a context that stays DECIMAL is therefore still rejected -- by the surrounding type,
        // which is the thing that actually determines what the kernel computes.
        //
        // Where the enclosing type is DOUBLE, Flink's own generated code narrows the literal the
        // same way, so matching it is agreement with the CPU path rather than a new rounding risk.
        return RowCost.ZERO;
    }

    @Override
    public RowCost visitCall(RexCall call) {
        if (!isSupportedType(call.getType())) {
            return RowCost.ineligible(
                    "unsupported result type "
                            + call.getType()
                            + " from "
                            + call.getOperator().getName());
        }

        if (!AccelIrBuilder.supports(call)) {
            return RowCost.ineligible(
                    "no accelerator IR for operator " + call.getOperator().getName());
        }
        if (call.getKind() == SqlKind.CAST && !isSafeCast(call)) {
            return RowCost.ineligible("unsupported cast to " + call.getType());
        }
        RowCost total = RowCost.ZERO;
        for (RexNode operand : call.getOperands()) {
            // BIGINT is admitted as a column but refused as an operand, matching what the
            // generator will accept. Staging is DoubleArray, and a long beyond 2^53 does not
            // survive that round trip, so an expression over one would quietly disagree with the
            // CPU plan for large keys. Projecting such a column through is still fine: it never
            // reaches the device. Costing it here as eligible and having generation refuse it
            // later showed up in EXPLAIN as an unexplained fallback.
            // A staged column crosses to the device as a bare primitive. There is no null
            // representation in the buffer and no three-valued logic in the generated kernel, so a
            // null would arrive as 0.0 -- which looks like data -- and be computed on.
            //
            // What the refusal costs is more than it sounds: Calcite does not narrow nullability
            // through a filter, so `SELECT f(x) FROM t WHERE x IS NOT NULL` still types x as
            // nullable and is refused even though a null is impossible there. Conservative is the
            // safe direction, and the reason names the fix rather than leaving the user guessing.
            if (operand.getType().isNullable()) {
                return RowCost.ineligible(
                        "nullable operand of "
                                + call.getOperator().getName()
                                + " (type "
                                + operand.getType()
                                + "): a device kernel has no null representation, so declare the "
                                + "column NOT NULL to make this expression offloadable");
            }
            if (operand.getType().getSqlTypeName() == SqlTypeName.BIGINT) {
                return RowCost.ineligible(
                        "BIGINT operand of "
                                + call.getOperator().getName()
                                + ": values beyond 2^53 would not survive the double staging "
                                + "buffers, so the device could disagree with the CPU plan");
            }
            total = total.plus(operand.accept(this));
            if (!total.isEligible()) {
                return total;
            }
        }
        return total;
    }

    /**
     * Only casts between the supported numeric types are allowed, and only where the target holds
     * the source. A narrowing cast has overflow semantics the kernel does not implement, and SQL
     * requires an error rather than a wrap — which a device kernel cannot raise (TornadoVM has no
     * exception support).
     */
    private static boolean isSafeCast(RexCall call) {
        SqlTypeName from = call.getOperands().get(0).getType().getSqlTypeName();
        SqlTypeName to = call.getType().getSqlTypeName();
        if (from == to) {
            return true;
        }
        switch (from) {
            case INTEGER:
                return to == SqlTypeName.BIGINT
                        || to == SqlTypeName.DOUBLE
                        || to == SqlTypeName.FLOAT
                        || to == SqlTypeName.REAL;
            case BIGINT:
                return to == SqlTypeName.DOUBLE;
            case FLOAT:
            case REAL:
                return to == SqlTypeName.DOUBLE;
            default:
                return false;
        }
    }

    // --- Everything below has no device representation. ---

    @Override
    public RowCost visitLocalRef(RexLocalRef ref) {
        // Calc uses RexLocalRef into a common sub-expression program. Supporting it means costing
        // the program rather than the tree, which is a separate piece of work; refuse for now
        // rather than silently costing a reference as free.
        return RowCost.ineligible("RexLocalRef (Calc program form) not yet costed");
    }

    @Override
    public RowCost visitOver(RexOver over) {
        return RowCost.ineligible("window function " + over.getOperator().getName());
    }

    @Override
    public RowCost visitCorrelVariable(RexCorrelVariable v) {
        return RowCost.ineligible("correlation variable");
    }

    @Override
    public RowCost visitDynamicParam(RexDynamicParam p) {
        return RowCost.ineligible("dynamic parameter");
    }

    @Override
    public RowCost visitRangeRef(RexRangeRef r) {
        return RowCost.ineligible("range reference");
    }

    @Override
    public RowCost visitFieldAccess(RexFieldAccess a) {
        return RowCost.ineligible("nested field access");
    }

    @Override
    public RowCost visitSubQuery(RexSubQuery q) {
        return RowCost.ineligible("subquery");
    }

    @Override
    public RowCost visitTableInputRef(RexTableInputRef r) {
        return RowCost.ineligible("table input reference");
    }

    @Override
    public RowCost visitPatternFieldRef(RexPatternFieldRef r) {
        return RowCost.ineligible("MATCH_RECOGNIZE pattern reference");
    }
}
