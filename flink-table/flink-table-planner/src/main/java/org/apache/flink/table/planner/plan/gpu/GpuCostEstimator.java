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
 * of one device, and this one ran inside Flink where it could not be right for every accelerator. A
 * provider applies its own weights to these counts.
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
 * the order of magnitude a provider's own comparison resolves, and because the current kernel
 * library does no CSE either, so the estimate matches what actually executes.
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
     * <p>Delegates to {@link #estimateAll} so a single expression and a list of them count the same
     * way. The visitor below decides eligibility; it deliberately does not count, because counting
     * is the IR's job and two independent counters would drift.
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
                return RowCost.ineligible("no accelerator IR representation for " + node);
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
     *
     * <p>A nullable column costs one byte more (M2.9). Validity has to cross to the device with the
     * values, and although a provider is free to pack it far tighter than a byte — a bit a column a
     * row is the obvious encoding — charging something rather than nothing keeps the ratio honest
     * for an expression over many nullable columns, which is the case where it stops being a
     * rounding error.
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
                                if (ref.getType().isNullable()) {
                                    bytes[0] += 1;
                                }
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

        if (isSingleWidthFloat(call.getType())) {
            // Flink evaluates FLOAT arithmetic in float and rounds at every step. A kernel that
            // computes the whole expression in double and narrows once at the end rounds once, and
            // the two are not the same number.
            //
            // They do agree for a single operation, which is why this was easy to miss: double has
            // 53 bits of mantissa against float's 24, comfortably past the 2p+2 that makes a
            // double-rounded single operation exact. It is a chain that diverges -- (a+b)+c rounds
            // twice on the CPU and once on the device -- and a chain is what any expression worth
            // offloading is.
            //
            // Refused rather than fixed. Generating float arithmetic throughout is a real option
            // and the IR can express it; it is also new vocabulary for the kernel generator, which
            // this project does not add without a measurement justifying it, and nothing measured
            // so far says FLOAT expressions matter. The reason names the fix the user can apply.
            return RowCost.ineligible(
                    "FLOAT result from "
                            + call.getOperator().getName()
                            + ": Flink rounds float arithmetic at every step and a device kernel"
                            + " would round once, so the two would disagree; compute in DOUBLE to"
                            + " make this expression offloadable");
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
            // Nullable operands used to be refused here, and are not any more (M2.9).
            //
            // The refusal was correct while a kernel had no way to say "this value is not there",
            // and it cost nearly everything: measured over Flink's own test DDLs, 95.4% of numeric
            // columns are nullable, 93% of tables declare no NOT NULL numeric column at all, and
            // nothing in a query rescues one -- not IS NOT NULL, not a predicate implying it, not
            // a subquery -- because Calcite does not narrow a column's declared type through a
            // filter. Only the DDL, which a query author reading a catalog table cannot change.
            //
            // The IR could always express this: outputType() is a Flink LogicalType and carries
            // isNullable(). So the subtree now reaches the provider with the nullability intact and
            // the provider decides. One that cannot represent a null declines in accept(), the
            // code-generated operator runs, and the answer is the same as it was -- which is what
            // makes relaxing this safe before any kernel can act on it.
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
     * Whether this is Flink's 32-bit floating type, under either of the two names it answers to.
     *
     * <p>Calcite's {@code FLOAT} and {@code REAL} are both 4-byte in Flink, and which one a plan
     * carries depends on how the type arrived. Checking one and not the other would leave half the
     * cases eligible.
     */
    private static boolean isSingleWidthFloat(RelDataType type) {
        SqlTypeName name = type.getSqlTypeName();
        return name == SqlTypeName.FLOAT || name == SqlTypeName.REAL;
    }

    /**
     * Only casts between the supported numeric types are allowed, and only where the target holds
     * the source. A narrowing cast has overflow semantics the kernel does not implement, and SQL
     * requires an error rather than a wrap — which a device kernel cannot raise (the accelerator
     * runtime has no exception support).
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
