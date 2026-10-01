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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.accelerator.AccelAggCall;
import org.apache.flink.table.accelerator.AccelAggFunction;
import org.apache.flink.table.accelerator.AccelAggregate;
import org.apache.flink.table.accelerator.AccelCall;
import org.apache.flink.table.accelerator.AccelExpression;
import org.apache.flink.table.accelerator.AccelFilter;
import org.apache.flink.table.accelerator.AccelFunction;
import org.apache.flink.table.accelerator.AccelInput;
import org.apache.flink.table.accelerator.AccelInputRef;
import org.apache.flink.table.accelerator.AccelJoin;
import org.apache.flink.table.accelerator.AccelLiteral;
import org.apache.flink.table.accelerator.AccelNode;
import org.apache.flink.table.accelerator.AccelOverAggregate;
import org.apache.flink.table.accelerator.AccelProject;
import org.apache.flink.table.accelerator.AccelScan;
import org.apache.flink.table.accelerator.AccelSort;
import org.apache.flink.table.accelerator.AccelWorkProfile;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.DecimalDataUtils;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.apache.flink.table.planner.plan.gpu.GpuJoinSupport;
import org.apache.flink.table.planner.plan.gpu.GpuOverAggregateSupport;
import org.apache.flink.table.planner.plan.nodes.exec.spec.JoinSpec;
import org.apache.flink.table.planner.plan.nodes.exec.spec.OverSpec;
import org.apache.flink.table.planner.plan.nodes.exec.spec.SortSpec;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

/**
 * Converts the planner's {@code RexNode} trees into accelerator IR.
 *
 * <p>This class is the only place Calcite and the IR meet, and it lives in the planner for that
 * reason: {@code flink-table-common} holds the IR and must not learn about the optimizer.
 *
 * <h2>The mapping is the type check</h2>
 *
 * <p>An operator becomes an {@link AccelFunction} only if it appears in one of three maps, keyed on
 * {@link SqlKind}, on the identity of a Calcite operator, or on the identity of a Flink function
 * definition — never on a name string. A catalog function called {@code SQRT} is a different object
 * from {@code SqlStdOperatorTable.SQRT}, so it simply has no mapping and the conversion fails,
 * rather than being rendered as the builtin and quietly returning a different answer.
 *
 * <p>That used to take an explicit guard — recognise the impostor, then trust the name. Keying on
 * identity removes the impostor as a category: there is nothing to recognise, because only the
 * built-in objects are in the maps.
 *
 * <p>Failure to convert is an ordinary outcome: it means this subtree is not offloadable, which is
 * what an empty {@link Optional} says.
 */
public final class AccelIrBuilder {

    /** Operators recognised by kind. */
    private static final Map<SqlKind, AccelFunction> BY_KIND = new HashMap<>();

    /**
     * Calcite operators, keyed on the operator object itself.
     *
     * <p>An {@link IdentityHashMap} states the intent; {@link org.apache.calcite.sql.SqlOperator}
     * does not override {@code equals}, so a {@code HashMap} would behave the same and say less.
     */
    private static final Map<SqlOperator, AccelFunction> BY_CALCITE_OPERATOR =
            new IdentityHashMap<>();

    /** Flink function definitions, keyed on the definition object itself. */
    private static final Map<FunctionDefinition, AccelFunction> BY_DEFINITION =
            new IdentityHashMap<>();

    static {
        BY_KIND.put(SqlKind.PLUS, AccelFunction.PLUS);
        BY_KIND.put(SqlKind.MINUS, AccelFunction.MINUS);
        BY_KIND.put(SqlKind.TIMES, AccelFunction.TIMES);
        BY_KIND.put(SqlKind.DIVIDE, AccelFunction.DIVIDE);
        BY_KIND.put(SqlKind.MOD, AccelFunction.MOD);
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
        // The two natural operations on a validity bit. Trivial to serve once a provider carries
        // validity and impossible before, which is why they arrive with V2 rather than V1.
        BY_KIND.put(SqlKind.IS_NULL, AccelFunction.IS_NULL);
        BY_KIND.put(SqlKind.IS_NOT_NULL, AccelFunction.IS_NOT_NULL);
        BY_KIND.put(SqlKind.FLOOR, AccelFunction.FLOOR);
        BY_KIND.put(SqlKind.CEIL, AccelFunction.CEIL);
        BY_KIND.put(SqlKind.LEAST, AccelFunction.LEAST);
        BY_KIND.put(SqlKind.GREATEST, AccelFunction.GREATEST);

        // Identity, not name. Every entry below is the very operator object the planner puts in
        // the RexCall, so a match means "this is that function", not "this is called that".
        //
        // Which table an operator comes from is not guessable and was measured rather than
        // assumed: most arrive as Calcite's own, LEAST and GREATEST as Flink function definitions
        // behind a bridging operator, and LOG2 and TANH from Flink's Calcite-side table, which is
        // neither. Getting one wrong costs nothing visible -- the function is simply never
        // offloaded again -- which is exactly how the earlier bug survived, so
        // AccelIrBuilderTest walks this map and checks every entry still resolves.
        BY_CALCITE_OPERATOR.put(SqlStdOperatorTable.ABS, AccelFunction.ABS);
        BY_CALCITE_OPERATOR.put(SqlStdOperatorTable.SQRT, AccelFunction.SQRT);
        BY_CALCITE_OPERATOR.put(SqlStdOperatorTable.EXP, AccelFunction.EXP);
        BY_CALCITE_OPERATOR.put(SqlStdOperatorTable.LN, AccelFunction.LN);
        BY_CALCITE_OPERATOR.put(SqlStdOperatorTable.POWER, AccelFunction.POWER);
        BY_CALCITE_OPERATOR.put(SqlStdOperatorTable.SIN, AccelFunction.SIN);
        BY_CALCITE_OPERATOR.put(SqlStdOperatorTable.COS, AccelFunction.COS);
        BY_CALCITE_OPERATOR.put(SqlStdOperatorTable.TAN, AccelFunction.TAN);
        BY_CALCITE_OPERATOR.put(SqlStdOperatorTable.ASIN, AccelFunction.ASIN);
        BY_CALCITE_OPERATOR.put(SqlStdOperatorTable.ACOS, AccelFunction.ACOS);
        BY_CALCITE_OPERATOR.put(SqlStdOperatorTable.ATAN, AccelFunction.ATAN);
        BY_CALCITE_OPERATOR.put(SqlStdOperatorTable.ATAN2, AccelFunction.ATAN2);
        BY_CALCITE_OPERATOR.put(SqlStdOperatorTable.SIGN, AccelFunction.SIGN);

        // Flink's own, on the Calcite side: no SqlStdOperatorTable entry exists for either.
        BY_CALCITE_OPERATOR.put(FlinkSqlOperatorTable.LOG2, AccelFunction.LOG2);
        BY_CALCITE_OPERATOR.put(FlinkSqlOperatorTable.TANH, AccelFunction.TANH);

        // And Flink's own, on the Table API side. These arrive as a BridgingSqlFunction wrapping
        // the definition, so the identity that means anything is the definition's, not the
        // operator's -- a bridging operator is constructed per call site.
        BY_DEFINITION.put(BuiltInFunctionDefinitions.LEAST, AccelFunction.LEAST);
        BY_DEFINITION.put(BuiltInFunctionDefinitions.GREATEST, AccelFunction.GREATEST);
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
     * Builds the subtree for one Calc: a projection, optionally over a filter, over the input.
     *
     * <p>This is what a provider is handed. It is a subtree rather than a bare expression list
     * because a provider must be able to recognise shapes spanning operators — and because the
     * filter's position relative to the projection is information it needs.
     *
     * @return empty when any expression has no representation, which is an ordinary outcome
     */
    /**
     * A grouped or ungrouped partial aggregate over an already-built projection.
     *
     * <p>The node this produces is Flink's <em>local</em> aggregate, which is partial: it may emit
     * several rows per key and the merge half above the shuffle combines them. A provider may
     * therefore aggregate a batch at a time and emit as it goes, carrying no state between batches
     * -- which is the whole reason a device group-by is workable at all.
     *
     * <p>Refused, by returning empty, when any call is one the IR has no function for. SUM and
     * COUNT(*) are what a local aggregate mostly produces; AVG is not among them because its
     * accumulator is two fields and the IR describes one value per call.
     *
     * @param grouping key field indexes into the projection's output
     * @param aggCalls the local aggregate's calls, in output order after the keys
     * @param input the projection feeding the aggregate, already in IR
     * @param outputType the aggregate's own output row
     */
    /**
     * The IR for a sort, or empty when the spec is not one a device can order.
     *
     * <p>Takes the input row rather than a subtree beneath it: a sort does not care what produced
     * its rows and cannot fuse with it — the producer is per-row and the sort is not, so there is
     * no single kernel the pair could become.
     */
    public static Optional<AccelNode> buildSort(SortSpec sortSpec, RowType inputType) {
        if (sortSpec.getFieldSize() != 1) {
            return Optional.empty();
        }
        SortSpec.SortFieldSpec field = sortSpec.getFieldSpec(0);
        return Optional.of(
                new AccelSort(
                        field.getFieldIndex(),
                        field.getIsAscendingOrder(),
                        field.getNullIsLast(),
                        new AccelInput(inputType),
                        inputType));
    }

    /**
     * The same sort, over a projection rather than over the operator's input.
     *
     * <p>What makes a fused pair one subtree instead of two. An unfused sort is handed an {@link
     * AccelInput} leaf and a provider has no way to know a projection preceded it, so the two nodes
     * become two task graphs and the projection's output crosses the bus and comes straight back.
     * Nesting the projection lets a provider compile one kernel and order its output where it lies.
     *
     * <p>A sort does not change the schema, so the output type is the projection's.
     */
    public static Optional<AccelNode> buildSort(
            SortSpec sortSpec, AccelNode input, RowType outputType) {
        if (sortSpec.getFieldSize() != 1) {
            return Optional.empty();
        }
        SortSpec.SortFieldSpec field = sortSpec.getFieldSpec(0);
        return Optional.of(
                new AccelSort(
                        field.getFieldIndex(),
                        field.getIsAscendingOrder(),
                        field.getNullIsLast(),
                        input,
                        outputType));
    }

    /**
     * The IR for a top-N: the same ordering, keeping only the first {@code limit} rows.
     *
     * <p>One node rather than a sort with a limit above it, for the reason {@link AccelSort}
     * records: a bounded sort is the only kind whose output is smaller than its input, and
     * splitting it would oblige a provider to return the whole ordering so that something else
     * could throw nearly all of it away.
     *
     * <p>Empty when the range does not start at the first row. {@code limitStart} is an offset into
     * the ordering, and an offset is not expressible here: a provider is asked for the first
     * {@code N} of a permutation, not a window of it, and the node has nowhere to put the
     * difference.
     *
     * @param limitStart first row of the ordering to emit, which must be 0
     * @param limitEnd one past the last row of the ordering to emit
     */
    public static Optional<AccelNode> buildTopN(
            SortSpec sortSpec, long limitStart, long limitEnd, RowType inputType) {
        if (sortSpec.getFieldSize() != 1 || limitStart != 0L || limitEnd <= 0L) {
            return Optional.empty();
        }
        SortSpec.SortFieldSpec field = sortSpec.getFieldSpec(0);
        return Optional.of(
                new AccelSort(
                        field.getFieldIndex(),
                        field.getIsAscendingOrder(),
                        field.getNullIsLast(),
                        limitEnd,
                        new AccelInput(inputType),
                        inputType));
    }

    /**
     * A local aggregate rooted at the scan that feeds it: a two-operator device region.
     *
     * <p>Refuses anything the cuDF binding cannot serve directly — more than one grouping key,
     * anything but {@code SUM}, a distinct or filtered call, a non-FP64 argument or a non-INT32
     * key. Those are refusals of shape, not of cost, so they are made here rather than left for a
     * provider to discover.
     *
     * <p>{@code input} is the device-resident subtree the aggregate sits on. Until V10 that was
     * always the {@link AccelScan} itself; it may now also be an {@link AccelProject} over one, in
     * which case the summed field indexes the projection's output and not the file's columns.
     *
     * <p>An empty {@code grouping} is admitted and means a global sum. Flink only ever asks for
     * one on the <em>local</em> half of a two-phase aggregate, so the answer a provider returns is
     * a partial that the {@code Final} aggregate above the substitution combines — one row per
     * subtask, not one row for the query.
     *
     * @param grouping key field indexes into {@code inputType}, possibly empty
     * @param aggCalls the local half's calls, in the order they follow the keys
     */
    public static Optional<AccelNode> buildAggregateOverScan(
            int[] grouping,
            List<AggregateCall> aggCalls,
            AccelNode input,
            RowType inputType,
            RowType outputType) {
        if (grouping.length > 1 || aggCalls.isEmpty()) {
            return Optional.empty();
        }
        // More than one call is admitted because a contraction is many of them. A Gram matrix is
        // d(d+1)/2 sums over one projection, and refusing past the first turned the one shape in
        // this project whose advantage grows with the query into a shape the region could not
        // carry. Which call means what is the provider's business -- the planner's job is to say
        // that each is a plain SUM over a column of the input, and nothing here reads the
        // aggregate as a Gram.
        final List<AccelAggCall> accelCalls = new ArrayList<>(aggCalls.size());
        for (AggregateCall call : aggCalls) {
            if (call.isDistinct() || call.hasFilter() || call.getArgList().size() != 1) {
                return Optional.empty();
            }
            // SUM and MAX, and nothing else. Both combine associatively across batches, which is
            // what makes a per-batch partial a valid answer for the Final aggregate above the
            // substitution to combine. AVG and COUNT(x) do not, for different reasons.
            final AccelAggFunction function;
            switch (call.getAggregation().getName()) {
                case "SUM":
                    function = AccelAggFunction.SUM;
                    break;
                case "MAX":
                    function = AccelAggFunction.MAX;
                    break;
                default:
                    return Optional.empty();
            }
            final int valueField = call.getArgList().get(0);
            if (valueField >= inputType.getFieldCount()) {
                return Optional.empty();
            }
            // FLOAT beside DOUBLE: Flink sums a FLOAT column in a FLOAT accumulator, so an FP32
            // query is FP32 on both arms and a provider that serves one is not quietly changing
            // the arithmetic. Mixed widths across the calls are refused below.
            final LogicalType valueType = inputType.getTypeAt(valueField);
            if (!(valueType instanceof DoubleType) && !(valueType instanceof FloatType)) {
                return Optional.empty();
            }
            if (!valueType.getTypeRoot().equals(inputType.getTypeAt(
                    aggCalls.get(0).getArgList().get(0)).getTypeRoot())) {
                return Optional.empty();
            }
            accelCalls.add(new AccelAggCall(function, valueField, valueType));
        }
        // One function across all the calls. A provider serves the set as one contraction -- a
        // Gram matrix is d(d+1)/2 sums of one shape -- and a mixture would be two contractions
        // sharing a staging buffer, which is a different thing and is not described here.
        for (AccelAggCall call : accelCalls) {
            if (call.function() != accelCalls.get(0).function()) {
                return Optional.empty();
            }
        }
        for (int keyField : grouping) {
            if (keyField >= inputType.getFieldCount()
                    || !(inputType.getTypeAt(keyField) instanceof IntType)) {
                return Optional.empty();
            }
        }
        return Optional.of(
                new AccelAggregate(grouping.clone(), accelCalls, input, outputType));
    }

    /**
     * A join rooted at the two scans that feed it: the first region with two sources.
     *
     * <p>Offered only as the <em>inside</em> of a region, never as a node to be offloaded on its
     * own. A join that emits is priced by its output, which is the product of its inputs; a join
     * inside a region emits nothing, because the projection above consumes each pair on the device
     * and the aggregate above that reduces them away. {@link
     * org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecNestedLoopJoin} therefore
     * exposes this and does not report itself offloadable.
     *
     * <p>Refusals here are of shape. The condition has to be one equality between one column of
     * each side, because anything else is a predicate over a <em>pair</em> of rows and this IR has
     * no node that evaluates one. The type has to be an inner join, because an outer one emits
     * rows with no partner and the region's output would then depend on which pairs did not
     * match. Both keys have to be {@code INT NOT NULL}: a device column has no representation for
     * a null, and SQL says a null key matches nothing while a column with no mask would match it
     * to every other null.
     *
     * @param leftIsBuild which side the planner chose to build, which decides nothing about the
     *     output and everything about which side has to be resident
     */
    public static Optional<AccelNode> buildJoinOver(
            FlinkJoinType joinType,
            RexNode condition,
            boolean leftIsBuild,
            AccelNode left,
            AccelNode right,
            RowType leftType,
            RowType rightType,
            RowType outputType) {
        if (joinType != FlinkJoinType.INNER) {
            return Optional.empty();
        }
        // No condition, or one that is literally TRUE: a cross join. Calcite writes the second for
        // `FROM a, b`, which is how a query asks for every pair -- and every pair is the shape
        // this exists for, so it is admitted first and not as an afterthought.
        if (condition == null || condition.isAlwaysTrue()) {
            return Optional.of(
                    new AccelJoin(
                            AccelJoin.NO_KEY,
                            AccelJoin.NO_KEY,
                            leftIsBuild,
                            AccelJoin.UNKNOWN_ROWS,
                            leftIsBuild ? left : right,
                            leftIsBuild ? right : left,
                            outputType));
        }
        if (!(condition instanceof RexCall)
                || ((RexCall) condition).getOperator().getKind() != SqlKind.EQUALS) {
            return Optional.empty();
        }
        final List<RexNode> operands = ((RexCall) condition).getOperands();
        if (operands.size() != 2
                || !(operands.get(0) instanceof RexInputRef)
                || !(operands.get(1) instanceof RexInputRef)) {
            return Optional.empty();
        }
        final int leftWidth = leftType.getFieldCount();
        int a = ((RexInputRef) operands.get(0)).getIndex();
        int b = ((RexInputRef) operands.get(1)).getIndex();
        // One index each side, in either order as Calcite wrote it.
        if (a > b) {
            final int swap = a;
            a = b;
            b = swap;
        }
        if (a >= leftWidth || b < leftWidth) {
            return Optional.empty();
        }
        final int leftKey = a;
        final int rightKey = b - leftWidth;
        if (!isDenseIntKey(leftType, leftKey) || !isDenseIntKey(rightType, rightKey)) {
            return Optional.empty();
        }
        return Optional.of(
                new AccelJoin(
                        leftIsBuild ? leftKey : rightKey,
                        leftIsBuild ? rightKey : leftKey,
                        leftIsBuild,
                        AccelJoin.UNKNOWN_ROWS,
                        leftIsBuild ? left : right,
                        leftIsBuild ? right : left,
                        outputType));
    }

    private static boolean isDenseIntKey(RowType type, int field) {
        if (field >= type.getFieldCount()) {
            return false;
        }
        final LogicalType key = type.getTypeAt(field);
        return key instanceof IntType && !key.isNullable();
    }

    /**
     * A projection rooted at the device subtree that feeds it rather than at an anonymous input.
     *
     * <p>The mirror of {@link #buildTopNOverScan} for a {@link org.apache.flink.table.planner.plan
     * .nodes.exec.batch.BatchExecCalc}: {@link #buildCalc} roots the same expressions at an
     * {@link AccelInput}, meaning "rows arrive, already on the host", which is the right answer
     * when the Calc is offloaded on its own. Rooted at a scan instead, the projection is something
     * an operator <em>above</em> it can keep on the device.
     *
     * <p>A filter is refused rather than rendered. The generator can write one, but it writes it
     * as a mask the host then compacts, and nothing here yet consumes a masked device batch — so
     * admitting one would produce a subtree whose row count is a lie.
     */
    public static Optional<AccelNode> buildProjectOver(
            List<RexNode> projection,
            @Nullable RexNode condition,
            AccelNode input,
            RowType outputType) {
        if (condition != null) {
            return Optional.empty();
        }
        final List<AccelExpression> projections = new ArrayList<>(projection.size());
        for (RexNode expression : projection) {
            final Optional<AccelExpression> converted = convert(expression);
            if (!converted.isPresent()) {
                return Optional.empty();
            }
            projections.add(converted.get());
        }
        return Optional.of(new AccelProject(projections, input, outputType));
    }

    /**
     * The same top-N, but rooted at the scan that feeds it rather than at an anonymous input.
     *
     * <p>The difference is the whole of M9: an {@link AccelInput} says "rows arrive, already on the
     * host", so a provider handed one can at best decorate work that has already been paid for. An
     * {@link AccelScan} says "here is the file", which is a question a provider can answer by
     * never bringing the rows to the host at all.
     *
     * <p>Refuses the same shapes {@link #buildTopN} refuses, and for the same reasons: one sort
     * key, no offset, a positive limit.
     */
    public static Optional<AccelNode> buildTopNOverScan(
            SortSpec sortSpec, long limitStart, long limitEnd, AccelScan scan) {
        if (sortSpec.getFieldSize() != 1 || limitStart != 0L || limitEnd <= 0L) {
            return Optional.empty();
        }
        SortSpec.SortFieldSpec field = sortSpec.getFieldSpec(0);
        return Optional.of(
                new AccelSort(
                        field.getFieldIndex(),
                        field.getIsAscendingOrder(),
                        field.getNullIsLast(),
                        limitEnd,
                        scan,
                        scan.outputType()));
    }

    /**
     * Builds the IR for an inner equi-join, or empty where the shape is not describable.
     *
     * <p>Takes its inputs in build/probe order, which is the order the transformation puts them in
     * and not the order the query wrote them; {@code buildIsLeft} carries the query's order, which
     * the output row still follows.
     */
    /**
     * Builds the IR for a running total, or empty where the shape is not describable.
     *
     * <p>{@link GpuOverAggregateSupport#estimate} has already refused everything this cannot
     * express by the time this is called; the check here is the one that survives being called from
     * somewhere else.
     */
    public static Optional<AccelNode> buildOverAggregate(
            OverSpec overSpec, RowType inputType, RowType outputType) {
        OptionalInt valueField = GpuOverAggregateSupport.valueFieldOf(overSpec);
        if (!valueField.isPresent() || valueField.getAsInt() >= inputType.getFieldCount()) {
            return Optional.empty();
        }
        return Optional.of(
                new AccelOverAggregate(
                        valueField.getAsInt(), new AccelInput(inputType), outputType));
    }

    public static Optional<AccelNode> buildJoin(
            JoinSpec joinSpec,
            RowType buildType,
            RowType probeType,
            RowType outputType,
            boolean buildIsLeft,
            long estimatedBuildRows) {
        if (joinSpec.getLeftKeys().length != 1 || joinSpec.getRightKeys().length != 1) {
            return Optional.empty();
        }
        return Optional.of(
                new AccelJoin(
                        GpuJoinSupport.buildKeyOf(joinSpec, buildIsLeft),
                        GpuJoinSupport.probeKeyOf(joinSpec, buildIsLeft),
                        buildIsLeft,
                        estimatedBuildRows,
                        new AccelInput(buildType),
                        new AccelInput(probeType),
                        outputType));
    }

    public static Optional<AccelNode> buildAggregate(
            int[] grouping, List<AggregateCall> aggCalls, AccelNode input, RowType outputType) {

        List<AccelAggCall> calls = new ArrayList<>(aggCalls.size());
        for (int i = 0; i < aggCalls.size(); i++) {
            AggregateCall call = aggCalls.get(i);
            Optional<AccelAggCall> converted =
                    convertAggCall(call, outputType, grouping.length + i);
            if (!converted.isPresent()) {
                return Optional.empty();
            }
            calls.add(converted.get());
        }
        if (calls.isEmpty()) {
            // Nothing to contract; the grouping alone is a DISTINCT and a different operator.
            return Optional.empty();
        }
        return Optional.of(new AccelAggregate(grouping, calls, input, outputType));
    }

    private static Optional<AccelAggCall> convertAggCall(
            AggregateCall call, RowType outputType, int outputField) {
        if (call.isDistinct() || call.isApproximate() || call.hasFilter()) {
            // DISTINCT needs a second pass the IR does not describe, and a FILTER is a predicate
            // the aggregate would have to evaluate rather than the projection beneath it.
            return Optional.empty();
        }
        LogicalType resultType = outputType.getTypeAt(outputField);
        switch (call.getAggregation().getKind()) {
            case SUM:
            case SUM0:
                if (call.getArgList().size() != 1) {
                    return Optional.empty();
                }
                return Optional.of(
                        new AccelAggCall(
                                AccelAggFunction.SUM, call.getArgList().get(0), resultType));
            case COUNT:
                // Only COUNT(*). COUNT(x) skips nulls, which is a different contraction and needs
                // the validity of x, not just its values.
                if (!call.getArgList().isEmpty()) {
                    return Optional.empty();
                }
                return Optional.of(
                        new AccelAggCall(
                                AccelAggFunction.COUNT_STAR,
                                AccelAggCall.NO_INPUT_FIELD,
                                resultType));
            default:
                return Optional.empty();
        }
    }

    public static Optional<AccelNode> buildCalc(
            List<RexNode> projection,
            @Nullable RexNode condition,
            RowType inputType,
            RowType outputType) {

        AccelNode input = new AccelInput(inputType);
        if (condition != null) {
            Optional<AccelExpression> filter = convert(condition);
            if (!filter.isPresent()) {
                return Optional.empty();
            }
            input = new AccelFilter(filter.get(), input, inputType);
        }
        List<AccelExpression> projections = new ArrayList<>(projection.size());
        for (RexNode expression : projection) {
            Optional<AccelExpression> converted = convert(expression);
            if (!converted.isPresent()) {
                return Optional.empty();
            }
            projections.add(converted.get());
        }
        return Optional.of(new AccelProject(projections, input, outputType));
    }

    /**
     * Whether this call has an IR representation at all.
     *
     * <p>The single admission list. Before this existed the vocabulary was stated three times -- in
     * the cost estimator's weight table, in the kernel generator's switch, and implicitly in
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
            // Division is two operators wearing one SqlKind. On an integral type SQL truncates
            // toward zero, and a kernel computing in double answers 3.5 where SQL answers 3 --
            // which is invisible until it is a grouping key and the groups are wrong. The IR
            // therefore separates them rather than letting the generator guess from the types.
            if (byKind == AccelFunction.DIVIDE && isIntegral(call.getType())) {
                return AccelFunction.INT_DIVIDE;
            }
            return byKind;
        }
        SqlOperator operator = call.getOperator();
        if (operator instanceof BridgingSqlFunction) {
            // The bridging operator is built per call site, so its identity means nothing; the
            // definition behind it is the stable object. A user's catalog function reaches here
            // the same way with a definition that is in no map, and is refused by not matching --
            // it no longer has to be recognised as an impostor first.
            return BY_DEFINITION.get(((BridgingSqlFunction) operator).getDefinition());
        }
        return BY_CALCITE_OPERATOR.get(operator);
    }

    /** Whether this type is one SQL does integer arithmetic on. */
    private static boolean isIntegral(org.apache.calcite.rel.type.RelDataType type) {
        switch (type.getSqlTypeName()) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
                return true;
            default:
                return false;
        }
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
            box = asFlinkDouble((BigDecimal) value, type.get());
        } else if (value instanceof Number) {
            box = ((Number) value).doubleValue();
        } else if (value instanceof Boolean) {
            box = (Boolean) value;
        } else {
            return Optional.empty();
        }
        return Optional.of(new AccelLiteral(box, type.get()));
    }

    /**
     * A DECIMAL literal as the double Flink's own runtime would use, which is not always the
     * nearest one.
     *
     * <p>{@code BigDecimal.doubleValue()} is correctly rounded. Flink is not: {@code
     * DecimalDataUtils.doubleValue} converts a compact decimal — precision 18 or less, held as a
     * {@code long} — as {@code ((double) unscaledLong) / POW10[scale]}, a division that rounds
     * again. For some literals the two land on different doubles.
     *
     * <p>This mattered because the IR carries a {@code double} and the provider generates a kernel
     * from it: converting the better way here made the kernel compute something Flink's CPU does
     * not. Traced from a disagreement over 10M rows in which one constant differed on every row
     * while seven others agreed:
     *
     * <pre>
     *   0.9909456437464825   correctly rounded -> 0.9909456437464825
     *                        Flink's runtime   -> 0.9909456437464824
     * </pre>
     *
     * <p>An accelerator has to reproduce the engine it is accelerating, including where the engine
     * is less accurate than it could be. Anything else is a different query.
     */
    @VisibleForTesting
    static double asFlinkDouble(BigDecimal value, LogicalType declared) {
        if (declared instanceof DecimalType) {
            final DecimalType type = (DecimalType) declared;
            final DecimalData asFlink =
                    DecimalData.fromBigDecimal(value, type.getPrecision(), type.getScale());
            if (asFlink != null) {
                return DecimalDataUtils.doubleValue(asFlink);
            }
        }
        return value.doubleValue();
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
