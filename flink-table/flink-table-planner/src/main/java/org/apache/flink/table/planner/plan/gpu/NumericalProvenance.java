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

import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.GpuOffloadExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Which computed values may differ on a device, and what a plan is allowed to do with them.
 *
 * <h2>The problem this exists for</h2>
 *
 * <p>A device's {@code EXP}, {@code LN}, {@code SIN} and friends come from CUDA's math library,
 * which specifies its own accuracy and is not required to agree with {@code java.lang.Math}. A
 * filter over such a value was already refused. A <em>projection</em> was not, and the value then
 * left the offloaded subtree — so the query returned a different number depending on where it ran.
 * Measured: an {@code ORDER BY ... LIMIT 20} whose twenty selected ids and their order were
 * identical on both arms, but one returned score differed in its last ulp.
 *
 * <p>Two separate promises are at stake and they are worth keeping apart:
 *
 * <ul>
 *   <li>the <b>output-value</b> promise — the numbers a query returns are Flink's numbers;
 *   <li>the <b>membership</b> promise — the rows a query returns are Flink's rows.
 * </ul>
 *
 * <p>Protecting the second does not give the first. A projection that only ever reaches the sink
 * breaks no membership promise and still hands back a different number.
 *
 * <h2>The contract</h2>
 *
 * <p><b>Strict, and the default.</b> A subtree that computes a drift-prone value is not offloaded
 * at all. Both promises hold, and because no approximate value is ever produced there is no
 * provenance to track anywhere in the plan.
 *
 * <p><b>Approximate, opt-in</b> via {@code table.exec.accelerator.approximate-projections}. The
 * output-value promise is explicitly given up; the membership promise is still enforced, by
 * refusing the offload whenever a drift-prone value can reach something that decides which rows
 * come back or in what order. This is not "strict transparent acceleration" and is never presented
 * as such.
 *
 * <h2>Why the classification errs towards refusing</h2>
 *
 * <p>{@link #reachesOnlyValueConsumers} walks forward from the producing node through the node
 * types whose effect on membership is understood. Anything else — a node type not in that list, or
 * a plan shape not analysed — is treated as unsafe. A provenance analysis that silently loses track
 * of a value is worse than none, because it produces a guarantee nobody checked.
 */
public final class NumericalProvenance {

    private NumericalProvenance() {}

    /**
     * The functions whose device implementation is not required to match {@code java.lang.Math}.
     *
     * <p>The same set the provider's {@code DriftSensitivePredicate} refuses in a filter, kept
     * deliberately in step with it: one list, two enforcement points, and a function added to one
     * without the other would open exactly the gap this class closes.
     */
    private static final Set<SqlOperator> DRIFT_PRONE =
            new HashSet<>(
                    Arrays.asList(
                            SqlStdOperatorTable.SQRT,
                            SqlStdOperatorTable.EXP,
                            SqlStdOperatorTable.LN,
                            SqlStdOperatorTable.LOG10,
                            SqlStdOperatorTable.POWER,
                            SqlStdOperatorTable.SIN,
                            SqlStdOperatorTable.COS,
                            SqlStdOperatorTable.TAN,
                            SqlStdOperatorTable.ASIN,
                            SqlStdOperatorTable.ACOS,
                            SqlStdOperatorTable.ATAN,
                            SqlStdOperatorTable.ATAN2));

    /**
     * Matched by name as well, for the operators Flink defines outside Calcite's standard table.
     */
    private static final Set<String> DRIFT_PRONE_NAMES =
            new HashSet<>(Arrays.asList("LOG2", "TANH", "SINH", "COSH", "CBRT", "LOG"));

    /** Whether any of these expressions computes a value a device may round differently. */
    public static boolean computesApproximateValues(
            List<RexNode> projection, @Nullable RexNode condition) {
        for (RexNode node : projection) {
            if (isApproximate(node)) {
                return true;
            }
        }
        return condition != null && isApproximate(condition);
    }

    /**
     * Which output columns of this projection may drift, by index.
     *
     * <p>A condition that drifts poisons the whole node rather than a column — it decides rows
     * directly — and is reported as every column, so a caller cannot conclude anything is safe.
     */
    public static Set<Integer> approximateColumns(
            List<RexNode> projection, @Nullable RexNode condition) {
        if (condition != null && isApproximate(condition)) {
            final Set<Integer> all = new HashSet<>();
            for (int i = 0; i < projection.size(); i++) {
                all.add(i);
            }
            return all;
        }
        final Set<Integer> drifting = new HashSet<>();
        for (int i = 0; i < projection.size(); i++) {
            if (isApproximate(projection.get(i))) {
                drifting.add(i);
            }
        }
        return drifting;
    }

    /**
     * Where drift on these input columns lands in a projection's output.
     *
     * <p>An output column drifts if its expression reads a drifting input column, or if it
     * computes a drift-prone function of its own — the second case is what makes this composable,
     * since a chain of projections each adding a little is the normal shape.
     *
     * <p>Null when the condition reads a drifting column: that is a filter deciding rows from a
     * value the device rounds differently, which is the case this whole analysis exists to refuse.
     */
    @Nullable
    public static Set<Integer> forwardDrift(
            List<RexNode> projection, @Nullable RexNode condition, Set<Integer> incoming) {
        if (condition != null && (reads(condition, incoming) || isApproximate(condition))) {
            return null;
        }
        final Set<Integer> forwarded = new HashSet<>();
        for (int i = 0; i < projection.size(); i++) {
            final RexNode expression = projection.get(i);
            if (reads(expression, incoming) || isApproximate(expression)) {
                forwarded.add(i);
            }
        }
        return forwarded;
    }

    /** Whether an expression reads any of these input columns. */
    private static boolean reads(RexNode expression, Set<Integer> columns) {
        final boolean[] found = {false};
        expression.accept(
                new RexVisitorImpl<Void>(true) {
                    @Override
                    public Void visitInputRef(RexInputRef input) {
                        if (columns.contains(input.getIndex())) {
                            found[0] = true;
                        }
                        return null;
                    }
                });
        return found[0];
    }

    private static boolean isApproximate(RexNode node) {
        final boolean[] found = {false};
        node.accept(
                new RexVisitorImpl<Void>(true) {
                    @Override
                    public Void visitCall(RexCall call) {
                        final SqlOperator operator = call.getOperator();
                        if (DRIFT_PRONE.contains(operator)
                                || DRIFT_PRONE_NAMES.contains(operator.getName().toUpperCase())) {
                            found[0] = true;
                        }
                        return super.visitCall(call);
                    }
                });
        return found[0];
    }

    /**
     * Whether every consumer of this node, transitively, only carries values rather than deciding
     * rows with them.
     *
     * <p>Value-carrying, and the whole list: a sink, and a {@code Calc} with no condition — a
     * projection, which computes new values but selects nothing. Everything else is treated as
     * deciding membership or order, including node types that do not appear here at all.
     *
     * <p>The walk is forward, which an {@code ExecNode} graph does not support directly, so the
     * caller supplies the consumers it built while walking the graph.
     */
    public static boolean reachesOnlyValueConsumers(
            ExecNode<?> node, java.util.Map<ExecNode<?>, List<ExecNode<?>>> consumers) {
        return reachesOnlyValueConsumers(node, consumers, null);
    }

    /**
     * The same question, asked of particular columns.
     *
     * <p>Node granularity refuses more than it has to in one shape that matters: a grouped
     * aggregate decides membership <em>of groups</em>, and the groups are decided by its grouping
     * keys alone. A drifting value that feeds only an aggregate call cannot move a row into a
     * different group — it changes what the group sums to, which is a value. The unqualified form
     * treats the whole node as unsafe and so refuses
     * {@code SELECT k, SUM(haversine(...)) ... GROUP BY k}, where {@code k} is untouched.
     *
     * <p>{@code drifting} is indexed into this node's output row, which is the aggregate's input
     * row, so the grouping keys are in the same index space. Null means "assume every column", and
     * the analysis falls back to node granularity — which is what a consumer more than one step
     * away gets, since the mapping between index spaces is not tracked across a node.
     */
    public static boolean reachesOnlyValueConsumers(
            ExecNode<?> node,
            java.util.Map<ExecNode<?>, List<ExecNode<?>>> consumers,
            @Nullable Set<Integer> drifting) {
        final java.util.Deque<Reach> pending = new java.util.ArrayDeque<>();
        final Set<ExecNode<?>> seen =
                java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
        pending.add(new Reach(node, drifting));
        seen.add(node);
        while (!pending.isEmpty()) {
            final Reach current = pending.poll();
            for (ExecNode<?> consumer : consumers.getOrDefault(current.node, java.util.List.of())) {
                final Set<Integer> forwarded = forward(consumer, current.drifting);
                if (!isValueOnly(consumer) && forwarded == null) {
                    return false;
                }
                if (seen.add(consumer)) {
                    pending.add(new Reach(consumer, forwarded));
                }
            }
        }
        return true;
    }

    /** A node reached by the walk, and which of its output columns carry the drift. */
    private static final class Reach {
        private final ExecNode<?> node;
        private final @Nullable Set<Integer> drifting;

        private Reach(ExecNode<?> node, @Nullable Set<Integer> drifting) {
            this.node = node;
            this.drifting = drifting;
        }
    }

    /**
     * Where drift on these columns lands in the consumer's output row, or null when unknown.
     *
     * <p>Three mappings are known, and they are the three the walk has to cross to get from a
     * projection to a sink: an exchange leaves the row alone, a node that can describe its own
     * projection is asked, and a grouped aggregate moves the drift to where its calls land.
     * Anything else is unknown, which is not by itself a refusal — see the caller.
     */
    private static @Nullable Set<Integer> forward(
            ExecNode<?> consumer, @Nullable Set<Integer> drifting) {
        if (drifting == null) {
            return null;
        }
        if (drifting.isEmpty()) {
            // Nothing drifts here and nothing downstream can start drifting on its own.
            return drifting;
        }
        if (isExchange(consumer)) {
            // A hash exchange whose keys drift sends a row to a different subtask, which is a
            // membership decision made with a drifting value however the consumer behaves.
            final int[] keys = partitionKeysOf(consumer);
            if (keys != null) {
                for (int key : keys) {
                    if (drifting.contains(key)) {
                        return null;
                    }
                }
            }
            return drifting;
        }
        if (consumer instanceof GpuOffloadExecNode) {
            final Set<Integer> mapped = ((GpuOffloadExecNode) consumer).forwardDrift(drifting);
            if (mapped != null) {
                return mapped;
            }
        }
        return throughAggregate(consumer, drifting);
    }

    private static boolean isExchange(ExecNode<?> consumer) {
        return consumer.getClass().getSimpleName().endsWith("Exchange");
    }

    /**
     * A hash exchange's partition keys, or null when it does not partition by value.
     *
     * <p>Read off the required distribution rather than reflectively: {@code InputProperty} is the
     * declared shape of the edge and is the thing that actually decides the partitioning.
     */
    @Nullable
    private static int[] partitionKeysOf(ExecNode<?> consumer) {
        for (InputProperty property : consumer.getInputProperties()) {
            final InputProperty.RequiredDistribution distribution =
                    property.getRequiredDistribution();
            if (distribution instanceof InputProperty.HashDistribution) {
                return ((InputProperty.HashDistribution) distribution).getKeys();
            }
        }
        return null;
    }

    /**
     * Where the drift goes after a grouped aggregate, or null if the aggregate is not safe.
     *
     * <p>A grouped aggregate decides membership of groups, and the groups come from the grouping
     * keys alone. If none of them drifts, a row cannot land in a different group — the drift only
     * changes what each group sums to. That is a value, and it survives into the output at the
     * positions the aggregate calls occupy, which follow the grouping keys.
     *
     * <p>Returning the forwarded set rather than a boolean is what lets the walk cross the local
     * aggregate, the exchange and the global aggregate of a two-phase {@code GROUP BY}: each step
     * re-expresses the drift in the next node's index space instead of giving up on it. Giving up
     * is what refused {@code SELECT k, SUM(haversine(...)) FROM t GROUP BY k}, whose grouping key
     * is untouched.
     */
    private static @Nullable Set<Integer> throughAggregate(
            ExecNode<?> consumer, @Nullable Set<Integer> drifting) {
        if (drifting == null || !consumer.getClass().getSimpleName().endsWith("Aggregate")) {
            return null;
        }
        final int[] grouping = groupingOf(consumer);
        if (grouping == null) {
            return null;
        }
        for (int key : grouping) {
            if (drifting.contains(key)) {
                return null;
            }
        }
        final int arity = consumer.getOutputType().getChildren().size();
        final Set<Integer> forwarded = new HashSet<>();
        for (int i = grouping.length; i < arity; i++) {
            forwarded.add(i);
        }
        return forwarded;
    }

    /** The grouping keys, read reflectively; null when absent or unreadable — the safe direction. */
    @Nullable
    private static int[] groupingOf(ExecNode<?> consumer) {
        for (Class<?> type = consumer.getClass(); type != null; type = type.getSuperclass()) {
            try {
                final java.lang.reflect.Field field = type.getDeclaredField("grouping");
                field.setAccessible(true);
                final Object grouping = field.get(consumer);
                return grouping instanceof int[] ? (int[]) grouping : null;
            } catch (NoSuchFieldException keepLooking) {
                continue;
            } catch (ReflectiveOperationException | RuntimeException notVisible) {
                return null;
            }
        }
        return null;
    }

    /**
     * Whether this consumer only passes values through.
     *
     * <p>Matched on the exec node's own description rather than by importing every batch node
     * class: the planner module already has them, but naming them here would tie this analysis to a
     * fixed list that a new node type silently falls off. Anything unrecognised is not value-only.
     */
    private static boolean isValueOnly(ExecNode<?> consumer) {
        final String kind = consumer.getClass().getSimpleName();
        if (kind.endsWith("Sink")) {
            return true;
        }
        if (kind.endsWith("Calc")) {
            // A Calc with a condition filters, and a filter over a drifting value decides rows.
            return !hasCondition(consumer);
        }
        if (kind.endsWith("Aggregate")) {
            // An aggregate with no grouping keys produces one row whatever the values are: it
            // cannot put a row in a different group, because there is only one. An aggregate WITH
            // grouping keys decides membership of groups, and whether that matters depends on
            // which columns drift -- see throughAggregate.
            return isUngrouped(consumer);
        }
        if (kind.endsWith("Exchange")) {
            // A shuffle moves rows between subtasks. It changes the order values are summed in,
            // which is already true of the CPU, but it does not decide which rows exist.
            return true;
        }
        return false;
    }

    /**
     * Whether an aggregate-like node groups by anything.
     *
     * <p>Absent or unreadable grouping means "assume it groups" — the safe direction for a check
     * whose whole job is to be conservative.
     */
    private static boolean isUngrouped(ExecNode<?> consumer) {
        final int[] grouping = groupingOf(consumer);
        return grouping != null && grouping.length == 0;
    }

    /**
     * Whether a {@code Calc}-like node carries a filter condition.
     *
     * <p>Reflective because {@code condition} is private to the node and this analysis lives
     * outside it; a node that does not expose one is treated as though it filters, which is the
     * safe direction.
     */
    private static boolean hasCondition(ExecNode<?> consumer) {
        try {
            final java.lang.reflect.Field field = consumer.getClass().getDeclaredField("condition");
            field.setAccessible(true);
            return field.get(consumer) != null;
        } catch (ReflectiveOperationException | RuntimeException notVisible) {
            return true;
        }
    }
}
