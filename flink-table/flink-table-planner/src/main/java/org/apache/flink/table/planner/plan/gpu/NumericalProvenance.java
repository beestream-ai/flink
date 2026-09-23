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

import org.apache.calcite.rex.RexCall;
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
        final java.util.Deque<ExecNode<?>> pending = new java.util.ArrayDeque<>();
        final Set<ExecNode<?>> seen =
                java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
        pending.add(node);
        seen.add(node);
        while (!pending.isEmpty()) {
            final ExecNode<?> current = pending.poll();
            for (ExecNode<?> consumer : consumers.getOrDefault(current, java.util.List.of())) {
                if (!isValueOnly(consumer)) {
                    return false;
                }
                if (seen.add(consumer)) {
                    pending.add(consumer);
                }
            }
        }
        return true;
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
            // grouping keys decides membership of groups and is not value-only.
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
     * <p>Read reflectively, like the condition above, and absent or unreadable means "assume it
     * groups" — the safe direction for a check whose whole job is to be conservative.
     */
    private static boolean isUngrouped(ExecNode<?> consumer) {
        try {
            final java.lang.reflect.Field field = consumer.getClass().getDeclaredField("grouping");
            field.setAccessible(true);
            final Object grouping = field.get(consumer);
            return grouping instanceof int[] && ((int[]) grouping).length == 0;
        } catch (ReflectiveOperationException | RuntimeException notVisible) {
            return false;
        }
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
