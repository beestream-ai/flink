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

package org.apache.flink.table.planner.plan.nodes.exec.processor;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.planner.plan.gpu.GpuOffloadAssignment;
import org.apache.flink.table.planner.plan.gpu.GpuOffloadDecision;
import org.apache.flink.table.planner.plan.gpu.NumericalProvenance;
import org.apache.flink.table.planner.plan.gpu.DeviceScanPlacement;
import org.apache.flink.table.planner.plan.gpu.RowCost;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph;
import org.apache.flink.table.planner.plan.nodes.exec.GpuOffloadExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecAcceleratedGroup;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecAcceleratedSort;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecHashAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecSort;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecCalc;
import org.apache.flink.table.planner.plan.nodes.exec.visitor.AbstractExecNodeExactlyOnceVisitor;
import org.apache.flink.table.types.logical.RowType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Finds maximal connected subtrees whose nodes can all run as device kernels, costs each subtree as
 * a unit, and records the decision on its nodes.
 *
 * <p>Mostly it annotates rather than rewrites: the substitution — emitting one offload operator per
 * selected group instead of per-node {@code CodeGenOperatorFactory}s — happens during translation,
 * which is where each node builds its {@code Transformation}. Separating the two keeps the decision
 * in one place and makes it inspectable before anything is built.
 *
 * <p>The one exception is fusion, and it has to be an exception. A projection feeding an aggregate
 * becomes a single {@link BatchExecAcceleratedGroup} owning both, because a fused pair is one
 * operator and a graph that says otherwise is lying: the absorbed node used to stay in the graph
 * producing no transformation, which is what made the node count, {@code EXPLAIN} and the JobGraph
 * disagree. M4.1.
 *
 * <h2>Why subtrees rather than nodes</h2>
 *
 * <p>Two independent reasons, both measured.
 *
 * <p><b>Transfer count.</b> A task graph per operator pays a host-to-device-to-host round trip even
 * when the consumer is another offloaded operator on the same device. Grouping turns 2n transfers
 * into 2.
 *
 * <p><b>Clearing the floor.</b> Costs sum across a group, so a chain of Calcs can exceed a
 * threshold that none of its members exceeds alone. A single cheap node is expected to be rejected,
 * and that is the correct outcome rather than a limitation.
 *
 * <h2>Grouping rule</h2>
 *
 * <p>A candidate node joins its consumer's group only when it has exactly one consumer. A node
 * feeding two consumers cannot have its output kept on the device for one of them and materialised
 * on the host for the other without either duplicating the kernel or adding a copy-out that the
 * grouping exists to avoid, so it starts its own group instead.
 */
public class GpuOffloadProcessor implements ExecNodeGraphProcessor {

    /** Group id for a node that was examined but could not join any subtree. */
    public static final int NO_GROUP = -1;

    @Override
    public ExecNodeGraph process(ExecNodeGraph execGraph, ProcessorContext context) {
        TableConfig config = context.getPlanner().getTableConfig();
        if (!config.get(ExecutionConfigOptions.TABLE_EXEC_ACCELERATOR_ENABLED)) {
            return execGraph;
        }
        // No thresholds: the planner decides what is expressible, not what is worth moving.
        GpuOffloadDecision decision = new GpuOffloadDecision();

        List<ExecNode<?>> order = topologicalOrder(execGraph);
        Map<ExecNode<?>, Integer> consumerCount = countConsumers(order);
        Map<ExecNode<?>, List<ExecNode<?>>> consumers = consumersOf(order);
        final boolean approximateProjectionsAllowed =
                config.get(ExecutionConfigOptions.TABLE_EXEC_ACCELERATOR_APPROXIMATE_PROJECTIONS);
        Map<ExecNode<?>, RowCost> costs = new IdentityHashMap<>();
        Map<ExecNode<?>, Integer> groupOf = new IdentityHashMap<>();
        Map<Integer, List<ExecNode<?>>> groups = new LinkedHashMap<>();
        int nextGroupId = 0;

        // Producers are visited before consumers, so a node's inputs already have a group when it
        // is considered and can be absorbed into the node's own.
        for (ExecNode<?> node : order) {
            if (!node.supportGpuOffload()) {
                continue;
            }
            // The numerical-provenance contract, before anything about cost.
            //
            // Strict default: a node computing a drift-prone value is not offloaded, so the query
            // returns Flink's numbers and no approximate value enters the plan for the rest of the
            // analysis to track. Opt-in: the value may be computed, but only where it cannot reach
            // anything that decides which rows come back or in what order -- membership stays
            // Flink's either way. NumericalProvenance treats an unrecognised consumer as unsafe.
            if (node.computesApproximateValues()) {
                final String refusal;
                if (!approximateProjectionsAllowed) {
                    refusal =
                            "computes a value a device may round differently from Flink (EXP, LN,"
                                    + " SIN and the like); set"
                                    + " table.exec.accelerator.approximate-projections to allow it, and"
                                    + " read what that gives up";
                } else if (!NumericalProvenance.reachesOnlyValueConsumers(
                        node, consumers, node.approximateColumns())) {
                    refusal =
                            "computes a value a device may round differently from Flink, and that"
                                    + " value reaches a consumer that decides which rows are returned"
                                    + " or in what order";
                } else {
                    refusal = null;
                }
                if (refusal != null) {
                    assign(
                            node,
                            new GpuOffloadAssignment(
                                    NO_GROUP,
                                    decision.forSubtree(
                                            Collections.singletonList(
                                                    RowCost.ineligible(refusal)))));
                    continue;
                }
            }
            RowCost cost = node.estimateRowCost();
            if (!cost.isEligible()) {
                // Record the reason so EXPLAIN can say why, but do not group an ineligible node:
                // one such member would poison an otherwise viable subtree.
                assign(
                        node,
                        new GpuOffloadAssignment(
                                NO_GROUP, decision.forSubtree(Collections.singletonList(cost))));
                continue;
            }
            costs.put(node, cost);

            Integer group = null;
            for (ExecNode<?> input : inputsOf(node)) {
                Integer inputGroup = groupOf.get(input);
                if (inputGroup == null || consumerCount.getOrDefault(input, 0) != 1) {
                    continue;
                }
                if (group == null) {
                    group = inputGroup;
                } else if (!group.equals(inputGroup)) {
                    // The node joins two upstream groups; merge the smaller into the larger.
                    group = merge(groups, groupOf, group, inputGroup);
                }
            }
            if (group == null) {
                group = nextGroupId++;
                groups.put(group, new ArrayList<>());
            }
            groups.get(group).add(node);
            groupOf.put(node, group);
        }

        for (Map.Entry<Integer, List<ExecNode<?>>> entry : groups.entrySet()) {
            List<RowCost> memberCosts = new ArrayList<>();
            for (ExecNode<?> member : entry.getValue()) {
                memberCosts.add(costs.get(member));
            }
            GpuOffloadDecision.Verdict verdict = decision.forSubtree(memberCosts);
            verdict =
                    requireStrictArithmeticIfObservable(
                            verdict, entry.getValue(), consumers, approximateProjectionsAllowed);
            for (ExecNode<?> member : entry.getValue()) {
                assign(member, new GpuOffloadAssignment(entry.getKey(), verdict));
            }
        }

        // After the groups, because what reads a scan is the only thing that decides whether
        // reading it on a device is worth anything, and that is not known until the operators
        // above it have their verdicts.
        offerScans(order, consumers, decision);

        return fuse(execGraph, groupOf, config);
    }

    /**
     * Replaces each fusable pair with one node owning both. M4.1.
     *
     * <p>A pair qualifies when the processor put a {@code Calc} and the aggregate it feeds in the
     * same group and selected that group. Sharing a group is what says the two were judged
     * together; feeding means there is something to fuse.
     *
     * <p>Skipped entirely for a compiled plan. The plan format knows the node types Flink ships and
     * this is not one of them, so writing a plan containing it would produce something no release
     * can read back — and M4.4 already settled that a compiled plan does not offload, which makes
     * fusing it pointless as well as unwritable.
     */
    private static ExecNodeGraph fuse(
            ExecNodeGraph execGraph, Map<ExecNode<?>, Integer> groupOf, TableConfig config) {
        // The aggregate's gate is per-kind, not global, and the difference matters. It defaults to
        // off because M4.0 measured a *CPU* fused pair as worth nothing while fusing took away the
        // projection's own accelerator path -- a bad trade while the fused pair had no device
        // route. A fused sort does have one: GpuProjectedSortOperator runs the kernel and
        // cudf::sorted_order in one task graph, so the projection's output is never copied back.
        // Gating that on an option named for the aggregate would turn off the thing it exists for.
        final boolean fuseAggregate =
                config.get(ExecutionConfigOptions.TABLE_EXEC_ACCELERATOR_FUSE_AGGREGATE);
        List<ExecNode<?>> newRoots = new ArrayList<>();
        Map<ExecNode<?>, ExecNode<?>> rewritten = new IdentityHashMap<>();
        for (ExecNode<?> root : execGraph.getRootNodes()) {
            newRoots.add(rewrite(root, groupOf, rewritten, fuseAggregate));
        }
        return new ExecNodeGraph(execGraph.getFlinkVersion(), newRoots);
    }

    /** Rebuilds the sub-graph below {@code node}, substituting fused pairs as it goes. */
    private static ExecNode<?> rewrite(
            ExecNode<?> node,
            Map<ExecNode<?>, Integer> groupOf,
            Map<ExecNode<?>, ExecNode<?>> rewritten,
            boolean fuseAggregate) {
        ExecNode<?> already = rewritten.get(node);
        if (already != null) {
            return already;
        }
        List<ExecEdge> edges = node.getInputEdges();
        for (int i = 0; i < edges.size(); i++) {
            ExecNode<?> newSource =
                    rewrite(edges.get(i).getSource(), groupOf, rewritten, fuseAggregate);
            if (newSource != edges.get(i).getSource()) {
                node.replaceInputEdge(i, ExecEdge.builder().source(newSource).target(node).build());
            }
        }
        ExecNode<?> result = fuseIfPossible(node, groupOf, fuseAggregate);
        rewritten.put(node, result);
        return result;
    }

    /** The fused node, or the node unchanged. */
    private static ExecNode<?> fuseIfPossible(
            ExecNode<?> node, Map<ExecNode<?>, Integer> groupOf, boolean fuseAggregate) {
        if (node.isCompiled()) {
            return node;
        }
        // Two terminal kinds can absorb the projection beneath them. Both are the same rewrite and
        // both exist for the same reason: left as two nodes they become two task graphs, and the
        // projection's output crosses the bus and comes straight back rather than being read where
        // it lies. A chain of Calcs needs no rewrite -- the grouping already turns those into one
        // subtree and the generator into one kernel -- so a maximal chain is a run of Calcs and at
        // most one of these on top.
        if (!(node instanceof BatchExecHashAggregate) && !(node instanceof BatchExecSort)) {
            return node;
        }
        GpuOffloadAssignment mine = node.getGpuOffloadAssignment();
        if (mine == null || !mine.isOffloaded()) {
            return node;
        }
        ExecNode<?> input = node.getInputEdges().get(0).getSource();
        if (!(input instanceof CommonExecCalc)) {
            return node;
        }
        Integer theirGroup = groupOf.get(input);
        if (theirGroup == null || !theirGroup.equals(mine.groupId())) {
            return node;
        }
        CommonExecCalc calc = (CommonExecCalc) input;
        if (node instanceof BatchExecSort) {
            BatchExecSort sort = (BatchExecSort) node;
            BatchExecAcceleratedSort fusedSort =
                    new BatchExecAcceleratedSort(
                            calc,
                            sort,
                            InputProperty.DEFAULT,
                            (RowType) sort.getOutputType(),
                            "AcceleratedSort("
                                    + calc.getDescription()
                                    + ", "
                                    + sort.getDescription()
                                    + ")");
            fusedSort.setInputEdges(
                    Collections.singletonList(
                            ExecEdge.builder()
                                    .source(calc.getInputEdges().get(0).getSource())
                                    .target(fusedSort)
                                    .build()));
            fusedSort.setGpuOffloadAssignment(mine);
            return fusedSort;
        }
        if (!fuseAggregate) {
            return node;
        }
        BatchExecHashAggregate aggregate = (BatchExecHashAggregate) node;
        BatchExecAcceleratedGroup fused =
                new BatchExecAcceleratedGroup(
                        calc,
                        aggregate,
                        InputProperty.DEFAULT,
                        (RowType) aggregate.getOutputType(),
                        "AcceleratedGroup("
                                + calc.getDescription()
                                + ", "
                                + aggregate.getDescription()
                                + ")");
        fused.setInputEdges(
                Collections.singletonList(
                        ExecEdge.builder()
                                .source(calc.getInputEdges().get(0).getSource())
                                .target(fused)
                                .build()));
        fused.setGpuOffloadAssignment(mine);
        return fused;
    }

    private static Integer merge(
            Map<Integer, List<ExecNode<?>>> groups,
            Map<ExecNode<?>, Integer> groupOf,
            Integer a,
            Integer b) {
        List<ExecNode<?>> from = groups.get(b);
        List<ExecNode<?>> into = groups.get(a);
        if (from.size() > into.size()) {
            return merge(groups, groupOf, b, a);
        }
        into.addAll(from);
        for (ExecNode<?> node : from) {
            groupOf.put(node, a);
        }
        groups.remove(b);
        return a;
    }

    /**
     * Rows the subtree will see, taken as the largest estimate among its members.
     *
     * <p>Members of one group run as a single kernel over the same batches, and a filter can only
     * narrow what follows it, so the widest member is what the kernel is actually sized against.
     * Unknown wins over any number: a group holding one node the planner could not estimate is a
     * group whose total work is not known, and guessing there would be worse than skipping the
     * gate.
     */
    private static double rowCountOf(List<ExecNode<?>> members) {
        double widest = 0.0;
        for (ExecNode<?> member : members) {
            double rows = rowCountOf(member);
            if (rows == GpuOffloadExecNode.UNKNOWN_ROW_COUNT) {
                return GpuOffloadExecNode.UNKNOWN_ROW_COUNT;
            }
            widest = Math.max(widest, rows);
        }
        return widest;
    }

    private static double rowCountOf(ExecNode<?> node) {
        return node.getEstimatedRowCount();
    }

    /**
     * Marks a group's work as needing the CPU's floating-point rounding, where the difference could
     * be observed.
     *
     * <p>An accelerator may evaluate {@code a * b + c} as one fused operation with a single
     * rounding. That is a more accurate answer than Flink's two roundings and a <em>different</em>
     * one, and unlike a transcendental it cannot be spotted by looking for a function name:
     * {@code -}, {@code *} and {@code +} are each exact under IEEE 754, and it is their
     * <em>composition</em> that a device is free to round differently. Measured on this project on
     * a query containing no transcendental at all, the CPU returned 24 rows and the device 23.
     *
     * <p>So the question is not what the expression contains but where its value goes. If it
     * reaches only consumers that pass values through, a difference in the last bits changes an
     * output column and nothing else. If it reaches a filter, a join condition, a grouping key, a
     * sort key or a limit, it decides which rows come back — and then the same SQL answers
     * differently depending on which machine ran it.
     *
     * <p>{@link NumericalProvenance} already answers that question for transcendentals, and it
     * answers this one unchanged: a {@code Calc} with a condition, a grouped aggregate and anything
     * it does not recognise — a sort, a join, a limit, a rank — are all not value-only.
     *
     * <p>Two reasons to require it, and the first is not about reach at all. When
     * {@code approximate-projections} is off — the default — the deployment has asked for values
     * that match Flink's, and a fused multiply-add does not match them whether or not anything
     * downstream looks. When it is on, value drift is accepted and only membership is left to
     * protect, which is the reach question below.
     *
     * <p>Deliberately per group rather than per column. Tracking which output column a consumer
     * actually keys on would let a query that sorts on an untouched column keep its fused
     * arithmetic; not tracking it costs that query the fused instruction and costs a wrong answer
     * nothing. The cheap direction here is the safe one.
     *
     * <p>This marks rather than refuses. A provider that can honour the request offloads the
     * subtree and rounds the way Flink does; one that cannot must decline it.
     */
    private static GpuOffloadDecision.Verdict requireStrictArithmeticIfObservable(
            GpuOffloadDecision.Verdict verdict,
            List<ExecNode<?>> members,
            Map<ExecNode<?>, List<ExecNode<?>>> consumers,
            boolean approximateProjectionsAllowed) {
        if (verdict.work() == null || verdict.work().requiresStrictArithmetic()) {
            return verdict;
        }
        if (!approximateProjectionsAllowed) {
            // The deployment has not opted into values that differ from Flink's, and a fused
            // multiply-add differs from Flink's. approximate-projections governs the library
            // functions -- EXP, LN, SIN -- because those are what the analysis can name, but the
            // guarantee it describes is about output values, and arithmetic fusion breaks the same
            // guarantee without any of those appearing in the expression. Nothing downstream has
            // to observe it for this to matter: the user asked for the CPU's numbers.
            return verdict.withWork(verdict.work().withStrictArithmetic());
        }
        for (ExecNode<?> member : members) {
            if (!NumericalProvenance.reachesOnlyValueConsumers(member, consumers)) {
                return verdict.withWork(verdict.work().withStrictArithmetic());
            }
        }
        return verdict;
    }

    private static void assign(ExecNode<?> node, GpuOffloadAssignment assignment) {
        node.setGpuOffloadAssignment(assignment);
    }

    private static List<ExecNode<?>> inputsOf(ExecNode<?> node) {
        List<ExecNode<?>> inputs = new ArrayList<>();
        node.getInputEdges().forEach(edge -> inputs.add(edge.getSource()));
        return inputs;
    }

    /** Producers before consumers. */
    private static List<ExecNode<?>> topologicalOrder(ExecNodeGraph graph) {
        List<ExecNode<?>> order = new ArrayList<>();
        AbstractExecNodeExactlyOnceVisitor visitor =
                new AbstractExecNodeExactlyOnceVisitor() {
                    @Override
                    protected void visitNode(ExecNode<?> node) {
                        visitInputs(node);
                        order.add(node);
                    }
                };
        graph.getRootNodes().forEach(node -> node.accept(visitor));
        return order;
    }

    /**
     * The forward edges, which an {@code ExecNode} graph does not carry.
     *
     * <p>Each node lists its inputs, so the consumers of a node have to be collected by walking
     * every node once and inverting. Identity-keyed, like the rest of the maps here: two exec nodes
     * can be equal without being the same node in the graph.
     */
    private static Map<ExecNode<?>, List<ExecNode<?>>> consumersOf(List<ExecNode<?>> order) {
        Map<ExecNode<?>, List<ExecNode<?>>> consumers = new IdentityHashMap<>();
        for (ExecNode<?> node : order) {
            for (ExecNode<?> input : inputsOf(node)) {
                consumers.computeIfAbsent(input, unused -> new ArrayList<>()).add(node);
            }
        }
        return consumers;
    }

    private static Map<ExecNode<?>, Integer> countConsumers(List<ExecNode<?>> order) {
        // Identity, like the rest of the maps here and like Flink's own exactly-once visitor:
        // ExecNode does not override equals, and two structurally identical nodes are still
        // distinct plan positions.
        Map<ExecNode<?>, Integer> counts = new IdentityHashMap<>();
        for (ExecNode<?> node : order) {
            for (ExecNode<?> input : inputsOf(node)) {
                counts.merge(input, 1, Integer::sum);
            }
        }
        return counts;
    }

    /**
     * Records, for every source that could be read by a device, what came back when it was asked.
     *
     * <p>Separate from the operator pass and deliberately without a group. A scan that joined the
     * grouping would be placed on a device alongside the operator above it, and the operator above
     * it reads {@code RowData} — so the rows would come off the device one at a time, which is the
     * cost the whole exercise exists to remove, paid to no purpose. Until an operator can consume
     * a device-resident batch, the honest verdict for every scan is a decline, and what this pass
     * buys is that the decline is <em>visible</em>: EXPLAIN was previously silent about sources
     * altogether, so a reader could not tell an unsupported format from a supported one nothing
     * had asked about.
     *
     * <p>A source that offers no scan at all — a different connector, an unreadable format — is
     * left without an assignment, exactly as before, so EXPLAIN says nothing about it and no plan
     * moves.
     */
    private void offerScans(
            List<ExecNode<?>> order,
            Map<ExecNode<?>, List<ExecNode<?>>> consumers,
            GpuOffloadDecision decision) {
        for (ExecNode<?> node : order) {
            final java.util.Optional<org.apache.flink.table.accelerator.AccelScan> scan =
                    node.deviceScan();
            if (!scan.isPresent()) {
                continue;
            }
            final RowCost cost =
                    DeviceScanPlacement.decide(scan.get().format(), consumerKind(node, consumers));
            assign(
                    node,
                    new GpuOffloadAssignment(
                            NO_GROUP, decision.forSubtree(Collections.singletonList(cost))));
        }
    }

    /**
     * What reads this scan, in the only three kinds that change the answer.
     *
     * <p>A scan with several consumers takes the best of them: if any consumer can use the batch
     * as it is, the read pays for itself there and the others are served by a transition they
     * would have paid for anyway.
     */
    private static DeviceScanPlacement.Consumer consumerKind(
            ExecNode<?> scan, Map<ExecNode<?>, List<ExecNode<?>>> consumers) {
        DeviceScanPlacement.Consumer best = DeviceScanPlacement.Consumer.NONE_OFFLOADED;
        for (ExecNode<?> consumer : consumers.getOrDefault(scan, Collections.emptyList())) {
            final GpuOffloadAssignment assignment = consumer.getGpuOffloadAssignment();
            if (assignment == null || !assignment.isOffloaded()) {
                continue;
            }
            if (consumer.consumesDeviceInput()) {
                return DeviceScanPlacement.Consumer.OFFLOADED_READS_DEVICE;
            }
            best = DeviceScanPlacement.Consumer.OFFLOADED_READS_ROWS;
        }
        return best;
    }
}
