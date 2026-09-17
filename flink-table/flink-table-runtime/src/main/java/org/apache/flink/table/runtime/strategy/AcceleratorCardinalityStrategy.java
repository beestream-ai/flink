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

package org.apache.flink.table.runtime.strategy;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.scheduler.adaptivebatch.BlockingResultInfo;
import org.apache.flink.runtime.scheduler.adaptivebatch.OperatorsFinished;
import org.apache.flink.runtime.scheduler.adaptivebatch.StreamGraphOptimizationStrategy;
import org.apache.flink.streaming.api.graph.StreamGraphContext;
import org.apache.flink.streaming.api.graph.util.ImmutableStreamEdge;
import org.apache.flink.streaming.api.graph.util.ImmutableStreamGraph;
import org.apache.flink.streaming.api.graph.util.ImmutableStreamNode;
import org.apache.flink.table.runtime.gpu.GpuOrCpuCalcOperatorFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Tells an offload node how big its input turned out to be, before it is scheduled.
 *
 * <p>Whether a subtree <em>can</em> run on a device is a property of the query and is settled in
 * the planner. Whether running it there repays setting the device up is a property of how much work
 * arrives, and the planner is the worst place in the system to ask: it has catalog statistics and a
 * configured parallelism, and both can be wrong by orders of magnitude. By the time the input has
 * been produced, the byte count is exact and the parallelism is the one the scheduler actually
 * chose. This strategy carries that pair to the operator factory, which does the comparing. M4.3.
 *
 * <p>It needs no change to {@code flink-runtime}. {@link StreamGraphOptimizationStrategy} is a
 * public extension point registered through configuration, and adaptive join already reaches
 * operator factories through it the same way — {@code getOperatorFactory} hands back the live
 * instance sitting in the StreamGraph, and mutating it before the JobGraph is built is the
 * supported mechanism rather than a trick.
 *
 * <h2>An upper bound, reached by walking forward</h2>
 *
 * <p>An offloadable Calc is never the direct consumer of a blocking result. Flink puts the
 * distribution on the edge into the operator that needs it — an aggregate, a join — and the Calc
 * follows on a forward edge behind it. So measuring the finished result and looking only at its
 * immediate consumer finds nothing, every time.
 *
 * <p>What the walk gives instead is a bound, and the bound is what the question needs. Rows only
 * ever shrink along a chain of single-input operators: a filter drops them, an aggregate collapses
 * them, nothing in that shape invents any. So the bytes a finished result produced cap what every
 * such consumer downstream of it can possibly read. If even that cap is under one batch, the node
 * certainly cannot fill one, and refusing is sound rather than approximate.
 *
 * <p>The walk therefore stops at any node with more than one input — a join or a union can produce
 * more rows than either side gave it, and the bound would stop being one.
 *
 * <h2>What it cannot see</h2>
 *
 * <p>A chain with no blocking result above it at all. A Calc reading straight from a chained source
 * has nothing finished to measure, so the planner's estimate stands. There is no result to measure
 * until something has finished writing one.
 *
 * <p>It also runs only under the AdaptiveBatch scheduler, which is the batch default but is
 * mutually exclusive with {@code table.exec.accelerator.require-device} — AdaptiveBatch flattens
 * the external-resource requirement to UNKNOWN before the ResourceManager sees it, so that option
 * needs {@code jobmanager.scheduler: Default}, and under Default this strategy never runs.
 */
@Internal
public class AcceleratorCardinalityStrategy implements StreamGraphOptimizationStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(AcceleratorCardinalityStrategy.class);

    @Override
    public boolean onOperatorsFinished(
            OperatorsFinished operatorsFinished, StreamGraphContext context) {
        ImmutableStreamGraph streamGraph = context.getStreamGraph();
        Map<Integer, List<BlockingResultInfo>> results = operatorsFinished.getResultInfoMap();

        for (Integer finishedNodeId : operatorsFinished.getFinishedStreamNodeIds()) {
            long bytes = bytesProduced(results.get(finishedNodeId));
            if (bytes <= 0) {
                continue;
            }
            for (ImmutableStreamNode consumer :
                    boundedConsumersOf(streamGraph, finishedNodeId, new HashSet<>())) {
                GpuOrCpuCalcOperatorFactory offload =
                        (GpuOrCpuCalcOperatorFactory) consumer.getOperatorFactory();
                int parallelism = consumer.getParallelism();
                if (parallelism <= 0) {
                    // AdaptiveBatch reports -1 for a vertex whose parallelism it has not chosen
                    // yet. Standing a 1 in for it computes the work of a serial vertex that is
                    // about to be parallel, which overstates what each subtask sees and lets
                    // through the case this gate exists to refuse. Saying nothing leaves the
                    // planner's estimate in place, which is the honest answer until the number
                    // exists. A deployment saw the alternative, logging "over -1 subtasks the
                    // scheduler chose".
                    LOG.debug(
                            "Accelerator cardinality for stream node {}: parallelism undecided, leaving the planner's estimate",
                            consumer.getId());
                    continue;
                }
                long rows = rowsPerSubtask(bytes, parallelism, offload.bytesPerRow());
                offload.refineCardinality(
                        rows,
                        "at most "
                                + bytes
                                + " bytes reached this chain, over "
                                + parallelism
                                + " subtasks the scheduler chose");
                LOG.debug(
                        "Accelerator cardinality for stream node {}: at most {} rows a subtask, from {} bytes at parallelism {}",
                        consumer.getId(),
                        rows,
                        bytes,
                        parallelism);
            }
        }
        return true;
    }

    /**
     * Every offload node whose input is bounded by what the given node produced.
     *
     * <p>Forward through single-input consumers only. Multi-input nodes end the walk because a join
     * can emit more rows than it read, and past one the byte count stops bounding anything.
     */
    private static List<ImmutableStreamNode> boundedConsumersOf(
            ImmutableStreamGraph streamGraph, Integer nodeId, Set<Integer> visited) {
        List<ImmutableStreamNode> found = new ArrayList<>();
        if (!visited.add(nodeId)) {
            return found;
        }
        for (ImmutableStreamEdge edge : streamGraph.getStreamNode(nodeId).getOutEdges()) {
            ImmutableStreamNode consumer = streamGraph.getStreamNode(edge.getTargetId());
            if (consumer.getInEdges().size() > 1) {
                continue;
            }
            if (consumer.getOperatorFactory() instanceof GpuOrCpuCalcOperatorFactory) {
                found.add(consumer);
            }
            found.addAll(boundedConsumersOf(streamGraph, consumer.getId(), visited));
        }
        return found;
    }

    /**
     * Rows one subtask will see, from the bytes the input really produced.
     *
     * <p>The number the scheduler settled on is the divisor, which is the whole point of doing this
     * here: {@code table.exec.resource.default-parallelism} is a request, and under AdaptiveBatch
     * it is routinely not what the vertex ends up running at.
     *
     * <p>Bytes are what a blocking result records; rows have to be inferred, and the divisor used
     * is the staging width the operator is already sized against. That over-states the width of an
     * upstream row, so the row count comes out low and the gate errs toward the CPU — the side that
     * is always correct.
     */
    static long rowsPerSubtask(long bytesProduced, int parallelism, int bytesPerRow) {
        // Callers filter an undecided parallelism out before here; the floor guards a caller
        // that forgets, and is deliberately not a way of treating -1 as serial.
        int subtasks = Math.max(1, parallelism);
        int width = Math.max(1, bytesPerRow);
        return bytesProduced / width / subtasks;
    }

    private static long bytesProduced(List<BlockingResultInfo> infos) {
        if (infos == null) {
            return 0L;
        }
        long total = 0L;
        for (BlockingResultInfo info : infos) {
            total += info.getNumBytesProduced();
        }
        return total;
    }
}
