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

package org.apache.flink.table.planner.plan.nodes.exec.batch;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.table.accelerator.AccelNode;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.accelerator.AccelIrBuilder;
import org.apache.flink.table.planner.plan.gpu.GpuOffloadAssignment;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecCalc;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.runtime.accelerator.AcceleratedCalcSpec;
import org.apache.flink.table.runtime.gpu.ChainedCalcAggregateFactory;
import org.apache.flink.table.runtime.gpu.GpuOrCpuCalcOperatorFactory;
import org.apache.flink.table.runtime.operators.CodeGenOperatorFactory;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import java.util.Collections;
import java.util.Optional;

/**
 * One {@link org.apache.flink.table.planner.plan.nodes.exec.ExecNode} owning a projection and the
 * sort it feeds, emitting one operator.
 *
 * <p>The sibling of {@link BatchExecAcceleratedGroup}, and it exists for the same reason: two
 * offloadable nodes left as two nodes become two task graphs, and the intermediate crosses the bus
 * and comes straight back.
 *
 * <h2>What the pair costs unfused</h2>
 *
 * <p>Each of the two operators stages its input into device buffers and, when it is done, builds a
 * {@code BinaryRowData} for every row it emits. The second operator's first act is to take those
 * rows apart again. §T12 attributed <b>616 ms of a 954 ms offloaded sort to building four million
 * binary rows</b>, against 15.7 ms for the ordering itself — so a fused pair removes an entire
 * drain and an entire stage, which is most of what the pair spends.
 *
 * <p>Fused, the kernel writes its columns and {@code cudf::sorted_order} orders the key column
 * where it lies, in the same task graph. One stage in, one drain out.
 *
 * <h2>Why the grouping already knows which pairs qualify</h2>
 *
 * <p>{@code GpuOffloadProcessor} finds maximal connected subtrees whose nodes can all run on a
 * device, and only lets a node join its consumer's group when it has exactly one consumer. A pair
 * in one group is therefore already known to be adjacent, offloadable and unshared; this node is
 * the rewrite that makes the graph say so.
 *
 * <h2>The CPU half</h2>
 *
 * <p>No device path is offered here — this is what runs when there is no device or a provider
 * declines. It chains the generated Calc ahead of Flink's own {@code SortOperator} inside one
 * operator, which is what the pair did before this node existed and produces identical results.
 */
public class BatchExecAcceleratedSort extends ExecNodeBase<RowData>
        implements BatchExecNode<RowData>, SingleTransformationTranslator<RowData> {

    public static final String ACCELERATED_SORT_TRANSFORMATION = "accelerated-sort";

    private final CommonExecCalc calc;
    private final BatchExecSort sort;

    public BatchExecAcceleratedSort(
            CommonExecCalc calc,
            BatchExecSort sort,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        super(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(BatchExecAcceleratedSort.class),
                sort.getPersistedConfig(),
                Collections.singletonList(inputProperty),
                outputType,
                description);
        this.calc = calc;
        this.sort = sort;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        final ExecEdge inputEdge = getInputEdges().get(0);
        final Transformation<RowData> inputTransform =
                (Transformation<RowData>) inputEdge.translateToPlan(planner);

        final CodeGeneratorContext calcCtx =
                new CodeGeneratorContext(config, planner.getFlinkContext().getClassLoader());
        final CodeGenOperatorFactory<RowData> calcFactory =
                calc.generateCalcOperator(calcCtx, inputTransform);
        final BatchExecSort.GeneratedSort generated = sort.generateSortOperator(planner, config);

        return ExecNodeUtil.createOneInputTransformation(
                inputTransform,
                createTransformationMeta(ACCELERATED_SORT_TRANSFORMATION, config),
                substitute(
                        config,
                        (RowType) inputEdge.getOutputType(),
                        new ChainedCalcAggregateFactory(calcFactory, generated.factory)),
                InternalTypeInfo.of((RowType) getOutputType()),
                inputTransform.getParallelism(),
                generated.managedMemory,
                false);
    }

    /**
     * Offers the fused pair to an accelerator as one subtree, or returns the chained CPU pair.
     *
     * <p>The subtree is a sort <em>over a projection</em> rather than over a leaf, which is the
     * whole difference: a provider handed {@code AccelSort(AccelInput)} has no way to know a
     * projection preceded it.
     */
    private StreamOperatorFactory<RowData> substitute(
            ExecNodeConfig config, RowType inputType, StreamOperatorFactory<RowData> onCpu) {
        GpuOffloadAssignment assignment = getGpuOffloadAssignment();
        if (assignment == null || !assignment.isOffloaded() || config.isCompiled()) {
            // M4.4: a compiled plan does not offload, and the fused node is no exception.
            return onCpu;
        }
        Optional<AccelNode> projection =
                AccelIrBuilder.buildCalc(
                        calc.projection(),
                        calc.condition(),
                        inputType,
                        (RowType) calc.getOutputType());
        if (!projection.isPresent()) {
            return onCpu;
        }
        Optional<AccelNode> subtree =
                AccelIrBuilder.buildSort(
                        sort.sortSpec(), projection.get(), (RowType) getOutputType());
        if (!subtree.isPresent()) {
            return onCpu;
        }
        AcceleratedCalcSpec spec =
                new AcceleratedCalcSpec(
                        subtree.get(),
                        assignment.verdict().work(),
                        (RowType) getOutputType(),
                        config.get(ExecutionConfigOptions.TABLE_EXEC_ACCELERATOR_BATCH_SIZE),
                        config.get(ExecutionConfigOptions.TABLE_EXEC_ACCELERATOR_MIN_SPEEDUP));
        return new GpuOrCpuCalcOperatorFactory(spec, onCpu);
    }
}
