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
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.table.accelerator.AccelNode;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.accelerator.AccelIrBuilder;
import org.apache.flink.table.planner.plan.gpu.GpuOffloadAssignment;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecCalc;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.runtime.accelerator.AcceleratedCalcSpec;
import org.apache.flink.table.runtime.gpu.ChainedCalcAggregateOperator;
import org.apache.flink.table.runtime.gpu.GpuOrCpuCalcOperatorFactory;
import org.apache.flink.table.runtime.operators.CodeGenOperatorFactory;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * One {@link ExecNode} owning a projection and the aggregate it feeds, emitting one operator.
 *
 * <p>This is M4.1. Fusion used to happen during translation: {@code BatchExecHashAggregate} reached
 * sideways at its input, absorbed it, and emitted a single operator for both. The absorbed {@code
 * Calc} stayed in the graph while producing no transformation of its own, so the node count and the
 * transformation count disagreed, and so did {@code EXPLAIN} and the vertex list of the JobGraph it
 * produced. That was recorded as blocker C1. Fusion is now a rewrite the offload processor
 * performs, and a fused pair is one node.
 *
 * <h2>Why fuse at all</h2>
 *
 * <p>Not for the operator count. M4.0 measured a fused pair against an unfused one on an RTX 5070
 * Ti and the answer is entirely about where the intermediate lives: as two device task graphs the
 * projection's output crosses the bus and comes straight back, and as one task graph it never
 * leaves. That is worth between 1.06x and 4.53x, decided almost entirely by how many columns the
 * projection emits — 4.5x at twenty columns, and still 2.05x against arithmetic heavy enough to
 * dominate. Measured on the same hardware, fusing the two <em>CPU</em> operators is worth nothing
 * at all: 0.6% with overlapping ranges, because Flink's own operator chaining already does it.
 *
 * <p>So this node exists for a device route that M1.1 removed and that nothing has yet restored.
 * Until it is, the node emits the generated Calc chained ahead of the generated aggregate, which is
 * what the pair did before and produces identical results. What has changed is that there is now
 * one place for the device plan to go, and it is a node rather than a side effect of translation.
 */
public class BatchExecAcceleratedGroup extends ExecNodeBase<RowData>
        implements BatchExecNode<RowData>, SingleTransformationTranslator<RowData> {

    public static final String ACCELERATED_GROUP_TRANSFORMATION = "accelerated-group";

    private final CommonExecCalc calc;
    private final BatchExecHashAggregate aggregate;

    public BatchExecAcceleratedGroup(
            CommonExecCalc calc,
            BatchExecHashAggregate aggregate,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        super(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(BatchExecAcceleratedGroup.class),
                aggregate.getPersistedConfig(),
                Collections.singletonList(inputProperty),
                outputType,
                description);
        this.calc = calc;
        this.aggregate = aggregate;
    }

    /** The nodes this one absorbed, producers first. Inspected by EXPLAIN and by tests. */
    public List<ExecNode<?>> getMemberExecNodes() {
        return Arrays.asList(calc, aggregate);
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
        final BatchExecHashAggregate.GeneratedAggregate generated =
                aggregate.generateAggregateOperator(planner, config);

        return ExecNodeUtil.createOneInputTransformation(
                inputTransform,
                createTransformationMeta(ACCELERATED_GROUP_TRANSFORMATION, config),
                substitute(
                        planner,
                        config,
                        inputTransform,
                        new ChainedOnlyFactory(calcFactory, generated.factory)),
                InternalTypeInfo.of((RowType) getOutputType()),
                inputTransform.getParallelism(),
                generated.managedMemory,
                false);
    }

    /**
     * Offers the fused pair to an accelerator, or returns the chained CPU operator unchanged.
     *
     * <p>This is the device route M1.1 removed and M4.1 built the node for. The IR describes the
     * whole pair -- an {@link org.apache.flink.table.accelerator.AccelAggregate} over the
     * projection's {@code AccelProject} -- so a provider can run both halves in one device plan
     * with the intermediate never leaving the card, which M4.0 measured at 1.06x to 4.53x depending
     * on how wide that intermediate is.
     *
     * <p>Both implementations travel in the JobGraph exactly as they do for a Calc, and the same
     * factory decides between them on the TaskManager. Nothing here asks whether a device exists.
     */
    private StreamOperatorFactory<RowData> substitute(
            PlannerBase planner,
            ExecNodeConfig config,
            Transformation<RowData> inputTransform,
            StreamOperatorFactory<RowData> onCpu) {
        GpuOffloadAssignment assignment = getGpuOffloadAssignment();
        if (assignment == null || !assignment.isOffloaded() || config.isCompiled()) {
            // M4.4: a compiled plan does not offload, and the fused node is no exception.
            return onCpu;
        }
        RowType inputType = (RowType) getInputEdges().get(0).getOutputType();
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
                AccelIrBuilder.buildAggregate(
                        aggregate.grouping(),
                        aggregate.aggCalls(),
                        projection.get(),
                        (RowType) getOutputType());
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

    /**
     * Runs both halves inside one operator, with the projection's output wired into the aggregate.
     *
     * <p>No device path is offered, because the fused pair has none to offer: M1.1 removed it. The
     * members still have to run, and running them as one chained operator is what the pair did
     * before this node existed.
     */
    private static final class ChainedOnlyFactory extends AbstractStreamOperatorFactory<RowData> {

        private static final long serialVersionUID = 1L;

        private final StreamOperatorFactory<RowData> calcFactory;
        private final StreamOperatorFactory<RowData> aggregateFactory;

        private ChainedOnlyFactory(
                StreamOperatorFactory<RowData> calcFactory,
                StreamOperatorFactory<RowData> aggregateFactory) {
            this.calcFactory = calcFactory;
            this.aggregateFactory = aggregateFactory;
        }

        @Override
        public <T extends StreamOperator<RowData>> T createStreamOperator(
                StreamOperatorParameters<RowData> parameters) {
            return ChainedCalcAggregateOperator.chain(
                    parameters, calcFactory, aggregateFactory, processingTimeService);
        }

        @Override
        public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
            return aggregateFactory.getStreamOperatorClass(classLoader);
        }
    }
}
