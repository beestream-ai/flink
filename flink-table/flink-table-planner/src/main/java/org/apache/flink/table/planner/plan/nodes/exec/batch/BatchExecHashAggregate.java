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

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.agg.batch.AggWithoutKeysCodeGenerator;
import org.apache.flink.table.planner.codegen.agg.batch.HashAggCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.fusion.OpFusionCodegenSpecGenerator;
import org.apache.flink.table.planner.plan.fusion.generator.OneInputOpFusionCodegenSpecGenerator;
import org.apache.flink.table.planner.plan.fusion.spec.HashAggFusionCodegenSpec;
import org.apache.flink.table.planner.plan.gpu.GpuAggregateSupport;
import org.apache.flink.table.planner.plan.gpu.GpuOffloadAssignment;
import org.apache.flink.table.planner.plan.gpu.GpuOffloadDecision;
import org.apache.flink.table.planner.plan.gpu.GpuOffloadOptions;
import org.apache.flink.table.planner.plan.gpu.RowCost;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecCalc;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.utils.AggregateInfoList;
import org.apache.flink.table.planner.plan.utils.AggregateUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.runtime.generated.GeneratedOperator;
import org.apache.flink.table.runtime.gpu.GpuAggregateSpec;
import org.apache.flink.table.runtime.gpu.GpuKernelSource;
import org.apache.flink.table.runtime.gpu.GpuOrCpuAggregateOperatorFactory;
import org.apache.flink.table.runtime.operators.CodeGenOperatorFactory;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.calcite.rel.core.AggregateCall;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/** Batch {@link ExecNode} for hash-based aggregate operator. */
@ExecNodeMetadata(
        name = "batch-exec-hash-aggregate",
        version = 1,
        producedTransformations = BatchExecHashAggregate.HASH_AGGREGATE_TRANSFORMATION,
        consumedOptions = {
            "table.exec.resource.hash-agg.memory",
            "table.exec.sort.max-num-file-handles",
            "table.exec.spill-compression.enabled",
            "table.exec.spill-compression.block-size"
        },
        minPlanVersion = FlinkVersion.v2_0,
        minStateVersion = FlinkVersion.v2_0)
public class BatchExecHashAggregate extends ExecNodeBase<RowData>
        implements BatchExecNode<RowData>, SingleTransformationTranslator<RowData> {

    public static final String HASH_AGGREGATE_TRANSFORMATION = "hash-aggregate";
    public static final String FIELD_NAME_GROUPING = "grouping";
    public static final String FIELD_NAME_AUX_GROUPING = "auxGrouping";
    public static final String FIELD_NAME_AGG_CALLS = "aggCalls";
    public static final String FIELD_NAME_AGG_INPUT_ROW_TYPE = "aggInputRowType";
    public static final String FIELD_NAME_IS_MERGE = "isMerge";
    public static final String FIELD_NAME_IS_FINAL = "isFinal";
    public static final String FIELD_NAME_SUPPORT_ADAPTIVE_LOCAL_HASH_AGG =
            "supportAdaptiveLocalHashAgg";

    @JsonProperty(FIELD_NAME_GROUPING)
    private final int[] grouping;

    @JsonProperty(FIELD_NAME_AUX_GROUPING)
    private final int[] auxGrouping;

    @JsonProperty(FIELD_NAME_AGG_CALLS)
    private final AggregateCall[] aggCalls;

    @JsonProperty(FIELD_NAME_AGG_INPUT_ROW_TYPE)
    private final RowType aggInputRowType;

    @JsonProperty(FIELD_NAME_IS_MERGE)
    private final boolean isMerge;

    @JsonProperty(FIELD_NAME_IS_FINAL)
    private final boolean isFinal;

    @JsonProperty(FIELD_NAME_SUPPORT_ADAPTIVE_LOCAL_HASH_AGG)
    private final boolean supportAdaptiveLocalHashAgg;

    public BatchExecHashAggregate(
            ReadableConfig tableConfig,
            int[] grouping,
            int[] auxGrouping,
            AggregateCall[] aggCalls,
            RowType aggInputRowType,
            boolean isMerge,
            boolean isFinal,
            boolean supportAdaptiveLocalHashAgg,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        super(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(BatchExecHashAggregate.class),
                ExecNodeContext.newPersistedConfig(BatchExecHashAggregate.class, tableConfig),
                Collections.singletonList(inputProperty),
                outputType,
                description);
        this.grouping = grouping;
        this.auxGrouping = auxGrouping;
        this.aggCalls = aggCalls;
        this.aggInputRowType = aggInputRowType;
        this.isMerge = isMerge;
        this.isFinal = isFinal;
        this.supportAdaptiveLocalHashAgg = supportAdaptiveLocalHashAgg;
    }

    @JsonCreator
    public BatchExecHashAggregate(
            @JsonProperty(FIELD_NAME_ID) int id,
            @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
            @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig persistedConfig,
            @JsonProperty(FIELD_NAME_GROUPING) int[] grouping,
            @JsonProperty(FIELD_NAME_AUX_GROUPING) int[] auxGrouping,
            @JsonProperty(FIELD_NAME_AGG_CALLS) AggregateCall[] aggCalls,
            @JsonProperty(FIELD_NAME_AGG_INPUT_ROW_TYPE) RowType aggInputRowType,
            @JsonProperty(FIELD_NAME_IS_MERGE) boolean isMerge,
            @JsonProperty(FIELD_NAME_IS_FINAL) boolean isFinal,
            @JsonProperty(FIELD_NAME_SUPPORT_ADAPTIVE_LOCAL_HASH_AGG)
                    boolean supportAdaptiveLocalHashAgg,
            @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
            @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
        super(id, context, persistedConfig, inputProperties, outputType, description);
        this.grouping = grouping;
        this.auxGrouping = auxGrouping;
        this.aggCalls = aggCalls;
        this.aggInputRowType = aggInputRowType;
        this.isMerge = isMerge;
        this.isFinal = isFinal;
        this.supportAdaptiveLocalHashAgg = supportAdaptiveLocalHashAgg;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        final ExecEdge inputEdge = getInputEdges().get(0);
        final CommonExecCalc fusedCalc = fusedCalc();
        // When a Calc is fused into this node it produces no transformation of its own: this
        // operator consumes what the Calc would have consumed, and runs the Calc's work itself --
        // as a kernel writing to a device buffer the contraction then reads, or as the generated
        // Calc operator chained ahead of the generated aggregate when there is no device.
        final Transformation<RowData> inputTransform =
                (Transformation<RowData>)
                        (fusedCalc == null
                                ? inputEdge.translateToPlan(planner)
                                : fusedCalc.getInputEdges().get(0).translateToPlan(planner));

        final RowType inputRowType = (RowType) inputEdge.getOutputType();
        final RowType outputRowType = (RowType) getOutputType();

        final CodeGeneratorContext ctx =
                new CodeGeneratorContext(config, planner.getFlinkContext().getClassLoader());

        final AggregateInfoList aggInfos =
                AggregateUtil.transformToBatchAggregateInfoList(
                        planner.getTypeFactory(),
                        aggInputRowType,
                        JavaScalaConversionUtil.toScala(Arrays.asList(aggCalls)),
                        null, // aggCallNeedRetractions
                        null); // orderKeyIndexes

        final long managedMemory;
        final GeneratedOperator<OneInputStreamOperator<RowData, RowData>> generatedOperator;
        if (grouping.length == 0) {
            managedMemory = 0L;
            generatedOperator =
                    AggWithoutKeysCodeGenerator.genWithoutKeys(
                            ctx,
                            planner.createRelBuilder(),
                            aggInfos,
                            inputRowType,
                            outputRowType,
                            isMerge,
                            isFinal,
                            "NoGrouping");
        } else {
            managedMemory =
                    config.get(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_HASH_AGG_MEMORY)
                            .getBytes();
            generatedOperator =
                    HashAggCodeGenerator.genWithKeys(
                            ctx,
                            planner.createRelBuilder(),
                            aggInfos,
                            inputRowType,
                            outputRowType,
                            grouping,
                            auxGrouping,
                            isMerge,
                            isFinal,
                            supportAdaptiveLocalHashAgg,
                            config.get(ExecutionConfigOptions.TABLE_EXEC_SORT_MAX_NUM_FILE_HANDLES),
                            config.get(ExecutionConfigOptions.TABLE_EXEC_SPILL_COMPRESSION_ENABLED),
                            (int)
                                    config.get(
                                                    ExecutionConfigOptions
                                                            .TABLE_EXEC_SPILL_COMPRESSION_BLOCK_SIZE)
                                            .getBytes());
        }

        final CodeGenOperatorFactory<RowData> aggregateFactory =
                new CodeGenOperatorFactory<>(generatedOperator);
        return ExecNodeUtil.createOneInputTransformation(
                inputTransform,
                createTransformationMeta(HASH_AGGREGATE_TRANSFORMATION, config),
                wrapForFusedGpuOffload(
                        planner, config, fusedCalc, inputTransform, aggregateFactory),
                InternalTypeInfo.of(outputRowType),
                inputTransform.getParallelism(),
                managedMemory,
                false);
    }

    /**
     * The {@code Calc} feeding this node, when the offload processor put the two in one subtree.
     *
     * <p>Both conditions matter. Sharing a group means the processor judged the pair together and
     * decided the subtree was worth offloading; being a {@code Calc} means there is an expression
     * to fuse. Either missing and this is an ordinary aggregate.
     *
     * @return the fused Calc, or null
     */
    private @Nullable CommonExecCalc fusedCalc() {
        GpuOffloadAssignment mine = getGpuOffloadAssignment();
        if (mine == null || !mine.isOffloaded()) {
            return null;
        }
        ExecNode<?> input = getInputEdges().get(0).getSource();
        if (!(input instanceof CommonExecCalc)) {
            return null;
        }
        GpuOffloadAssignment theirs = input.getGpuOffloadAssignment();
        if (theirs == null || !theirs.isOffloaded() || theirs.groupId() != mine.groupId()) {
            return null;
        }
        return (CommonExecCalc) input;
    }

    /**
     * Builds the operator that carries both halves, or leaves the generated aggregate alone.
     *
     * <p>What is decided here is a property of the query: whether the pair was selected, and
     * whether the Calc's expressions can be written as a kernel. Whether a device exists is not
     * knowable in the client and is settled by {@link GpuOrCpuAggregateOperatorFactory} on arrival.
     */
    private StreamOperatorFactory<RowData> wrapForFusedGpuOffload(
            PlannerBase planner,
            ExecNodeConfig config,
            @Nullable CommonExecCalc fusedCalc,
            Transformation<RowData> inputTransform,
            CodeGenOperatorFactory<RowData> aggregateFactory) {
        if (fusedCalc == null) {
            return aggregateFactory;
        }
        Optional<GpuKernelSource> kernel = fusedCalc.gpuKernel();
        if (!kernel.isPresent()) {
            recordFallback("the fused Calc's expressions cannot be generated as a device kernel");
            return chainedOnly(planner, config, fusedCalc, inputTransform, aggregateFactory);
        }
        int[] sumSources = sumSources();
        if (sumSources == null) {
            recordFallback("the aggregate's outputs do not map one to one onto its calls");
            return chainedOnly(planner, config, fusedCalc, inputTransform, aggregateFactory);
        }
        GpuAggregateSpec spec =
                new GpuAggregateSpec(
                        kernel.get(),
                        sumSources,
                        (RowType) fusedCalc.getOutputType(),
                        (RowType) getOutputType(),
                        planner.getTableConfig().get(GpuOffloadOptions.BATCH_SIZE),
                        getGpuOffloadAssignment().verdict().rowCost());
        return new GpuOrCpuAggregateOperatorFactory(
                spec, calcFactoryFor(planner, config, fusedCalc, inputTransform), aggregateFactory);
    }

    /**
     * The fused pair with no device path: still one operator, because the Calc has no
     * transformation of its own once fused, and still both generated operators.
     */
    private StreamOperatorFactory<RowData> chainedOnly(
            PlannerBase planner,
            ExecNodeConfig config,
            CommonExecCalc fusedCalc,
            Transformation<RowData> inputTransform,
            CodeGenOperatorFactory<RowData> aggregateFactory) {
        return new ChainedOnlyFactory(
                calcFactoryFor(planner, config, fusedCalc, inputTransform), aggregateFactory);
    }

    private CodeGenOperatorFactory<RowData> calcFactoryFor(
            PlannerBase planner,
            ExecNodeConfig config,
            CommonExecCalc fusedCalc,
            Transformation<RowData> inputTransform) {
        CodeGeneratorContext calcCtx =
                new CodeGeneratorContext(config, planner.getFlinkContext().getClassLoader());
        return fusedCalc.generateCalcOperator(calcCtx, inputTransform);
    }

    /**
     * Which Calc output column feeds each aggregate output field.
     *
     * @return the mapping, or null when this node's shape is not the one {@code
     *     GpuAggregateSupport} admitted -- a mismatch here means the two disagree, and the safe
     *     reading is that this is not fusible after all
     */
    @Nullable
    private int[] sumSources() {
        if (aggCalls.length != getOutputType().getChildren().size()) {
            return null;
        }
        int[] sources = new int[aggCalls.length];
        for (int i = 0; i < aggCalls.length; i++) {
            AggregateCall call = aggCalls[i];
            if (call.getArgList().isEmpty()) {
                sources[i] = GpuAggregateSpec.COUNT_STAR;
            } else if (call.getArgList().size() == 1) {
                sources[i] = call.getArgList().get(0);
            } else {
                return null;
            }
        }
        return sources;
    }

    /** Rewrites the verdict so EXPLAIN reports why a selected pair still ran on the CPU. */
    private void recordFallback(String reason) {
        GpuOffloadAssignment assignment = getGpuOffloadAssignment();
        if (assignment != null) {
            setGpuOffloadAssignment(
                    new GpuOffloadAssignment(
                            assignment.groupId(),
                            GpuOffloadDecision.fallback(assignment.verdict(), reason)));
        }
    }

    /**
     * The fused pair on the CPU, with no device path offered at all.
     *
     * <p>Distinct from {@link GpuOrCpuAggregateOperatorFactory}: that one asks the TaskManager and
     * falls back, this one has already established in the client that there is nothing to ask
     * about, because the expressions could not be generated. Both produce the same chained
     * operator, so results are identical either way.
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
            return GpuOrCpuAggregateOperatorFactory.chain(
                    parameters, calcFactory, aggregateFactory, processingTimeService);
        }

        @Override
        public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
            return aggregateFactory.getStreamOperatorClass(classLoader);
        }
    }

    /**
     * Offloadable when this is an ungrouped contraction the device has a primitive for.
     *
     * <p>The test is {@code isMerge}, and only that. A merge aggregate consumes partial
     * accumulators produced by other subtasks, which is not a contraction over rows and has one
     * input row per subtask to work with; there is nothing for a device to do. Everything else
     * reads rows and contracts them.
     *
     * <p>{@code isFinal} is deliberately <em>not</em> tested. Excluding it looks right, since the
     * final half of a two-phase aggregate is the merge, but the two flags are independent: a
     * one-phase aggregate reads rows and is also final, and refusing it would give up the whole
     * shape for no reason. {@code isMerge} already excludes the case that matters.
     */
    @Override
    public boolean supportGpuOffload() {
        return !isMerge;
    }

    /**
     * Costs the contraction, which is deliberately cheap.
     *
     * <p>An aggregate alone never clears the offload floor and should not: contracting a row is a
     * multiply and an add. What makes the pair worth offloading is the <code>Calc</code> beneath
     * it, and the processor sums a subtree's costs precisely so a cheap contraction rides along
     * with the expensive expression feeding it rather than being judged on its own.
     */
    @Override
    public RowCost estimateRowCost() {
        if (isMerge) {
            return RowCost.ineligible("a merge aggregate consumes partials, not rows");
        }
        return GpuAggregateSupport.estimate(grouping, auxGrouping, aggCalls);
    }

    @Override
    public boolean supportFusionCodegen() {
        return true;
    }

    @Override
    protected OpFusionCodegenSpecGenerator translateToFusionCodegenSpecInternal(
            PlannerBase planner, ExecNodeConfig config, CodeGeneratorContext parentCtx) {
        OpFusionCodegenSpecGenerator input =
                getInputEdges().get(0).translateToFusionCodegenSpec(planner, parentCtx);

        final AggregateInfoList aggInfos =
                AggregateUtil.transformToBatchAggregateInfoList(
                        planner.getTypeFactory(),
                        aggInputRowType,
                        JavaScalaConversionUtil.toScala(Arrays.asList(aggCalls)),
                        null, // aggCallNeedRetractions
                        null); // orderKeyIndexes
        long managedMemory =
                config.get(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_HASH_AGG_MEMORY).getBytes();
        if (grouping.length == 0) {
            managedMemory = 0L;
        }

        OpFusionCodegenSpecGenerator hashAggSpecGenerator =
                new OneInputOpFusionCodegenSpecGenerator(
                        input,
                        managedMemory,
                        (RowType) getOutputType(),
                        new HashAggFusionCodegenSpec(
                                new CodeGeneratorContext(
                                        config,
                                        planner.getFlinkContext().getClassLoader(),
                                        parentCtx),
                                planner.createRelBuilder(),
                                aggInfos,
                                grouping,
                                auxGrouping,
                                isFinal,
                                isMerge,
                                supportAdaptiveLocalHashAgg,
                                config.get(
                                        ExecutionConfigOptions
                                                .TABLE_EXEC_SORT_MAX_NUM_FILE_HANDLES),
                                config.get(
                                        ExecutionConfigOptions
                                                .TABLE_EXEC_SPILL_COMPRESSION_ENABLED),
                                (int)
                                        config.get(
                                                        ExecutionConfigOptions
                                                                .TABLE_EXEC_SPILL_COMPRESSION_BLOCK_SIZE)
                                                .getBytes()));
        input.addOutput(1, hashAggSpecGenerator);
        return hashAggSpecGenerator;
    }
}
