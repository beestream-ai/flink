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
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
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
import org.apache.flink.table.planner.plan.gpu.RowCost;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.utils.AggregateInfoList;
import org.apache.flink.table.planner.plan.utils.AggregateUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.runtime.generated.GeneratedOperator;
import org.apache.flink.table.runtime.operators.CodeGenOperatorFactory;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.calcite.rel.core.AggregateCall;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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

    private static final org.slf4j.Logger LOG =
            org.slf4j.LoggerFactory.getLogger(BatchExecHashAggregate.class);

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

        // A local aggregate sitting directly on a file source a provider will read is a
        // two-operator device region: the scan and the aggregate run in one execution plan and the
        // columns never come to the host. The provider's source replaces both, so the CPU reader
        // is never built. The global half above the exchange still merges the partials, which is
        // why substituting only the local half is safe.
        final java.util.Optional<Transformation<RowData>> deviceRegion =
                tryDeviceRegion(planner, config, inputEdge);
        if (deviceRegion.isPresent()) {
            return deviceRegion.get();
        }

        final Transformation<RowData> inputTransform =
                (Transformation<RowData>) inputEdge.translateToPlan(planner);

        final GeneratedAggregate generated = generateAggregateOperator(planner, config);
        return ExecNodeUtil.createOneInputTransformation(
                inputTransform,
                createTransformationMeta(HASH_AGGREGATE_TRANSFORMATION, config),
                generated.factory,
                InternalTypeInfo.of((RowType) getOutputType()),
                inputTransform.getParallelism(),
                generated.managedMemory,
                false);
    }

    /** Grouping key field indexes into this node's input row. */
    int[] grouping() {
        return grouping;
    }

    /** The calls this local aggregate computes, in the order they follow the keys. */
    java.util.List<AggregateCall> aggCalls() {
        return java.util.Arrays.asList(aggCalls);
    }

    /** Whether this half merges partial accumulators rather than reading rows. */
    boolean isMerge() {
        return isMerge;
    }

    /** The generated aggregate operator, and the managed memory it needs. */
    static final class GeneratedAggregate {
        final CodeGenOperatorFactory<RowData> factory;
        final long managedMemory;

        GeneratedAggregate(CodeGenOperatorFactory<RowData> factory, long managedMemory) {
            this.factory = factory;
            this.managedMemory = managedMemory;
        }
    }

    /**
     * Generates this node's operator without translating its input.
     *
     * <p>Separated so that {@link BatchExecAcceleratedGroup} can build it after absorbing this
     * node: a fused group owns both members and emits one operator, so it needs the aggregate's
     * code-generated half without the transformation that would otherwise come with it. M4.1.
     */
    GeneratedAggregate generateAggregateOperator(PlannerBase planner, ExecNodeConfig config) {
        final RowType inputRowType = (RowType) getInputEdges().get(0).getOutputType();
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

        return new GeneratedAggregate(
                new CodeGenOperatorFactory<>(generatedOperator), managedMemory);
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

    /**
     * The provider's source for a local aggregate directly over a readable scan, or empty.
     *
     * <p>Narrow, and declining is always free: the path below builds exactly the plan it built
     * before. Anything between the aggregate and the scan — a projection, a filter — declines,
     * because the IR would then describe less than the plan does. Lifting that restriction means
     * generating the projection's kernel inside the source, which is a larger change than this.
     */
    private java.util.Optional<Transformation<RowData>> tryDeviceRegion(
            PlannerBase planner, ExecNodeConfig config, ExecEdge inputEdge) {
        if (config.isCompiled() || isMerge) {
            return java.util.Optional.empty();
        }
        if (!config.get(ExecutionConfigOptions.TABLE_EXEC_ACCELERATOR_ENABLED)) {
            return java.util.Optional.empty();
        }
        final org.apache.flink.table.planner.plan.nodes.exec.ExecNode<?> input =
                inputEdge.getSource();
        final java.util.Optional<org.apache.flink.table.accelerator.AccelScan> scan =
                input.deviceScan();
        if (!scan.isPresent()) {
            return java.util.Optional.empty();
        }
        final RowType inputType = (RowType) inputEdge.getOutputType();
        final java.util.Optional<org.apache.flink.table.accelerator.AccelNode> subtree =
                org.apache.flink.table.planner.plan.accelerator.AccelIrBuilder.buildAggregateOverScan(
                        grouping, java.util.Arrays.asList(aggCalls), scan.get(), inputType,
                        (RowType) getOutputType());
        if (!subtree.isPresent()) {
            return java.util.Optional.empty();
        }
        final java.util.Optional<org.apache.flink.api.connector.source.Source<RowData, ?, ?>>
                source =
                        org.apache.flink.table.runtime.accelerator.AcceleratorProviders
                                .createScanSource(subtree.get(), (RowType) getOutputType());
        if (!source.isPresent()) {
            return java.util.Optional.empty();
        }
        LOG.info(
                "Accelerator reads the source and aggregates it: {} keys from {}",
                grouping.length,
                scan.get().format());
        return java.util.Optional.of(
                planner.getExecEnv()
                        .fromSource(
                                source.get(),
                                org.apache.flink.api.common.eventtime.WatermarkStrategy
                                        .noWatermarks(),
                                "AcceleratedScanAggregate",
                                InternalTypeInfo.of((RowType) getOutputType()))
                        .getTransformation());
    }
}
