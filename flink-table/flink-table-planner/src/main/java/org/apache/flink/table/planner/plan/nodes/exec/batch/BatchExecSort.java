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
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.table.accelerator.AccelNode;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.sort.SortCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.accelerator.AccelIrBuilder;
import org.apache.flink.table.planner.plan.gpu.GpuOffloadAssignment;
import org.apache.flink.table.planner.plan.gpu.GpuSortSupport;
import org.apache.flink.table.planner.plan.gpu.RowCost;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.spec.SortSpec;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.runtime.accelerator.AcceleratedCalcSpec;
import org.apache.flink.table.runtime.gpu.GpuOrCpuCalcOperatorFactory;
import org.apache.flink.table.runtime.operators.sort.SortOperator;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * {@link BatchExecNode} for Sort without limit.
 *
 * <p>This node will output all data rather than `limit` records.
 */
@ExecNodeMetadata(
        name = "batch-exec-sort",
        version = 1,
        producedTransformations = {BatchExecSort.SORT_TRANSFORMATION},
        consumedOptions = {
            "table.exec.sort.max-num-file-handles",
            "table.exec.sort.async-merge-enabled",
            "table.exec.spill-compression.enabled",
            "table.exec.spill-compression.block-size",
            "table.exec.resource.sort.memory"
        },
        minPlanVersion = FlinkVersion.v2_0,
        minStateVersion = FlinkVersion.v2_0)
public class BatchExecSort extends ExecNodeBase<RowData>
        implements BatchExecNode<RowData>, SingleTransformationTranslator<RowData> {
    public static final String SORT_TRANSFORMATION = "sort";

    public static final String FIELD_NAME_SORT_SPEC = "sortSpec";

    @JsonProperty(FIELD_NAME_SORT_SPEC)
    private final SortSpec sortSpec;

    public BatchExecSort(
            ReadableConfig tableConfig,
            SortSpec sortSpec,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        super(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(BatchExecSort.class),
                ExecNodeContext.newPersistedConfig(BatchExecSort.class, tableConfig),
                Collections.singletonList(inputProperty),
                outputType,
                description);
        this.sortSpec = sortSpec;
    }

    @JsonCreator
    public BatchExecSort(
            @JsonProperty(FIELD_NAME_ID) int id,
            @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
            @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig persistedConfig,
            @JsonProperty(FIELD_NAME_SORT_SPEC) SortSpec sortSpec,
            @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
            @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
        super(id, context, persistedConfig, inputProperties, outputType, description);
        this.sortSpec = sortSpec;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        ExecEdge inputEdge = getInputEdges().get(0);
        Transformation<RowData> inputTransform =
                (Transformation<RowData>) inputEdge.translateToPlan(planner);

        RowType inputType = (RowType) inputEdge.getOutputType();
        SortCodeGenerator codeGen =
                new SortCodeGenerator(
                        config, planner.getFlinkContext().getClassLoader(), inputType, sortSpec);

        GeneratedSort generated = generateSortOperator(planner, config);
        return ExecNodeUtil.createOneInputTransformation(
                inputTransform,
                createTransformationMeta(SORT_TRANSFORMATION, config),
                substitute(config, inputType, generated.factory),
                InternalTypeInfo.of((RowType) getOutputType()),
                inputTransform.getParallelism(),
                generated.managedMemory,
                false);
    }

    /**
     * Flink's own sort operator, and the managed memory it needs.
     *
     * <p>Exposed so {@link BatchExecAcceleratedSort} can build the same operator for the CPU half
     * of a fused pair, the way {@code BatchExecHashAggregate.generateAggregateOperator} is exposed
     * for the fused aggregate. Duplicating the construction there instead would mean two places
     * that have to agree about spill handles, compression and merge settings.
     */
    GeneratedSort generateSortOperator(PlannerBase planner, ExecNodeConfig config) {
        RowType inputType = (RowType) getInputEdges().get(0).getOutputType();
        SortCodeGenerator codeGen =
                new SortCodeGenerator(
                        config, planner.getFlinkContext().getClassLoader(), inputType, sortSpec);
        SortOperator operator =
                new SortOperator(
                        codeGen.generateNormalizedKeyComputer("BatchExecSortComputer"),
                        codeGen.generateRecordComparator("BatchExecSortComparator"),
                        config.get(ExecutionConfigOptions.TABLE_EXEC_SORT_MAX_NUM_FILE_HANDLES),
                        config.get(ExecutionConfigOptions.TABLE_EXEC_SPILL_COMPRESSION_ENABLED),
                        (int)
                                config.get(
                                                ExecutionConfigOptions
                                                        .TABLE_EXEC_SPILL_COMPRESSION_BLOCK_SIZE)
                                        .getBytes(),
                        config.get(ExecutionConfigOptions.TABLE_EXEC_SORT_ASYNC_MERGE_ENABLED));
        long sortMemory =
                config.get(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_SORT_MEMORY).getBytes();
        return new GeneratedSort(SimpleOperatorFactory.of(operator), sortMemory);
    }

    /** Flink's sort operator paired with the managed memory it has to be given. */
    static final class GeneratedSort {
        final StreamOperatorFactory<RowData> factory;
        final long managedMemory;

        GeneratedSort(StreamOperatorFactory<RowData> factory, long managedMemory) {
            this.factory = factory;
            this.managedMemory = managedMemory;
        }
    }

    /** The sort key, for the fused node that owns this sort. */
    SortSpec sortSpec() {
        return sortSpec;
    }

    /**
     * Offers the sort to an accelerator, or returns Flink's own sort operator unchanged.
     *
     * <p>The same seam the Calc has, and deliberately so: both implementations travel in the
     * JobGraph and the TaskManager picks. What differs is what a provider is being offered. A Calc
     * is per-row work a device can start on immediately; a sort cannot emit anything until it has
     * seen everything, so a provider that takes this is undertaking to hold the whole input — and
     * is expected to decline on size rather than spill, because {@code SortOperator} below already
     * knows how to spill and this does not.
     */
    private StreamOperatorFactory<RowData> substitute(
            ExecNodeConfig config, RowType inputType, StreamOperatorFactory<RowData> onCpu) {
        GpuOffloadAssignment assignment = getGpuOffloadAssignment();
        if (assignment == null || !assignment.isOffloaded() || config.isCompiled()) {
            // M4.4: a compiled plan does not offload, and a sort is no exception.
            return onCpu;
        }
        Optional<AccelNode> subtree = AccelIrBuilder.buildSort(sortSpec, inputType);
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
     * A sort is offloadable in principle; {@link #estimateRowCost()} decides whether this one is.
     *
     * <p>Unconditional, unlike the aggregate's {@code !isMerge}: there is no half of a sort that is
     * cheap to serve and no flag that distinguishes one. The refusals are all about the key.
     */
    @Override
    public boolean supportGpuOffload() {
        return true;
    }

    @Override
    public RowCost estimateRowCost() {
        RowType inputType = (RowType) getInputEdges().get(0).getOutputType();
        double rows = getEstimatedRowCount();
        long estimated = rows <= 0 ? 0L : (long) rows;
        return GpuSortSupport.estimate(sortSpec, inputType, estimated);
    }
}
