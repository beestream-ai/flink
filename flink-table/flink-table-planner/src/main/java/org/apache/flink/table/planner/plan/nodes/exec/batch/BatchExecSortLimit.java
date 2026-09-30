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
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.sort.ComparatorCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.spec.SortSpec;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.operators.sort.SortLimitOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.table.accelerator.AccelNode;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.planner.plan.accelerator.AccelIrBuilder;
import org.apache.flink.table.planner.plan.gpu.GpuOffloadAssignment;
import org.apache.flink.table.planner.plan.gpu.GpuSortSupport;
import org.apache.flink.table.planner.plan.gpu.RowCost;
import org.apache.flink.table.runtime.accelerator.AcceleratedCalcSpec;
import org.apache.flink.table.runtime.gpu.GpuOrCpuCalcOperatorFactory;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.Optional;
import java.util.List;

import static org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecLimit.FIELD_NAME_IS_GLOBAL;
import static org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecLimit.FIELD_NAME_LIMIT_END;
import static org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecLimit.FIELD_NAME_LIMIT_START;
import static org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecSort.FIELD_NAME_SORT_SPEC;

/**
 * {@link BatchExecNode} for Sort with limit.
 *
 * <p>This node will output data rank from `limitStart` to `limitEnd`.
 */
@ExecNodeMetadata(
        name = "batch-exec-sort-limit",
        version = 1,
        producedTransformations = BatchExecSortLimit.SORT_LIMIT_TRANSFORMATION,
        minPlanVersion = FlinkVersion.v2_0,
        minStateVersion = FlinkVersion.v2_0)
public class BatchExecSortLimit extends ExecNodeBase<RowData>
        implements BatchExecNode<RowData>, SingleTransformationTranslator<RowData> {

    private static final org.slf4j.Logger LOG =
            org.slf4j.LoggerFactory.getLogger(BatchExecSortLimit.class);

    public static final String SORT_LIMIT_TRANSFORMATION = "sort-limit";

    @JsonProperty(FIELD_NAME_SORT_SPEC)
    private final SortSpec sortSpec;

    @JsonProperty(FIELD_NAME_LIMIT_START)
    private final long limitStart;

    @JsonProperty(FIELD_NAME_LIMIT_END)
    private final long limitEnd;

    @JsonProperty(FIELD_NAME_IS_GLOBAL)
    private final boolean isGlobal;

    public BatchExecSortLimit(
            ReadableConfig tableConfig,
            SortSpec sortSpec,
            long limitStart,
            long limitEnd,
            boolean isGlobal,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        super(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(BatchExecSortLimit.class),
                ExecNodeContext.newPersistedConfig(BatchExecSortLimit.class, tableConfig),
                Collections.singletonList(inputProperty),
                outputType,
                description);
        this.sortSpec = sortSpec;
        this.limitStart = limitStart;
        this.limitEnd = limitEnd;
        this.isGlobal = isGlobal;
    }

    @JsonCreator
    public BatchExecSortLimit(
            @JsonProperty(FIELD_NAME_ID) int id,
            @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
            @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig persistedConfig,
            @JsonProperty(FIELD_NAME_SORT_SPEC) SortSpec sortSpec,
            @JsonProperty(FIELD_NAME_LIMIT_START) long limitStart,
            @JsonProperty(FIELD_NAME_LIMIT_END) long limitEnd,
            @JsonProperty(FIELD_NAME_IS_GLOBAL) boolean isGlobal,
            @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
            @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
        super(id, context, persistedConfig, inputProperties, outputType, description);
        this.sortSpec = sortSpec;
        this.limitStart = limitStart;
        this.limitEnd = limitEnd;
        this.isGlobal = isGlobal;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        if (limitEnd == Long.MAX_VALUE) {
            throw new TableException("Not support limitEnd is max value now!");
        }

        ExecEdge inputEdge = getInputEdges().get(0);

        // Before translating the input: if the whole subtree is a top-N over a file source a
        // provider can read itself, the provider's source replaces both, and the CPU reader is
        // never built. Translating the input first would be harmless but pointless -- the
        // transformation would be unreachable from any sink and dropped -- except that building it
        // can register state the planner then carries around, so it is skipped entirely.
        final java.util.Optional<Transformation<RowData>> deviceSource =
                tryDeviceScanSource(planner, config, inputEdge);
        if (deviceSource.isPresent()) {
            return deviceSource.get();
        }

        Transformation<RowData> inputTransform =
                (Transformation<RowData>) inputEdge.translateToPlan(planner);

        RowType inputType = (RowType) inputEdge.getOutputType();
        // generate comparator
        GeneratedRecordComparator genComparator =
                ComparatorCodeGenerator.gen(
                        config,
                        planner.getFlinkContext().getClassLoader(),
                        "SortLimitComparator",
                        inputType,
                        sortSpec);

        // TODO If input is ordered, there is no need to use the heap.
        SortLimitOperator operator =
                new SortLimitOperator(isGlobal, limitStart, limitEnd, genComparator);

        return ExecNodeUtil.createOneInputTransformation(
                inputTransform,
                createTransformationMeta(SORT_LIMIT_TRANSFORMATION, config),
                substitute(config, inputType, SimpleOperatorFactory.of(operator)),
                InternalTypeInfo.of(inputType),
                inputTransform.getParallelism(),
                false);
    }

    /**
     * Offers the top-N to an accelerator, or returns Flink's own heap operator unchanged.
     *
     * <p>The same seam {@link BatchExecSort} has, and it matters more here. An unbounded sort hands
     * back as many rows as it read, so the copy off the device is the size of the input and usually
     * eats the ordering it just paid for. A top-N hands back {@code limitEnd} rows however many it
     * read — the one ordering shape whose answer is small — which is why this node is worth
     * offering at all and the plain sort so often is not.
     *
     * <p>Offered for both the local and the global instance. They differ in what they are ordering,
     * not in what is asked of a provider: order one key, keep the first {@code N}. The local one is
     * where the rows are, so it is the one a device can save time on; the global one sees at most
     * {@code N} per subtask and will usually be refused on size, by the provider, for its own
     * reasons.
     */
    private StreamOperatorFactory<RowData> substitute(
            ExecNodeConfig config, RowType inputType, StreamOperatorFactory<RowData> onCpu) {
        GpuOffloadAssignment assignment = getGpuOffloadAssignment();
        if (assignment == null || !assignment.isOffloaded() || config.isCompiled()) {
            // M4.4: a compiled plan does not offload, and a top-N is no exception.
            return onCpu;
        }
        Optional<AccelNode> subtree =
                AccelIrBuilder.buildTopN(sortSpec, limitStart, limitEnd, inputType);
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
     * Offloadable in principle when the range starts at the first row.
     *
     * <p>A non-zero {@code limitStart} is an offset into the ordering, which {@link AccelSort}
     * cannot carry: a provider is asked for the first {@code N} of a permutation, not a window of
     * it. Refusing here rather than in the IR builder keeps the reason in the plan.
     */
    @Override
    public boolean supportGpuOffload() {
        return limitStart == 0L;
    }

    @Override
    public RowCost estimateRowCost() {
        RowType inputType = (RowType) getInputEdges().get(0).getOutputType();
        double rows = getEstimatedRowCount();
        long estimated = rows <= 0 ? 0L : (long) rows;
        return GpuSortSupport.estimate(sortSpec, inputType, estimated);
    }

    /**
     * The provider's own source for a top-N directly over a readable file source, or empty.
     *
     * <p>Kept narrow on purpose. The pattern is a bounded sort whose only input is a
     * {@code TableSourceScan} offering an {@link org.apache.flink.table.accelerator.AccelScan};
     * anything between them -- a projection, a filter, an exchange -- and this declines, because
     * the IR handed to the provider would then describe less than the plan does and the source
     * would return rows the query did not ask for.
     *
     * <p>Declining is always safe: the normal path below builds exactly the plan it built before.
     */
    private java.util.Optional<Transformation<RowData>> tryDeviceScanSource(
            PlannerBase planner, ExecNodeConfig config, ExecEdge inputEdge) {
        if (config.isCompiled() || limitStart != 0L) {
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
                AccelIrBuilder.buildTopNOverScan(sortSpec, limitStart, limitEnd, scan.get());
        if (!subtree.isPresent()) {
            return java.util.Optional.empty();
        }
        final java.util.Optional<org.apache.flink.api.connector.source.Source<RowData, ?, ?>>
                source =
                        org.apache.flink.table.runtime.accelerator.AcceleratorProviders
                                .createScanSource(subtree.get(), inputType);
        if (!source.isPresent()) {
            return java.util.Optional.empty();
        }
        LOG.info(
                "Accelerator reads the source: {} rows of {} from {}",
                limitEnd,
                inputType.getFieldCount(),
                scan.get().format());
        return java.util.Optional.of(
                planner.getExecEnv()
                        .fromSource(
                                source.get(),
                                org.apache.flink.api.common.eventtime.WatermarkStrategy
                                        .noWatermarks(),
                                "AcceleratedScanSort",
                                InternalTypeInfo.of(inputType))
                        .getTransformation());
    }
}
