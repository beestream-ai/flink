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
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.legacy.InputFormatSourceFunction;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.exec.spec.DynamicTableSourceSpec;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.UUID;

/**
 * Batch {@link ExecNode} to read data from an external source defined by a bounded {@link
 * ScanTableSource}.
 */
@ExecNodeMetadata(
        name = "batch-exec-table-source-scan",
        version = 1,
        producedTransformations = CommonExecTableSourceScan.SOURCE_TRANSFORMATION,
        minPlanVersion = FlinkVersion.v2_0,
        minStateVersion = FlinkVersion.v2_0)
public class BatchExecTableSourceScan extends CommonExecTableSourceScan
        implements BatchExecNode<RowData> {

    // Avoids creating different ids if translated multiple times
    private final String dynamicFilteringDataListenerID = UUID.randomUUID().toString();
    private final ReadableConfig tableConfig;

    // This constructor can be used only when table source scan has
    // BatchExecDynamicFilteringDataCollector input
    public BatchExecTableSourceScan(
            ReadableConfig tableConfig,
            DynamicTableSourceSpec tableSourceSpec,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        super(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(BatchExecTableSourceScan.class),
                ExecNodeContext.newPersistedConfig(BatchExecTableSourceScan.class, tableConfig),
                tableSourceSpec,
                Collections.singletonList(inputProperty),
                outputType,
                description);
        this.tableConfig = tableConfig;
    }

    public BatchExecTableSourceScan(
            ReadableConfig tableConfig,
            DynamicTableSourceSpec tableSourceSpec,
            RowType outputType,
            String description) {
        super(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(BatchExecTableSourceScan.class),
                ExecNodeContext.newPersistedConfig(BatchExecTableSourceScan.class, tableConfig),
                tableSourceSpec,
                Collections.emptyList(),
                outputType,
                description);
        this.tableConfig = tableConfig;
    }

    @JsonCreator
    public BatchExecTableSourceScan(
            @JsonProperty(FIELD_NAME_ID) int id,
            @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
            @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig tableConfig,
            @JsonProperty(FIELD_NAME_SCAN_TABLE_SOURCE) DynamicTableSourceSpec tableSourceSpec,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
            @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
        super(
                id,
                context,
                tableConfig,
                tableSourceSpec,
                Collections.emptyList(),
                outputType,
                description);
        this.tableConfig = tableConfig;
    }

    public String getDynamicFilteringDataListenerID() {
        return dynamicFilteringDataListenerID;
    }

    @Override
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        final Transformation<RowData> transformation =
                super.translateToPlanInternal(planner, config);
        // the boundedness has been checked via the runtime provider already, so we can safely
        // declare all legacy transformations as bounded to make the stream graph generator happy
        ExecNodeUtil.makeLegacySourceTransformationsBounded(transformation);
        return transformation;
    }

    public static BatchExecDynamicFilteringDataCollector getDynamicFilteringDataCollector(
            BatchExecNode<?> node) {
        Preconditions.checkState(
                node.getInputEdges().size() == 1,
                "The fact source must have one "
                        + "input representing dynamic filtering data collector");
        BatchExecNode<?> input = (BatchExecNode<?>) node.getInputEdges().get(0).getSource();
        if (input instanceof BatchExecDynamicFilteringDataCollector) {
            return (BatchExecDynamicFilteringDataCollector) input;
        }

        Preconditions.checkState(
                input instanceof BatchExecExchange,
                "There could only be BatchExecExchange "
                        + "between fact source and dynamic filtering data collector");
        return getDynamicFilteringDataCollector(input);
    }

    @Override
    public Transformation<RowData> createInputFormatTransformation(
            StreamExecutionEnvironment env,
            InputFormat<RowData, ?> inputFormat,
            InternalTypeInfo<RowData> outputTypeInfo,
            String operatorName) {
        // env.createInput will use ContinuousFileReaderOperator, but it do not support multiple
        // paths. If read partitioned source, after partition pruning, we need let InputFormat
        // to read multiple partitions which are multiple paths.
        // We can use InputFormatSourceFunction directly to support InputFormat.
        final InputFormatSourceFunction<RowData> function =
                new InputFormatSourceFunction<>(inputFormat, outputTypeInfo);
        return env.addSource(function, operatorName, outputTypeInfo).getTransformation();
    }

    @Override
    protected final boolean legacyUidsEnabled() {
        return false;
    }

    public BatchExecTableSourceScan copyAndRemoveInputs() {
        BatchExecTableSourceScan tableSourceScan =
                new BatchExecTableSourceScan(
                        tableConfig,
                        getTableSourceSpec(),
                        (RowType) getOutputType(),
                        getDescription());
        tableSourceScan.setInputEdges(Collections.emptyList());
        return tableSourceScan;
    }

    /**
     * The scan this node is, when a device reader could plausibly open it.
     *
     * <p>Answered from the table's own options rather than from the source object, because the
     * question is about the bytes on disk and not about the reader Flink happens to have built for
     * them. A connector that is not {@code filesystem} has no path a device reader could take and
     * is not offered.
     *
     * <p>The format is passed through rather than checked against a list. Which formats a device
     * can decode is the provider's knowledge, not Flink's, and an allowlist here would be a second
     * place to keep it in step — {@link org.apache.flink.table.accelerator.AccelScan} exists so
     * that a provider declines on the string.
     *
     * <p>Note what this does <em>not</em> do: it does not make the scan offloadable. {@link
     * #supportGpuOffload()} stays false, so the scan does not join the grouping that decides which
     * operators share a device, and every plan is the plan it was before. This answers "could a
     * provider read this", so the answer can be recorded and reported; acting on it needs an
     * operator that can consume a device-resident batch.
     */
    @Override
    public java.util.Optional<org.apache.flink.table.accelerator.AccelScan> deviceScan() {
        final java.util.Map<String, String> options;
        try {
            options =
                    getTableSourceSpec().getContextResolvedTable().getResolvedTable().getOptions();
        } catch (RuntimeException notResolvable) {
            // An inline or otherwise unresolved table. Unknown means "no scan to offer", which is
            // the direction this has to fail in.
            return java.util.Optional.empty();
        }
        if (options == null) {
            return java.util.Optional.empty();
        }
        if (!"filesystem".equals(options.get("connector"))) {
            return java.util.Optional.empty();
        }
        final String path = options.get("path");
        final String rawFormat = options.get("format");
        if (rawFormat == null || path == null) {
            return java.util.Optional.empty();
        }
        final String format = rawFormat.toLowerCase(java.util.Locale.ROOT);
        final RowType produced = (RowType) getOutputType();
        // The file's column indexes, not the produced row's. Projection pushdown has usually
        // already run by now, so the produced row is a subset -- and reading it as though field i
        // of the row were column i of the file is a wrong answer of the worst kind: the columns
        // exist, the types line up, and the number that comes back is a real sum of the wrong
        // data. SELECT lat, lon over (id, lat, lon) would read id and lat.
        final int[] projected = new int[produced.getFieldCount()];
        org.apache.flink.table.planner.plan.abilities.source.ProjectPushDownSpec pushDown = null;
        final java.util.List<org.apache.flink.table.planner.plan.abilities.source.SourceAbilitySpec>
                abilities = getTableSourceSpec().getSourceAbilities();
        if (abilities != null) {
            for (org.apache.flink.table.planner.plan.abilities.source.SourceAbilitySpec ability :
                    abilities) {
                if (ability
                        instanceof
                        org.apache.flink.table.planner.plan.abilities.source.ProjectPushDownSpec) {
                    pushDown =
                            (org.apache.flink.table.planner.plan.abilities.source
                                            .ProjectPushDownSpec)
                                    ability;
                } else {
                    // Any other ability has already changed what the CPU source would read,
                    // and the device source built from this replaces that source outright.
                    // Partition pushdown is the one that bites: the rule removes the partition
                    // predicate from the plan once it is pushed, so a scan described by path
                    // and column indexes alone reads every partition and the query returns a
                    // sum over the whole table -- the columns exist, the types line up, and
                    // the number is a real sum of the wrong rows. A pushed LIMIT and a
                    // metadata column are the same shape. Declining is the only safe answer
                    // until the IR can carry them.
                    return java.util.Optional.empty();
                }
            }
        }
        if (pushDown == null) {
            for (int i = 0; i < projected.length; i++) {
                projected[i] = i;
            }
        } else {
            final int[][] fields = pushDown.getProjectedFields();
            if (fields.length != projected.length) {
                return java.util.Optional.empty();
            }
            for (int i = 0; i < fields.length; i++) {
                // A nested reference -- a field inside a ROW -- has no column index a columnar
                // reader can be handed. Declining is correct; there is no index to pass.
                if (fields[i].length != 1) {
                    return java.util.Optional.empty();
                }
                projected[i] = fields[i][0];
            }
        }
        return java.util.Optional.of(
                new org.apache.flink.table.accelerator.AccelScan(
                        format, java.util.Collections.singletonList(path), projected, produced));
    }
}
