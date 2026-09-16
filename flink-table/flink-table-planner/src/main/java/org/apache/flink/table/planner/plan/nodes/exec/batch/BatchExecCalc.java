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
import org.apache.flink.api.common.operators.SlotSharingGroup;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.accelerator.AccelNode;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.accelerator.AccelIrBuilder;
import org.apache.flink.table.planner.plan.fusion.OpFusionCodegenSpecGenerator;
import org.apache.flink.table.planner.plan.fusion.generator.OneInputOpFusionCodegenSpecGenerator;
import org.apache.flink.table.planner.plan.fusion.spec.CalcFusionCodegenSpec;
import org.apache.flink.table.planner.plan.gpu.GpuCostEstimator;
import org.apache.flink.table.planner.plan.gpu.GpuOffloadAssignment;
import org.apache.flink.table.planner.plan.gpu.GpuOffloadDecision;
import org.apache.flink.table.planner.plan.gpu.GpuOffloadOptions;
import org.apache.flink.table.planner.plan.gpu.RowCost;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecCalc;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.runtime.accelerator.AcceleratedCalcSpec;
import org.apache.flink.table.runtime.gpu.GpuOrCpuCalcOperatorFactory;
import org.apache.flink.table.runtime.operators.CodeGenOperatorFactory;
import org.apache.flink.table.runtime.operators.TableStreamOperator;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.calcite.rex.RexNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/** Batch {@link ExecNode} for Calc. */
@ExecNodeMetadata(
        name = "batch-exec-calc",
        version = 1,
        producedTransformations = CommonExecCalc.CALC_TRANSFORMATION,
        minPlanVersion = FlinkVersion.v2_0,
        minStateVersion = FlinkVersion.v2_0)
public class BatchExecCalc extends CommonExecCalc implements BatchExecNode<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(BatchExecCalc.class);

    public BatchExecCalc(
            ReadableConfig tableConfig,
            List<RexNode> projection,
            @Nullable RexNode condition,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        super(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(BatchExecCalc.class),
                ExecNodeContext.newPersistedConfig(BatchExecCalc.class, tableConfig),
                projection,
                condition,
                TableStreamOperator.class,
                false, // retainHeader
                Collections.singletonList(inputProperty),
                outputType,
                description);
    }

    @JsonCreator
    public BatchExecCalc(
            @JsonProperty(FIELD_NAME_ID) int id,
            @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
            @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig persistedConfig,
            @JsonProperty(FIELD_NAME_PROJECTION) List<RexNode> projection,
            @JsonProperty(FIELD_NAME_CONDITION) @Nullable RexNode condition,
            @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
            @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
        super(
                id,
                context,
                persistedConfig,
                projection,
                condition,
                TableStreamOperator.class,
                false, // retainHeader
                inputProperties,
                outputType,
                description);
    }

    /**
     * A Calc is the natural first offload target: stateless, 1:1, no shuffle, no ordering
     * dependence. Its per-record work is a pure function of one row, which is exactly a
     * {@code @Parallel} loop body.
     *
     * <p>This says only that the shape is right. Whether these particular expressions are
     * expressible on a device, and whether they do enough work to be worth moving, are answered by
     * {@link #estimateRowCost()}.
     */
    @Override
    public boolean supportGpuOffload() {
        return true;
    }

    /**
     * Costs the projection and the condition as one unit, because they execute as one kernel over
     * the same staged buffer.
     */
    @Override
    public RowCost estimateRowCost() {
        List<RexNode> expressions = new ArrayList<>(projection);
        if (condition != null) {
            expressions.add(condition);
        }
        return GpuCostEstimator.estimateAll(expressions);
    }

    public boolean supportFusionCodegen() {
        return true;
    }

    @Override
    protected OpFusionCodegenSpecGenerator translateToFusionCodegenSpecInternal(
            PlannerBase planner, ExecNodeConfig config, CodeGeneratorContext parentCtx) {
        OpFusionCodegenSpecGenerator input =
                getInputEdges().get(0).translateToFusionCodegenSpec(planner, parentCtx);
        OpFusionCodegenSpecGenerator calcGenerator =
                new OneInputOpFusionCodegenSpecGenerator(
                        input,
                        0L,
                        (RowType) getOutputType(),
                        new CalcFusionCodegenSpec(
                                new CodeGeneratorContext(
                                        config,
                                        planner.getFlinkContext().getClassLoader(),
                                        parentCtx),
                                JavaScalaConversionUtil.toScala(projection),
                                JavaScalaConversionUtil.toScala(Optional.ofNullable(condition))));
        input.addOutput(1, calcGenerator);
        return calcGenerator;
    }

    @Override
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        Transformation<RowData> transform = super.translateToPlanInternal(planner, config);
        // Keyed off the factory that was actually built, not off the offload assignment: a node
        // can be selected and still fall back, and asking the scheduler for a device to run the
        // CPU operator on would be worse than not asking at all.
        if (transform instanceof OneInputTransformation
                && ((OneInputTransformation<?, ?>) transform).getOperatorFactory()
                        instanceof GpuOrCpuCalcOperatorFactory) {
            requestDeviceIfConfigured(planner, config, transform);
        }
        return transform;
    }

    /**
     * Asks the scheduler to place this task where a device is, when the operator has asked for
     * that.
     *
     * <p>This is the half of placement the client legitimately owns. Whether a device is
     * <em>used</em> is settled on the TaskManager and cannot be known here; whether one is
     * <em>required</em> is a property of the job the client is submitting, and is exactly what a
     * slot request expresses.
     *
     * <p>Off unless configured, because the failure modes are not symmetric. Without a requirement
     * a misplaced task runs on the CPU and logs that it did. With one that nothing can satisfy, the
     * job waits for a slot that will never come -- and Flink has no third option, since a resource
     * requirement is not a preference.
     */
    private void requestDeviceIfConfigured(
            PlannerBase planner, ExecNodeConfig config, Transformation<RowData> transform) {
        if (!config.get(GpuOffloadOptions.REQUIRE_DEVICE)) {
            return;
        }

        final Double cpuCores = config.getOptional(GpuOffloadOptions.DEVICE_CPU_CORES).orElse(null);
        final MemorySize taskHeap =
                config.getOptional(GpuOffloadOptions.DEVICE_TASK_HEAP).orElse(null);
        if (cpuCores == null || taskHeap == null) {
            // Fail here rather than let SlotSharingGroup.build() throw: it would report that cpu
            // and heap are required without saying which of this fork's options asked for them.
            throw new IllegalArgumentException(
                    GpuOffloadOptions.REQUIRE_DEVICE.key()
                            + " needs a whole slot specification, because Flink rejects a slot "
                            + "sharing group that names an external resource without also naming "
                            + "cpu cores and task heap. Set "
                            + GpuOffloadOptions.DEVICE_CPU_CORES.key()
                            + " and "
                            + GpuOffloadOptions.DEVICE_TASK_HEAP.key()
                            + " to values that suit the cluster.");
        }

        final String groupName = config.get(GpuOffloadOptions.DEVICE_SLOT_SHARING_GROUP);
        if (GpuOffloadOptions.DEFAULT_SLOT_SHARING_GROUP_NAME.equals(groupName)) {
            throw new IllegalArgumentException(
                    GpuOffloadOptions.DEVICE_SLOT_SHARING_GROUP.key()
                            + " cannot be \"default\". Flink resolves a vertex in the default "
                            + "group to its region's slot sharing group and never reads the "
                            + "resources registered under that name, so the requirement would be "
                            + "silently dropped rather than merely weakened. Name a group.");
        }
        final SlotSharingGroup group =
                SlotSharingGroup.newBuilder(groupName)
                        .setCpuCores(cpuCores)
                        .setTaskHeapMemory(taskHeap)
                        .setTaskOffHeapMemory(config.get(GpuOffloadOptions.DEVICE_TASK_OFF_HEAP))
                        .setExternalResource(
                                config.get(GpuOffloadOptions.DEVICE_RESOURCE_NAME),
                                config.get(GpuOffloadOptions.DEVICE_RESOURCE_AMOUNT))
                        .build();

        // Registering carries the resources; setting the group on the transformation is what puts
        // this operator in it. The default group name is the one every other operator already
        // uses, so by default this requirement covers the pipelined region and no chain is broken.
        planner.getExecEnv().registerSlotSharingGroup(group);
        transform.setSlotSharingGroup(group);
        // At INFO in the client, because a plan saying GPU is an intention and this is the one
        // thing the client can report as fact: that it asked. Whether a device was used is settled
        // on the TaskManager and reported there.
        LOG.info(
                "GPU offload requests a slot declaring {}={} in slot sharing group '{}' "
                        + "(cpu {}, task heap {})",
                config.get(GpuOffloadOptions.DEVICE_RESOURCE_NAME),
                config.get(GpuOffloadOptions.DEVICE_RESOURCE_AMOUNT),
                groupName,
                cpuCores,
                taskHeap);
    }

    /**
     * Wraps the code-generated operator so the TaskManager can run this Calc on a device, or
     * returns it unchanged when this node will never offload.
     *
     * <p>Overriding {@link CommonExecCalc#substituteOperator} rather than living there is the whole
     * of M2.6. Offload is batch-only — the processor that selects nodes for it runs in the batch
     * planner and nowhere else — but while this method sat in the shared base class, that was true
     * only because nobody had registered the processor on the streaming side. {@code
     * StreamExecCalc} inherited the machinery and was kept away from it by a registration, which is
     * a thing somebody changes by accident. Here it is a fact about the type.
     *
     * <p>It matters beyond tidiness because the streaming operator is the one with a {@code
     * RowKind} to preserve, and this path does not preserve it (M2.7).
     *
     * <p>Two things are decided here, and both are properties of the query rather than of any
     * machine: whether the offload processor selected this node, and whether its expressions can be
     * generated as a kernel at all. Either failing is a normal outcome, recorded so that EXPLAIN
     * reports it.
     *
     * <p>What is <em>not</em> decided here is whether a device exists. The planner runs in the
     * client, and the task will run wherever the scheduler puts it; asking the client's classpath
     * about the cluster's hardware was the bug this shape removes. Both implementations therefore
     * travel in the JobGraph and {@link GpuOrCpuCalcOperatorFactory} chooses on arrival.
     */
    @Override
    protected StreamOperatorFactory<RowData> substituteOperator(
            PlannerBase planner,
            ExecNodeConfig config,
            CodeGenOperatorFactory<RowData> codeGenerated) {
        RowType outputType = (RowType) getOutputType();
        GpuOffloadAssignment assignment = getGpuOffloadAssignment();
        if (assignment == null || !assignment.isOffloaded()) {
            return codeGenerated;
        }
        RowType inputType = (RowType) getInputEdges().get(0).getOutputType();
        Optional<AccelNode> subtree =
                AccelIrBuilder.buildCalc(projection, condition, inputType, outputType);
        if (!subtree.isPresent()) {
            recordGpuFallback(assignment, "expressions have no accelerator IR representation");
            return codeGenerated;
        }
        AcceleratedCalcSpec spec =
                new AcceleratedCalcSpec(
                        subtree.get(),
                        assignment.verdict().work(),
                        outputType,
                        planner.getTableConfig().get(GpuOffloadOptions.BATCH_SIZE),
                        planner.getTableConfig().get(GpuOffloadOptions.MIN_SPEEDUP));
        return new GpuOrCpuCalcOperatorFactory(spec, codeGenerated);
    }

    /** Rewrites the node's verdict so EXPLAIN reports why a selected node still ran on the CPU. */
    private void recordGpuFallback(GpuOffloadAssignment assignment, String reason) {
        setGpuOffloadAssignment(
                new GpuOffloadAssignment(
                        assignment.groupId(),
                        GpuOffloadDecision.fallback(assignment.verdict(), reason)));
    }
}
