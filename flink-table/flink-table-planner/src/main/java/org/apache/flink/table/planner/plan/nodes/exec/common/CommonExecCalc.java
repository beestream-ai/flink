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

package org.apache.flink.table.planner.plan.nodes.exec.common;

import org.apache.flink.api.common.operators.SlotSharingGroup;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.CalcCodeGenerator;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.gpu.GpuKernelGenerator;
import org.apache.flink.table.planner.plan.gpu.GpuOffloadAssignment;
import org.apache.flink.table.planner.plan.gpu.GpuOffloadDecision;
import org.apache.flink.table.planner.plan.gpu.GpuOffloadOptions;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.runtime.gpu.GpuCalcSpec;
import org.apache.flink.table.runtime.gpu.GpuKernelSource;
import org.apache.flink.table.runtime.gpu.GpuOrCpuCalcOperatorFactory;
import org.apache.flink.table.runtime.operators.CodeGenOperatorFactory;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.calcite.rex.RexNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Base class for exec Calc. */
public abstract class CommonExecCalc extends ExecNodeBase<RowData>
        implements SingleTransformationTranslator<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(CommonExecCalc.class);

    public static final String CALC_TRANSFORMATION = "calc";

    public static final String FIELD_NAME_PROJECTION = "projection";
    public static final String FIELD_NAME_CONDITION = "condition";

    @JsonProperty(FIELD_NAME_PROJECTION)
    protected final List<RexNode> projection;

    @JsonProperty(FIELD_NAME_CONDITION)
    protected final @Nullable RexNode condition;

    private final Class<?> operatorBaseClass;
    private final boolean retainHeader;

    protected CommonExecCalc(
            int id,
            ExecNodeContext context,
            ReadableConfig persistedConfig,
            List<RexNode> projection,
            @Nullable RexNode condition,
            Class<?> operatorBaseClass,
            boolean retainHeader,
            List<InputProperty> inputProperties,
            RowType outputType,
            String description) {
        super(id, context, persistedConfig, inputProperties, outputType, description);
        checkArgument(inputProperties.size() == 1);
        this.projection = checkNotNull(projection);
        this.condition = condition;
        this.operatorBaseClass = checkNotNull(operatorBaseClass);
        this.retainHeader = retainHeader;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        final ExecEdge inputEdge = getInputEdges().get(0);
        final Transformation<RowData> inputTransform =
                (Transformation<RowData>) inputEdge.translateToPlan(planner);
        final CodeGeneratorContext ctx =
                new CodeGeneratorContext(config, planner.getFlinkContext().getClassLoader())
                        .setOperatorBaseClass(operatorBaseClass);

        final CodeGenOperatorFactory<RowData> substituteStreamOperator =
                generateCalcOperator(ctx, inputTransform);
        final StreamOperatorFactory<RowData> factory =
                wrapForGpuOffload(planner, (RowType) getOutputType(), substituteStreamOperator);
        final Transformation<RowData> transform =
                ExecNodeUtil.createOneInputTransformation(
                        inputTransform,
                        createTransformationMeta(CALC_TRANSFORMATION, config),
                        factory,
                        InternalTypeInfo.of(getOutputType()),
                        inputTransform.getParallelism(),
                        false);
        // Keyed off the factory that was actually built, not off the offload assignment: a node
        // can be selected and still fall back, and asking the scheduler for a device to run the
        // CPU operator on would be worse than not asking at all.
        if (factory instanceof GpuOrCpuCalcOperatorFactory) {
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
     * The code-generated operator for this Calc, over a given input.
     *
     * <p>Separated from {@link #translateToPlanInternal} so an aggregate that has fused this node
     * can generate it without also generating this node's transformation. When the pair runs as one
     * operator there is no Calc transformation to build, but there is still a Calc operator: it is
     * what the TaskManager runs, chained ahead of the aggregate, when no device is available.
     */
    public CodeGenOperatorFactory<RowData> generateCalcOperator(
            CodeGeneratorContext ctx, Transformation<RowData> inputTransform) {
        return CalcCodeGenerator.generateCalcOperator(
                ctx,
                inputTransform,
                (RowType) getOutputType(),
                JavaScalaConversionUtil.toScala(projection),
                JavaScalaConversionUtil.toScala(Optional.ofNullable(this.condition)),
                retainHeader,
                getClass().getSimpleName());
    }

    /** This Calc's expressions as a device kernel, or empty if they cannot be generated. */
    public Optional<GpuKernelSource> gpuKernel() {
        return GpuKernelGenerator.generate(projection, condition, Integer.toString(getId()));
    }

    /**
     * Wraps the code-generated operator so the TaskManager can run this Calc on a device, or
     * returns it unchanged when this node will never offload.
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
    private StreamOperatorFactory<RowData> wrapForGpuOffload(
            PlannerBase planner,
            RowType outputType,
            CodeGenOperatorFactory<RowData> codeGenerated) {
        GpuOffloadAssignment assignment = getGpuOffloadAssignment();
        if (assignment == null || !assignment.isOffloaded()) {
            return codeGenerated;
        }
        Optional<GpuKernelSource> kernel =
                GpuKernelGenerator.generate(projection, condition, Integer.toString(getId()));
        if (!kernel.isPresent()) {
            recordGpuFallback(assignment, "expressions cannot be generated as a device kernel");
            return codeGenerated;
        }
        GpuCalcSpec spec =
                new GpuCalcSpec(
                        kernel.get(),
                        kernel.get().outputLayout(),
                        outputType,
                        planner.getTableConfig().get(GpuOffloadOptions.BATCH_SIZE),
                        assignment.verdict().rowCost());
        if (!spec.canStage()) {
            recordGpuFallback(assignment, "output row cannot be staged for a device");
            return codeGenerated;
        }
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
