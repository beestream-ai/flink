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
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.table.accelerator.AccelNode;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
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
import org.apache.flink.table.planner.plan.gpu.NumericalProvenance;
import org.apache.flink.table.planner.plan.gpu.RowCost;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecCalc;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;

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
            declareStagingMemory(config, transform);
            requestDeviceIfConfigured(planner, config, transform);
        }
        return transform;
    }

    /**
     * Declares how much managed memory an accelerated Calc wants for staging.
     *
     * <p>A weight, not an allocation. Flink divides the slot's managed memory between the operators
     * that asked for it in proportion to what they asked, so this decides a share; the runtime
     * turns that share into bytes and the batch size follows from the bytes. What the client can
     * contribute is the shape of the work — how wide a row is, and how many rows the configuration
     * is willing to stage — and that is exactly what it declares here.
     *
     * <p>Before this, the staging was invisible to Flink: the provider allocated off-heap for
     * itself, the slot's budget knew nothing about it, and the only way to make a cluster survive
     * was to hand-configure {@code taskmanager.memory.task.off-heap.size} per deployment. An
     * operator that declares nothing gets nothing, and the runtime falls back to allocating
     * privately, so a plan compiled before this existed still runs.
     */
    private void declareStagingMemory(ExecNodeConfig config, Transformation<RowData> transform) {
        GpuOffloadAssignment assignment = getGpuOffloadAssignment();
        if (assignment == null || !assignment.isOffloaded()) {
            return;
        }
        int bytesPerRow =
                assignment.verdict().work().inputBytesPerRow()
                        + assignment.verdict().work().outputBytesPerRow();
        if (bytesPerRow <= 0) {
            return;
        }
        long wanted =
                (long) bytesPerRow
                        * config.get(ExecutionConfigOptions.TABLE_EXEC_ACCELERATOR_BATCH_SIZE);
        ExecNodeUtil.setManagedMemoryWeight(transform, wanted);
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
        if (!config.get(ExecutionConfigOptions.TABLE_EXEC_ACCELERATOR_REQUIRE_DEVICE)) {
            return;
        }

        final Double cpuCores =
                config.getOptional(ExecutionConfigOptions.TABLE_EXEC_ACCELERATOR_DEVICE_CPU_CORES)
                        .orElse(null);
        final MemorySize taskHeap =
                config.getOptional(ExecutionConfigOptions.TABLE_EXEC_ACCELERATOR_DEVICE_TASK_HEAP)
                        .orElse(null);
        if (cpuCores == null || taskHeap == null) {
            // Fail here rather than let SlotSharingGroup.build() throw: it would report that cpu
            // and heap are required without saying which of this fork's options asked for them.
            throw new IllegalArgumentException(
                    ExecutionConfigOptions.TABLE_EXEC_ACCELERATOR_REQUIRE_DEVICE.key()
                            + " needs a whole slot specification, because Flink rejects a slot "
                            + "sharing group that names an external resource without also naming "
                            + "cpu cores and task heap. Set "
                            + ExecutionConfigOptions.TABLE_EXEC_ACCELERATOR_DEVICE_CPU_CORES.key()
                            + " and "
                            + ExecutionConfigOptions.TABLE_EXEC_ACCELERATOR_DEVICE_TASK_HEAP.key()
                            + " to values that suit the cluster.");
        }

        final String groupName =
                config.get(ExecutionConfigOptions.TABLE_EXEC_ACCELERATOR_DEVICE_SLOT_SHARING_GROUP);
        if (ExecutionConfigOptions.DEFAULT_SLOT_SHARING_GROUP_NAME.equals(groupName)) {
            throw new IllegalArgumentException(
                    ExecutionConfigOptions.TABLE_EXEC_ACCELERATOR_DEVICE_SLOT_SHARING_GROUP.key()
                            + " cannot be \"default\". Resources registered under that name are "
                            + "applied to every pipelined region in the job, not only the one "
                            + "holding the accelerated node, so every region would demand a "
                            + "device. Name a group, and this node's region is put in it.");
        }
        final SlotSharingGroup group =
                SlotSharingGroup.newBuilder(groupName)
                        .setCpuCores(cpuCores)
                        .setTaskHeapMemory(taskHeap)
                        .setTaskOffHeapMemory(
                                config.get(
                                        ExecutionConfigOptions
                                                .TABLE_EXEC_ACCELERATOR_DEVICE_TASK_OFF_HEAP))
                        .setExternalResource(
                                config.get(
                                        ExecutionConfigOptions
                                                .TABLE_EXEC_ACCELERATOR_DEVICE_RESOURCE_NAME),
                                config.get(
                                        ExecutionConfigOptions
                                                .TABLE_EXEC_ACCELERATOR_DEVICE_RESOURCE_AMOUNT))
                        .build();

        // Registering carries the resources; putting the region in the group is what makes them
        // apply to it. M6.1: the region, not the node alone -- see placeRegionIn.
        // Registered on the environment and, separately, carried on every transformation in the
        // region. The second is not redundant. Setting a group by name alone leaves the resources
        // to the environment-level registration, and whether that reaches the StreamGraph the
        // planner's transformations end up in depends on which environment object the job was
        // built from: with a freshly created one the profile arrives UNKNOWN and the requirement
        // is silently dropped, which is a job that looks configured for a device and asks for
        // nothing. Carrying the group object puts the spec on the transformation itself, where
        // StreamGraphGenerator reads it either way. Pinned by GpuPlacementChainingIT.
        planner.getExecEnv().registerSlotSharingGroup(group);
        placeRegionIn(transform, group);
        // At INFO in the client, because a plan saying GPU is an intention and this is the one
        // thing the client can report as fact: that it asked. Whether a device was used is settled
        // on the TaskManager and reported there.
        LOG.info(
                "GPU offload requests a slot declaring {}={} in slot sharing group '{}' "
                        + "(cpu {}, task heap {})",
                config.get(ExecutionConfigOptions.TABLE_EXEC_ACCELERATOR_DEVICE_RESOURCE_NAME),
                config.get(ExecutionConfigOptions.TABLE_EXEC_ACCELERATOR_DEVICE_RESOURCE_AMOUNT),
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
     *
     * <p><b>Compiled plans never offload</b> (M4.4). The decision this method consults is a {@code
     * GpuOffloadAssignment} the offload processor hung on the node, and that field is transient and
     * absent from the node's JSON: a plan written to a file and read back has no assignment and
     * would silently run on the CPU, while the same {@code CompiledPlan} held in memory still
     * carries one and would offload. A compiled plan exists precisely so that the operators do not
     * change out from under a running job, so the two must agree, and the way to make them agree is
     * to refuse both.
     *
     * <p>Serialising the assignment instead was the alternative, and it buys less than it costs. It
     * would pin the accelerator IR, the work profile and the cost model into a format Flink
     * undertakes to keep restorable across releases, and it still would not reproduce the original
     * verdict: whether a subtree actually runs on a device is settled per subtask on the
     * TaskManager, against whatever provider and hardware it finds. The plan can only record that
     * the node was <em>eligible</em>, which is not the thing a restored plan would be reproducing.
     */
    @Override
    public boolean computesApproximateValues() {
        return NumericalProvenance.computesApproximateValues(projection, condition);
    }

    @Override
    public java.util.Set<Integer> approximateColumns() {
        return NumericalProvenance.approximateColumns(projection, condition);
    }

    @Override
    public java.util.Set<Integer> forwardDrift(java.util.Set<Integer> incoming) {
        return NumericalProvenance.forwardDrift(projection, condition, incoming);
    }

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
        if (config.isCompiled()) {
            recordGpuFallback(assignment, COMPILED_PLAN_REASON);
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
                        planner.getTableConfig()
                                .get(ExecutionConfigOptions.TABLE_EXEC_ACCELERATOR_BATCH_SIZE),
                        planner.getTableConfig()
                                .get(ExecutionConfigOptions.TABLE_EXEC_ACCELERATOR_MIN_SPEEDUP));
        return new GpuOrCpuCalcOperatorFactory(
                spec,
                codeGenerated,
                planner.getTableConfig()
                        .get(ExecutionConfigOptions.TABLE_EXEC_ACCELERATOR_MAX_PARALLELISM));
    }

    /**
     * Why a node selected for offload runs on the CPU anyway inside a compiled plan.
     *
     * <p>Shared with {@link BatchExecHashAggregate} so that the two halves of a fused subtree give
     * the same account of themselves.
     */
    static final String COMPILED_PLAN_REASON =
            "compiled plan: the offload decision is not part of the plan format, so a restored "
                    + "plan cannot reproduce it and every compiled plan runs on the CPU";

    /**
     * Puts this node's pipelined region into the device's slot sharing group, not just this node.
     *
     * <p>This is M6.1, and the whole of it. Setting the group on the accelerated operator alone is
     * what the placement did before, and it has a cost that is not obvious from reading it: Flink
     * chains two operators only when their slot sharing groups match, so moving one node into a
     * named group unchains it from its input. M6.0 measured that at roughly a fifth of job wall
     * clock at 4M rows on a machine with no spare cores, growing linearly with the width of the row
     * crossing the boundary -- paid by a feature whose entire purpose is to be faster.
     *
     * <p>Walking up and setting the same group on the operators above puts the boundary back where
     * it already was. The walk stops at a {@link PartitionTransformation}, which is what an
     * exchange translates to and therefore where the pipelined region ends: the chain breaks there
     * whatever we do, and going further would enlarge the requirement to regions that have no
     * accelerated node in them.
     *
     * <p>Three other things stop it, each because continuing would claim something untrue:
     *
     * <ul>
     *   <li>a transformation already in a named group, which belongs to whoever named it;
     *   <li>a transformation with more than one input, since the requirement would spread across a
     *       join into a branch that does not need a device;
     *   <li>a transformation already visited, so a diamond cannot make this loop.
     * </ul>
     *
     * <p>What it deliberately does <em>not</em> do is use the default group name. Resources
     * registered under that name are applied to every region in the job rather than to this one,
     * which is why {@code requestDeviceIfConfigured} refuses it.
     */
    private static void placeRegionIn(Transformation<?> start, SlotSharingGroup group) {
        final Set<Transformation<?>> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        final Deque<Transformation<?>> pending = new ArrayDeque<>();
        pending.push(start);
        while (!pending.isEmpty()) {
            Transformation<?> current = pending.pop();
            if (!seen.add(current)) {
                continue;
            }
            current.setSlotSharingGroup(group);
            for (Transformation<?> input : current.getInputs()) {
                if (input instanceof PartitionTransformation) {
                    // The region ends here: an exchange breaks the chain on its own.
                    continue;
                }
                if (input.getInputs().size() > 1) {
                    continue;
                }
                if (input.getSlotSharingGroup().isPresent()
                        && !ExecutionConfigOptions.DEFAULT_SLOT_SHARING_GROUP_NAME.equals(
                                input.getSlotSharingGroup().get().getName())) {
                    continue;
                }
                pending.push(input);
            }
        }
    }

    /** Rewrites the node's verdict so EXPLAIN reports why a selected node still ran on the CPU. */
    private void recordGpuFallback(GpuOffloadAssignment assignment, String reason) {
        setGpuOffloadAssignment(
                new GpuOffloadAssignment(
                        assignment.groupId(),
                        GpuOffloadDecision.fallback(assignment.verdict(), reason)));
    }
}
