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

package org.apache.flink.table.planner.plan.gpu;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.MemorySize;

/**
 * Configuration for GPU offload.
 *
 * <p>Declared here rather than in {@code ExecutionConfigOptions} so the fork touches a single
 * module. If this were upstreamed the options would move there, where the documentation generator
 * picks them up.
 */
public class GpuOffloadOptions {

    public static final ConfigOption<Boolean> ENABLED =
            ConfigOptions.key(GpuOffloadDecision.ENABLED_KEY)
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "If true, subtrees of the plan whose per-row work is expressible as a "
                                    + "data-parallel kernel and exceeds "
                                    + GpuOffloadDecision.MIN_ROW_COST_KEY
                                    + " are executed on a GPU. Off by default.");

    /**
     * Whether an offloaded task should <em>ask</em> to be placed where a device is.
     *
     * <p>Off by default, and the default is not timidity. Flink has no soft resource constraint: a
     * slot request either carries the requirement or it does not. Turn this on against a cluster
     * where no TaskManager declares {@code external-resource.gpu} and the request can never be
     * satisfied, so the job waits for a slot indefinitely -- strictly worse than today's outcome,
     * which is that it runs on the CPU and says so in the TaskManager log.
     *
     * <p>So this is opt-in for the operator, and the operator has to have configured the other
     * side: {@code external-resource.list: gpu} and {@code external-resource.gpu.amount} on the
     * TaskManagers that have one.
     */
    public static final ConfigOption<Boolean> REQUIRE_DEVICE =
            ConfigOptions.key("table.exec.gpu-offload.require-device")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "If true, a job containing an offloaded node requests a slot that "
                                    + "declares the external resource named by "
                                    + "table.exec.gpu-offload.device.resource-name. Requires "
                                    + "TaskManagers to declare that resource; if none does, the "
                                    + "job will wait for a slot rather than fall back to the CPU.");

    /**
     * The slot sharing group the requirement is attached to.
     *
     * <p>Defaulting to {@code default} is a performance decision rather than a placement one.
     * {@link org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator#isChainable} refuses
     * to chain two operators in different slot sharing groups, so giving the offloaded Calc a group
     * of its own severs it from its source -- and every workload measured here is bound by reading
     * that source, so the shuffle would cost more than the placement wins. Naming a distinct group
     * is the finer-grained option and is available to anyone who has measured that trade for their
     * own query.
     */
    public static final ConfigOption<String> DEVICE_SLOT_SHARING_GROUP =
            ConfigOptions.key("table.exec.gpu-offload.device.slot-sharing-group")
                    .stringType()
                    .defaultValue("default")
                    .withDescription(
                            "Slot sharing group carrying the device requirement. The default "
                                    + "applies it to the whole pipelined region, which keeps the "
                                    + "offloaded node chained to its input; naming another group "
                                    + "places the node alone and breaks that chain.");

    public static final ConfigOption<String> DEVICE_RESOURCE_NAME =
            ConfigOptions.key("table.exec.gpu-offload.device.resource-name")
                    .stringType()
                    .defaultValue("gpu")
                    .withDescription(
                            "External resource name to request, matching the TaskManager's "
                                    + "external-resource.<name>.amount.");

    public static final ConfigOption<Double> DEVICE_RESOURCE_AMOUNT =
            ConfigOptions.key("table.exec.gpu-offload.device.resource-amount")
                    .doubleType()
                    .defaultValue(1.0)
                    .withDescription(
                            "How much of that resource one slot needs. One device per slot bounds "
                                    + "concurrent subtasks per device to the number of devices, "
                                    + "which is worth having on its own: throughput falls past "
                                    + "roughly eight subtasks sharing one card.");

    /**
     * CPU and heap for that slot, which are not optional.
     *
     * <p>{@code SlotSharingGroup.build()} rejects a group that specifies any resource without also
     * specifying cpu cores and task heap memory, so requesting a device means specifying the whole
     * slot. The planner has no basis for inventing those two numbers -- they depend on the cluster,
     * not the query -- so they are configuration, and requesting a device without setting them
     * deliberately fails fast rather than guessing.
     */
    public static final ConfigOption<Double> DEVICE_CPU_CORES =
            ConfigOptions.key("table.exec.gpu-offload.device.cpu-cores")
                    .doubleType()
                    .noDefaultValue()
                    .withDescription(
                            "CPU cores for a slot that carries the device requirement. Required "
                                    + "when require-device is true.");

    public static final ConfigOption<MemorySize> DEVICE_TASK_HEAP =
            ConfigOptions.key("table.exec.gpu-offload.device.task-heap")
                    .memoryType()
                    .noDefaultValue()
                    .withDescription(
                            "Task heap for a slot that carries the device requirement. Required "
                                    + "when require-device is true.");

    public static final ConfigOption<MemorySize> DEVICE_TASK_OFF_HEAP =
            ConfigOptions.key("table.exec.gpu-offload.device.task-off-heap")
                    .memoryType()
                    .defaultValue(MemorySize.parse("4 gb"))
                    .withDescription(
                            "Off-heap for that slot. Staging buffers are off-heap, so this is not "
                                    + "the place to leave the default of zero.");

    public static final ConfigOption<Long> MIN_TOTAL_WORK =
            ConfigOptions.key(GpuOffloadDecision.MIN_TOTAL_WORK_KEY)
                    .longType()
                    .defaultValue(GpuOffloadDecision.DEFAULT_MIN_TOTAL_WORK)
                    .withDescription(
                            "Minimum total work, in weighted operations across all rows, before a "
                                    + "subtree is offloaded. The per-row floor asks whether the "
                                    + "device is faster on each row; this asks whether there are "
                                    + "enough rows to repay what offloading costs once per task -- "
                                    + "compiling the kernel with javac, compiling it again for the "
                                    + "device, and allocating the staging buffers. A heavy "
                                    + "expression over a short input clears the first gate and "
                                    + "still loses.");

    public static final ConfigOption<Integer> MIN_ROW_COST =
            ConfigOptions.key(GpuOffloadDecision.MIN_ROW_COST_KEY)
                    .intType()
                    .defaultValue(GpuOffloadDecision.DEFAULT_MIN_ROW_COST)
                    .withDescription(
                            "Minimum estimated per-row work, in weighted operations, before a "
                                    + "subtree is offloaded. Expressions below this threshold are "
                                    + "faster on the CPU than the staging and device transfer the "
                                    + "offload would add. Device-specific: calibrate with the "
                                    + "intensity sweep in the offload harness rather than assuming "
                                    + "the default transfers.");

    public static final ConfigOption<Integer> BATCH_SIZE =
            ConfigOptions.key("table.exec.gpu-offload.batch-size")
                    .intType()
                    .defaultValue(262_144)
                    .withDescription(
                            "Rows staged before a kernel launch. Batching is not optional -- a "
                                    + "device kernel needs a sized contiguous buffer -- so this "
                                    + "trades memory per subtask against dispatch overhead. "
                                    + "Measured from 64K to 4M, device work stays near a tenth of "
                                    + "the total throughout and larger batches mainly reduce "
                                    + "TornadoVM dispatch cost.");

    private GpuOffloadOptions() {}
}
