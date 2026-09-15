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
                            "If true, subtrees of the plan whose per-row work can be represented "
                                    + "for an accelerator are offered to a provider on the "
                                    + "TaskManager, which decides against its own hardware "
                                    + "whether to run them. Off by default.");

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
     *
     * <p><b>And {@code jobmanager.scheduler: Default}.</b> The requirement reaches the JobGraph
     * intact under either scheduler -- the offloaded vertex's slot sharing group carries a {@code
     * ResourceProfile} naming the resource -- but only the default scheduler declares it.
     * AdaptiveBatch, which is what a batch job gets unless told otherwise, declares {@code
     * ResourceProfile{UNKNOWN}} instead, and a TaskManager with no device serves the job happily.
     * {@code GpuPlacementSchedulerIT} pins both halves of that, so the day it changes is a test
     * failure rather than a surprise.
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
                                    + "job will wait for a slot rather than fall back to the CPU. "
                                    + "Needs jobmanager.scheduler: Default -- the AdaptiveBatch "
                                    + "scheduler flattens the requirement to UNKNOWN before the "
                                    + "ResourceManager sees it, so the job runs anywhere.");

    /**
     * The slot sharing group the requirement is attached to, which cannot be the default one.
     *
     * <p>It would be better if it could. {@code StreamingJobGraphGenerator#isChainable} refuses to
     * chain two operators in different slot sharing groups, so a group of its own severs the
     * offloaded Calc from its source, and every workload measured here is bound by reading that
     * source. Attaching the requirement to the region everything already shares would cost nothing.
     *
     * <p>Flink does not allow it. In {@code setSlotSharingAndCoLocation}, a vertex whose group is
     * the default name takes the <em>region</em> slot sharing group and the resource registered
     * under that name is never consulted; only the branch for a named group calls {@code
     * streamGraph.getSlotSharingGroupResource(k)}. Registering resources for {@code default} is
     * therefore not a weaker requirement, it is silently no requirement at all -- so this rejects
     * that name rather than accepting a setting that does nothing.
     */
    public static final String DEFAULT_SLOT_SHARING_GROUP_NAME = "default";

    public static final ConfigOption<String> DEVICE_SLOT_SHARING_GROUP =
            ConfigOptions.key("table.exec.gpu-offload.device.slot-sharing-group")
                    .stringType()
                    .defaultValue("gpu-offload")
                    .withDescription(
                            "Slot sharing group carrying the device requirement. Cannot be "
                                    + "\"default\": Flink ignores resources registered for that "
                                    + "name. Because the group must therefore be a distinct one, "
                                    + "the offloaded node is no longer chained to its input.");

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

    /**
     * Whether an ungrouped aggregate is fused into the {@code Calc} beneath it.
     *
     * <p>On by default, because fusing is the point: unfused, the {@code Calc} writes every
     * projected row to the host for a separate aggregate to read back, and the intermediate is the
     * thing a device pass exists to avoid materialising.
     *
     * <p>It is switchable because the two paths are otherwise impossible to compare. Both arms have
     * to run in one session -- this project's own numbers moved 6% between sittings -- and without
     * a switch the unfused path is unreachable once the planner has learned to fuse.
     */
    public static final ConfigOption<Boolean> FUSE_AGGREGATE =
            ConfigOptions.key("table.exec.gpu-offload.fuse-aggregate")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "If true, an ungrouped SUM over an offloaded Calc runs as one device "
                                    + "pass with the projection, so the intermediate never returns "
                                    + "to the host. Turn off to measure what that is worth.");

    /**
     * How much faster than the CPU a provider must claim to be before Flink prefers it.
     *
     * <p>A <em>policy</em> constant, and the distinction matters: it says how much margin Flink
     * wants before taking an unfamiliar execution path, and it means the same thing on a GPU, an
     * FPGA and whatever comes next. The floors it replaced -- 96 weighted operations per row, ten
     * million in total -- were measurements of one RTX 4070 and meant nothing on any other device.
     *
     * <p>1.3 rather than 1.0 because the paths are not otherwise equal: the accelerated one defers
     * output by a batch, allocates staging buffers, and is the less familiar of the two. A claimed
     * 5% win is not worth those.
     */
    public static final ConfigOption<Double> MIN_SPEEDUP =
            ConfigOptions.key("table.exec.gpu-offload.min-speedup")
                    .doubleType()
                    .defaultValue(1.3)
                    .withDescription(
                            "How much faster than the CPU a provider must claim to be before its "
                                    + "plan is preferred. Compared against the provider's own "
                                    + "estimate for its own device, made on the TaskManager. Not "
                                    + "a hardware constant: it means the same on any accelerator.");

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
                                    + "the accelerator runtime dispatch cost.");

    private GpuOffloadOptions() {}
}
