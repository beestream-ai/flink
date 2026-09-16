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

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Whether a device requirement in the JobGraph actually stops a TaskManager that has no device.
 *
 * <p>It reaches the JobGraph intact: the vertex carrying the offloaded {@code Calc} has a slot
 * sharing group whose {@code ResourceProfile} names the external resource. Whether that constrains
 * anything then depends on the scheduler, and the two disagree.
 *
 * <p>Under the <b>default</b> scheduler the ResourceManager is told what the job needs and, against
 * a TaskManager declaring nothing, never satisfies it -- the job waits, which is the intended and
 * documented consequence of asking. Under <b>AdaptiveBatch</b>, which is what a batch job gets
 * unless told otherwise, the same JobGraph produces {@code ResourceRequirement{resourceProfile=
 * ResourceProfile{UNKNOWN}}} and the job runs anywhere.
 *
 * <p>Both are asserted, the second as a known gap rather than as intended behaviour. Since offload
 * is batch-only, the scheduler that discards the requirement is the one it will meet by default.
 */
class GpuPlacementSchedulerIT {

    /** Long enough that scheduling would have happened, short enough to keep the suite quick. */
    private static final int WAIT_SECONDS = 20;

    private static JobGraph offloadedJobGraph() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(1);
        StreamTableEnvironment t = StreamTableEnvironment.create(env);
        t.executeSql(
                "CREATE TABLE t (id BIGINT NOT NULL, val DOUBLE NOT NULL) WITH ("
                        + "'connector' = 'datagen', 'number-of-rows' = '1000')");
        t.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ACCELERATOR_ENABLED, true);
        t.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ACCELERATOR_REQUIRE_DEVICE, true);
        t.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ACCELERATOR_DEVICE_CPU_CORES, 0.5);
        t.getConfig()
                .set(
                        ExecutionConfigOptions.TABLE_EXEC_ACCELERATOR_DEVICE_TASK_HEAP,
                        MemorySize.parse("128m"));
        t.getConfig()
                .set(
                        ExecutionConfigOptions.TABLE_EXEC_ACCELERATOR_DEVICE_TASK_OFF_HEAP,
                        MemorySize.parse("64m"));
        t.toDataStream(
                        t.sqlQuery(
                                "SELECT EXP(val) * LN(val) + SIN(val) * COS(val)"
                                        + " + POWER(val, 3.0) AS h FROM t"),
                        Row.class)
                .sinkTo(new DiscardingSink<>());
        return StreamingJobGraphGenerator.createJobGraph(env.getStreamGraph());
    }

    /** Runs against a TaskManager that declares no external resource at all. */
    private static boolean completes(JobManagerOptions.SchedulerType scheduler) throws Exception {
        final JobGraph jobGraph = offloadedJobGraph();
        final Configuration conf = new Configuration();
        conf.set(TaskManagerOptions.NUM_TASK_SLOTS, 4);
        conf.set(JobManagerOptions.SCHEDULER, scheduler);
        try (MiniCluster cluster =
                new MiniCluster(
                        new MiniClusterConfiguration.Builder()
                                .setNumTaskManagers(1)
                                .setNumSlotsPerTaskManager(4)
                                .setConfiguration(conf)
                                .build())) {
            cluster.start();
            CompletableFuture<?> result =
                    cluster.submitJob(jobGraph)
                            .thenCompose(ignored -> cluster.requestJobResult(jobGraph.getJobID()));
            try {
                result.get(WAIT_SECONDS, TimeUnit.SECONDS);
                return true;
            } catch (TimeoutException e) {
                return false;
            }
        }
    }

    @Test
    @DisplayName("the requirement reaches the JobGraph, on the vertex that was offloaded")
    void theJobGraphCarriesIt() {
        boolean found = false;
        for (JobVertex vertex : offloadedJobGraph().getVertices()) {
            ResourceProfile profile = vertex.getSlotSharingGroup().getResourceProfile();
            // UNKNOWN carries no fields and throws from every accessor, which is what the vertices
            // that were not offloaded have.
            if (!profile.equals(ResourceProfile.UNKNOWN)
                    && profile.getExtendedResources().containsKey("gpu")) {
                found = true;
            }
        }
        assertTrue(found, "no vertex asked for a gpu");
    }

    @Test
    @DisplayName("the default scheduler enforces it: no device, no slot, the job waits")
    void defaultSchedulerEnforcesIt() throws Exception {
        assertFalse(
                completes(JobManagerOptions.SchedulerType.Default),
                "a TaskManager declaring no gpu should never have satisfied this job");
    }

    /**
     * The gap, asserted so that it is noticed when it closes.
     *
     * <p>The same JobGraph, the same TaskManager, and the requirement is flattened to {@code
     * UNKNOWN} before the ResourceManager sees it. Flip this assertion when a Flink release fixes
     * it, rather than discovering the change through a job that quietly stops being placed.
     */
    @Test
    @DisplayName("AdaptiveBatch discards it, and batch is where offload lives")
    void adaptiveBatchDiscardsIt() throws Exception {
        assertTrue(
                completes(JobManagerOptions.SchedulerType.AdaptiveBatch),
                "AdaptiveBatch appears to enforce the requirement now; if so this is good news "
                        + "and the option's documentation should stop warning about it");
    }
}
