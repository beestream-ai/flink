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
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * M6.1: asking for a device no longer unchains the node that asked.
 *
 * <p>Flink chains two operators only when their slot sharing groups match, so putting the
 * accelerated {@code Calc} alone into a named group took it out of its source's vertex. M6.0
 * measured what that costs — roughly a fifth of job wall clock at 4M rows on a machine with no
 * spare cores, growing linearly with the width of the row crossing the boundary. The fix is to put
 * the node's whole pipelined region into the group instead of the node alone.
 *
 * <p>Each case builds its own {@link StreamExecutionEnvironment}. {@code getExecutionEnvironment()}
 * hands back a shared instance in a plain JVM, and transformations registered by one case then leak
 * into the next one's graph.
 */
class GpuPlacementChainingIT {

    private static final String HEAVY =
            "EXP(val) * LN(val) + SIN(val) * COS(val) + POWER(val, 3.0)";

    private static JobGraph jobGraph(boolean requireDevice) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.executeSql(
                "CREATE TABLE t (id BIGINT NOT NULL, val DOUBLE NOT NULL) WITH ("
                        + "'connector' = 'datagen', 'number-of-rows' = '1000')");
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ACCELERATOR_ENABLED, true);
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ACCELERATOR_APPROXIMATE_PROJECTIONS, true);
        tEnv.getConfig()
                .set(ExecutionConfigOptions.TABLE_EXEC_ACCELERATOR_REQUIRE_DEVICE, requireDevice);
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ACCELERATOR_DEVICE_CPU_CORES, 0.5);
        tEnv.getConfig()
                .set(
                        ExecutionConfigOptions.TABLE_EXEC_ACCELERATOR_DEVICE_TASK_HEAP,
                        MemorySize.parse("128m"));
        tEnv.getConfig()
                .set(
                        ExecutionConfigOptions.TABLE_EXEC_ACCELERATOR_DEVICE_TASK_OFF_HEAP,
                        MemorySize.parse("64m"));
        tEnv.toDataStream(tEnv.sqlQuery("SELECT " + HEAVY + " AS h FROM t"), Row.class)
                .sinkTo(new DiscardingSink<>());
        return StreamingJobGraphGenerator.createJobGraph(env.getStreamGraph());
    }

    /** The vertex holding the source, by name; the Calc is chained into it when chaining held. */
    private static JobVertex sourceVertex(JobGraph graph) {
        for (JobVertex vertex : graph.getVertices()) {
            if (vertex.getName().startsWith("Source:")) {
                return vertex;
            }
        }
        throw new AssertionError("no source vertex in " + graph);
    }

    @Test
    @DisplayName("without a device requirement the Calc is chained to its source")
    void chainedWhenNothingIsRequired() {
        assertThat(sourceVertex(jobGraph(false)).getName())
                .as("baseline: this is the shape the requirement must not disturb")
                .contains("Calc");
    }

    @Test
    @DisplayName("and it is still chained to its source when a device is required")
    void stillChainedWhenADeviceIsRequired() {
        assertThat(sourceVertex(jobGraph(true)).getName())
                .as(
                        "the source and the Calc must share a vertex, or every record between them "
                                + "is serialized -- M6.0 priced that at about a fifth of wall clock")
                .contains("Calc");
    }

    @Test
    @DisplayName("the region's slot sharing group carries the external resource")
    void theRegionsGroupCarriesTheResource() {
        JobVertex source = sourceVertex(jobGraph(true));
        ResourceProfile profile = source.getSlotSharingGroup().getResourceProfile();

        assertThat(profile)
                .as("an UNKNOWN profile asks for nothing, which is the failure this guards")
                .isNotEqualTo(ResourceProfile.UNKNOWN);
        assertThat(profile.getExtendedResources()).containsKey("gpu");
        assertThat(profile.getCpuCores().getValue().doubleValue()).isEqualTo(0.5);
    }

    /**
     * The requirement covers the region, so the source is in the group too — that is the point.
     *
     * <p>Worth pinning separately from the chaining assertion: chaining is about a vertex, group
     * membership is about a slot, and the two could drift apart without this.
     */
    @Test
    @DisplayName("the whole region is in the named group, not just the accelerated node")
    void theRegionIsInTheNamedGroup() {
        JobGraph graph = jobGraph(true);
        String expected =
                ExecutionConfigOptions.TABLE_EXEC_ACCELERATOR_DEVICE_SLOT_SHARING_GROUP
                        .defaultValue();
        assertThat(sourceVertex(graph).getSlotSharingGroup().getSlotSharingGroupName())
                .isEqualTo(expected);
    }
}
