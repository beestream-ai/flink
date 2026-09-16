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

import org.apache.flink.configuration.MemorySize;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Asking the scheduler to put an offloaded task where a device is.
 *
 * <p>The requirement is opt-in and the reason is asymmetry rather than caution. Without it a
 * misplaced task runs on the CPU and logs that it did; with one that nothing can satisfy, the job
 * waits for a slot that never arrives, and Flink has no third option because a resource requirement
 * is not a preference.
 */
class GpuPlacementIT {

    private static final String HEAVY =
            "SELECT id, EXP(val) * LN(val) + SIN(val) * COS(val) + POWER(val, 3.0) AS h FROM t";

    /** Below the cost floor, so it is never selected and never asks for anything. */
    private static final String CHEAP = "SELECT id, val * 2.0 AS h FROM t";

    private TableEnvironment tEnv;

    @BeforeEach
    void setUp() {
        tEnv = TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        tEnv.executeSql(
                "CREATE TABLE t (id BIGINT NOT NULL, val DOUBLE NOT NULL) WITH ("
                        + "'connector' = 'datagen', 'number-of-rows' = '2000000')");
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ACCELERATOR_ENABLED, true);
    }

    private void requireDevice() {
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ACCELERATOR_REQUIRE_DEVICE, true);
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ACCELERATOR_DEVICE_CPU_CORES, 1.0);
        tEnv.getConfig()
                .set(
                        ExecutionConfigOptions.TABLE_EXEC_ACCELERATOR_DEVICE_TASK_HEAP,
                        MemorySize.parse("1 gb"));
    }

    @Test
    @DisplayName("nothing is requested unless asked for, which is the shipped default")
    void offByDefault() {
        assertDoesNotThrow(() -> tEnv.explainSql(HEAVY));
    }

    @Test
    @DisplayName("a fully specified slot is accepted")
    void requestsADeviceWhenConfigured() {
        requireDevice();
        assertDoesNotThrow(() -> tEnv.explainSql(HEAVY));
    }

    /**
     * The whole slot has to be specified, and the error should say so in this fork's own terms.
     *
     * <p>{@code SlotSharingGroup.build()} rejects a group naming an external resource without cpu
     * and heap. Left to itself it reports that cpu cores and task heap memory are required, which
     * is true and does not mention that {@code require-device} is what asked for them.
     */
    @Test
    @DisplayName("and an under-specified one fails with a reason, not with Flink's")
    void refusesAHalfSpecifiedSlot() {
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ACCELERATOR_REQUIRE_DEVICE, true);
        IllegalArgumentException thrown =
                assertThrows(IllegalArgumentException.class, () -> tEnv.explainSql(HEAVY));
        assertTrue(
                thrown.getMessage()
                        .contains(
                                ExecutionConfigOptions.TABLE_EXEC_ACCELERATOR_DEVICE_CPU_CORES
                                        .key()),
                thrown.getMessage());
    }

    /**
     * The default name is refused because it is too broad, not because it is ignored.
     *
     * <p>Since M6.1 the accelerated node keeps its chain by having its whole pipelined region put
     * into the named group, so "the default group would not break the chain" is no longer a reason
     * to want it. What is left is the reason to refuse it: {@code
     * StreamingJobGraphGenerator.buildVertexRegionSlotSharingGroups} applies the resources
     * registered under the default name to <em>every</em> region in the job, so a job with a
     * device-free branch would demand a device for that branch too.
     *
     * <p>The earlier rationale here — that Flink never reads resources registered under the default
     * name — was wrong, and the message said so to users.
     */
    @Test
    @DisplayName("the default slot sharing group is refused for being too broad")
    void refusesTheDefaultGroup() {
        requireDevice();
        tEnv.getConfig()
                .set(
                        ExecutionConfigOptions.TABLE_EXEC_ACCELERATOR_DEVICE_SLOT_SHARING_GROUP,
                        "default");
        IllegalArgumentException thrown =
                assertThrows(IllegalArgumentException.class, () -> tEnv.explainSql(HEAVY));
        assertTrue(
                thrown.getMessage().contains("every pipelined region in the job"),
                thrown.getMessage());
    }

    /** A node that was never selected must not drag a device requirement onto the job. */
    @Test
    @DisplayName("a query that does not offload asks for nothing, even half-configured")
    void requiringADeviceDemandsAWholeSlotSpecification() {
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ACCELERATOR_REQUIRE_DEVICE, true);

        // Every expressible query is eligible now, so asking for a device is no longer something
        // only heavy queries trigger. Flink rejects a slot sharing group naming an external
        // resource without cpu and heap, and the planner has no basis for inventing either, so it
        // fails here naming both options rather than deeper inside SlotSharingGroup.build().
        IllegalArgumentException thrown =
                assertThrows(IllegalArgumentException.class, () -> tEnv.explainSql(CHEAP));
        assertTrue(thrown.getMessage().contains("cpu-cores"), thrown::getMessage);
        assertTrue(thrown.getMessage().contains("task-heap"), thrown::getMessage);
    }
}
