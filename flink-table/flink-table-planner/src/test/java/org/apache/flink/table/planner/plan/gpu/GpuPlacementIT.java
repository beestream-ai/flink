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
        tEnv.getConfig().set(GpuOffloadOptions.ENABLED, true);
    }

    private void requireDevice() {
        tEnv.getConfig().set(GpuOffloadOptions.REQUIRE_DEVICE, true);
        tEnv.getConfig().set(GpuOffloadOptions.DEVICE_CPU_CORES, 1.0);
        tEnv.getConfig().set(GpuOffloadOptions.DEVICE_TASK_HEAP, MemorySize.parse("1 gb"));
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
        tEnv.getConfig().set(GpuOffloadOptions.REQUIRE_DEVICE, true);
        IllegalArgumentException thrown =
                assertThrows(IllegalArgumentException.class, () -> tEnv.explainSql(HEAVY));
        assertTrue(
                thrown.getMessage().contains(GpuOffloadOptions.DEVICE_CPU_CORES.key()),
                thrown.getMessage());
    }

    /**
     * The one name that would not break the chain is the one Flink ignores.
     *
     * <p>A vertex in the default group resolves to its region's slot sharing group, and the
     * resources registered under the default name are never read. Accepting the setting would
     * produce a job that looks configured for a device and asks for nothing.
     */
    @Test
    @DisplayName("the default slot sharing group is refused rather than silently ignored")
    void refusesTheDefaultGroup() {
        requireDevice();
        tEnv.getConfig().set(GpuOffloadOptions.DEVICE_SLOT_SHARING_GROUP, "default");
        IllegalArgumentException thrown =
                assertThrows(IllegalArgumentException.class, () -> tEnv.explainSql(HEAVY));
        assertTrue(thrown.getMessage().contains("silently dropped"), thrown.getMessage());
    }

    /** A node that was never selected must not drag a device requirement onto the job. */
    @Test
    @DisplayName("a query that does not offload asks for nothing, even half-configured")
    void requiringADeviceDemandsAWholeSlotSpecification() {
        tEnv.getConfig().set(GpuOffloadOptions.REQUIRE_DEVICE, true);

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
