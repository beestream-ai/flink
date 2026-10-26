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

package org.apache.flink.table.planner.plan.accelerator;

import org.apache.flink.table.api.CompiledPlan;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.PlanReference;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The compiled-plan contract (M4.4): a compiled plan never offloads, and says why.
 *
 * <p>The decision the offload processor makes is hung on the {@code ExecNode} as a transient {@code
 * GpuOffloadAssignment} and is not part of the node's JSON. So a plan written to a file and read
 * back has no decision at all, while the {@code CompiledPlan} object it was written from still
 * carries one. Left alone, the same plan would offload or not depending on whether it had been
 * through a file — which is the one thing a compiled plan exists to rule out.
 *
 * <p>Two contracts were available. Serialise the decision inputs so a restored plan reproduces the
 * original verdict; or refuse on compiled plans and record the reason. This project takes the
 * second, because the first cannot deliver what it promises: the verdict that matters is reached
 * per subtask on the TaskManager against the provider and hardware it finds there, so no amount of
 * plan state reproduces it. What the plan could carry is eligibility, and pinning the accelerator
 * IR and the cost model into a restorable format to carry it is a large and permanent commitment
 * for a feature that re-decides at runtime anyway.
 *
 * <p>These tests run with {@link TestAcceleratorProvider} on the classpath, so "did it offload" is
 * observable without a device: the provider counts the plans it is offered and the rows it sees.
 */
class AcceleratorCompiledPlanIT {

    /**
     * Arithmetic-heavy enough that the cost model selects it, so a refusal is the only variable.
     */
    private static final String QUERY =
            "SELECT id, EXP(val) * LN(val) + SIN(val) * COS(val) AS h FROM t WHERE val > 10.0";

    private TableEnvironment tEnv;
    private String sinkId;

    @TempDir Path tempDir;

    @BeforeEach
    void setUp() {
        tEnv = TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        tEnv.executeSql(
                "CREATE TABLE t (id BIGINT NOT NULL, val DOUBLE NOT NULL) WITH ("
                        + "'connector' = 'datagen', 'number-of-rows' = '50',"
                        + "'fields.id.kind' = 'sequence',"
                        + "'fields.id.start' = '1', 'fields.id.end' = '50',"
                        + "'fields.val.kind' = 'sequence',"
                        + "'fields.val.start' = '1', 'fields.val.end' = '50')");
        sinkId = "sink_" + System.nanoTime();
        tEnv.executeSql(
                "CREATE TABLE "
                        + sinkId
                        + " (id BIGINT NOT NULL, h DOUBLE NOT NULL) WITH ("
                        + "'connector' = 'values', 'sink-insert-only' = 'true')");
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ACCELERATOR_ENABLED, true);
        tEnv.getConfig()
                .set(ExecutionConfigOptions.TABLE_EXEC_ACCELERATOR_APPROXIMATE_PROJECTIONS, true);
    }

    @AfterEach
    void tearDown() {
        TestAcceleratorProvider.reset();
    }

    private String insert() {
        return "INSERT INTO " + sinkId + " " + QUERY;
    }

    @Test
    @DisplayName("the same INSERT offloads when it is planned normally")
    void theQueryOffloadsWhenNotCompiled() throws Exception {
        TestAcceleratorProvider.enable(10.0);

        tEnv.executeSql(insert()).await();

        assertThat(TestAcceleratorProvider.PLANS_OFFERED.get())
                .as("nothing is being tested unless this query offloads without a compiled plan")
                .isPositive();
        assertThat(TestAcceleratorProvider.ROWS_PROCESSED.get()).isPositive();
        assertThat(TestValuesTableFactory.getRawResultsAsStrings(sinkId)).isNotEmpty();
    }

    @Test
    @DisplayName(
            "a plan compiled and restored from JSON does not offload, and produces the same rows")
    void aRestoredPlanDoesNotOffload() throws Exception {
        // The rows the normal path produces, kept to compare against.
        TestAcceleratorProvider.enable(10.0);
        tEnv.executeSql(insert()).await();
        List<String> viaOffload = TestValuesTableFactory.getRawResultsAsStrings(sinkId);
        int offloadedRows = TestAcceleratorProvider.ROWS_PROCESSED.get();
        assertThat(offloadedRows).as("the baseline run must have offloaded").isPositive();

        TestAcceleratorProvider.reset();
        TestAcceleratorProvider.enable(10.0);

        Path planFile = tempDir.resolve("plan.json");
        tEnv.compilePlanSql(insert()).writeToFile(planFile.toFile());
        CompiledPlan restored = tEnv.loadPlan(PlanReference.fromFile(planFile.toFile()));
        restored.execute().await();

        assertThat(TestAcceleratorProvider.PLANS_OFFERED.get())
                .as("a restored plan must never be offered to a provider")
                .isZero();
        assertThat(TestAcceleratorProvider.ROWS_PROCESSED.get())
                .as("no row may reach an accelerator from a restored plan")
                .isZero();
        assertThat(TestValuesTableFactory.getRawResultsAsStrings(sinkId))
                .as("the CPU must produce the rows the accelerator produced")
                .containsExactlyInAnyOrderElementsOf(viaOffload);
    }

    /**
     * The plan format is Flink's, and the accelerator does not get to put anything in it.
     *
     * <p>Not a tidiness assertion. {@code ExecNodeBase} holds the offload assignment in a {@code
     * transient} field, which is a Java serialization marker and means nothing to Jackson: Jackson
     * walks accessors, found {@code getGpuOffloadAssignment()}, and wrote the whole decision into
     * the JSON. {@link org.apache.flink.table.planner.plan.gpu.GpuOffloadAssignment} has no
     * creator, so every compiled plan produced with offload enabled was unreadable — the round trip
     * in {@link #aRestoredPlanDoesNotOffload()} is what surfaced it. The estimated row count went
     * the same way.
     */
    @Test
    @DisplayName("nothing about the accelerator reaches the plan JSON")
    void thePlanFormatCarriesNoAcceleratorState() {
        TestAcceleratorProvider.enable(10.0);

        String json = tEnv.compilePlanSql(insert()).asJsonString();

        assertThat(json).doesNotContain("gpuOffloadAssignment").doesNotContain("estimatedRowCount");
    }

    @Test
    @DisplayName(
            "an in-memory CompiledPlan does not offload either, so the file makes no difference")
    void anUnwrittenCompiledPlanDoesNotOffload() throws Exception {
        TestAcceleratorProvider.enable(10.0);

        CompiledPlan plan = tEnv.compilePlanSql(insert());
        plan.execute().await();

        assertThat(TestAcceleratorProvider.PLANS_OFFERED.get())
                .as("whether the plan went through a file must not change what runs")
                .isZero();
        assertThat(TestAcceleratorProvider.ROWS_PROCESSED.get()).isZero();
        assertThat(TestValuesTableFactory.getRawResultsAsStrings(sinkId)).isNotEmpty();
    }
}
