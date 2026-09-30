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

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * M9.6 — EXPLAIN has to be able to answer "was my source considered, and what came back?".
 *
 * <p>Before this it could not: the plan printed a {@code TableSourceScan} and said nothing about
 * whether a device had been asked, so an unsupported format and a supported one nothing had looked
 * at were indistinguishable.
 */
class AcceleratedScanIT {

    @TempDir Path data;

    private TableEnvironment tEnv;

    @BeforeEach
    void setUp() {
        tEnv = TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ACCELERATOR_ENABLED, true);
    }

    private void fileSystemTable(String name, String format) {
        tEnv.executeSql(
                "CREATE TABLE "
                        + name
                        + " (k INT NOT NULL, v DOUBLE NOT NULL) WITH ("
                        + "'connector' = 'filesystem',"
                        + "'path' = '"
                        + data.toUri()
                        + "',"
                        + "'format' = '"
                        + format
                        + "')");
    }

    @Test
    @DisplayName("a file source a device could read is reported, with why it was not taken")
    void aFileSourceIsOfferedAndTheVerdictIsReported() {
        fileSystemTable("parq", "testcsv");

        String plan = tEnv.explainSql("SELECT k, v * 2.0 AS d FROM parq");

        assertThat(plan)
                .as("the scan must be named in the offload section:\n" + plan)
                .contains("TableSourceScan");
        assertThat(plan)
                .as("the reason must name the format and why it was still declined:\n" + plan)
                .contains("a testcsv file source was offered");
        assertThat(plan)
                .as("and must name the reason, not merely refuse:\n" + plan)
                .contains("every consumer of it reads");
    }

    /** The scan is reported for its own sake; the operator above it decides separately. */
    @Test
    @DisplayName("the verdict on the source does not change the verdict on the operator")
    void theOperatorAboveIsUnaffected() {
        fileSystemTable("parq", "testcsv");

        String withScan = tEnv.explainSql("SELECT k, v * 2.0 AS d FROM parq");

        assertThat(withScan)
                .as("the Calc above a reported scan is still considered on its own merits")
                .contains("Calc");
    }

    @Test
    @DisplayName("a source no device reader could open is not reported at all")
    void aNonFileSourceIsNotOffered() {
        tEnv.executeSql(
                "CREATE TABLE gen (k INT NOT NULL, v DOUBLE NOT NULL) WITH ("
                        + "'connector' = 'datagen', 'number-of-rows' = '10')");

        String plan = tEnv.explainSql("SELECT k, v * 2.0 AS d FROM gen");

        assertThat(plan)
                .as("datagen has no path a device reader could take:\n" + plan)
                .doesNotContain("source a device could read");
    }

    /**
     * The plan itself must not move. Offering a scan records a verdict; it does not place the scan
     * on a device, because nothing can yet consume what a device scan would produce.
     */
    @Test
    @DisplayName("reporting a scan leaves the physical plan byte for byte as it was")
    void thePhysicalPlanIsUnchanged() {
        fileSystemTable("parq", "testcsv");
        String offered = physicalPlanOf("SELECT k, v * 2.0 AS d FROM parq");

        TableEnvironment off =
                TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        off.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ACCELERATOR_ENABLED, false);
        off.executeSql(
                "CREATE TABLE parq (k INT NOT NULL, v DOUBLE NOT NULL) WITH ("
                        + "'connector' = 'filesystem','path' = '"
                        + data.toUri()
                        + "','format' = 'testcsv')");
        String baseline =
                physicalPlanOf(off, "SELECT k, v * 2.0 AS d FROM parq");

        assertThat(offered).isEqualTo(baseline);
    }

    private String physicalPlanOf(String sql) {
        return physicalPlanOf(tEnv, sql);
    }

    private static String physicalPlanOf(TableEnvironment env, String sql) {
        String plan = env.explainSql(sql);
        int start = plan.indexOf("== Optimized Physical Plan ==");
        int end = plan.indexOf("== Optimized Execution Plan ==");
        return start < 0 || end < 0 ? plan : plan.substring(start, end);
    }
}
