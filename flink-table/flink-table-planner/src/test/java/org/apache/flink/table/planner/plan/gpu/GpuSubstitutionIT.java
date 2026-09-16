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

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The substitution seam: what happens when a node is selected but the machine cannot serve it.
 *
 * <p>Whether a given expression can be turned into a kernel is covered by the provider module's
 * {@code AccelKernelGeneratorTest}. This is about the surrounding fallback, which has to be silent
 * to the query and correct in its results.
 *
 * <p>flink-table-planner has no dependency on the GPU runtime, so every job here runs on a JVM with
 * no provider on its classpath — the same position as a TaskManager without a GPU. The plan still
 * selects the node for offload, because that decision belongs to the query rather than to this
 * machine, and {@code GpuOrCpuCalcOperatorFactory} falls back when the operator is built. Getting
 * the right answer out is therefore the assertion that matters.
 */
class GpuSubstitutionIT {

    @Test
    @DisplayName("a node selected for offload still returns correct rows with no provider present")
    void selectedNodeFallsBackAndComputesCorrectly() throws Exception {
        TableEnvironment tEnv =
                TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        tEnv.executeSql(
                "CREATE TABLE t (id BIGINT NOT NULL, val DOUBLE NOT NULL) WITH ("
                        + "'connector' = 'datagen', 'number-of-rows' = '200')");
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ACCELERATOR_ENABLED, true);
        // Floors of 1 so even a trivial expression is selected, isolating the fallback from the
        // cost decision.

        String sql = "SELECT id, val, val * 2.0 + 1.0 AS scaled FROM t WHERE val > 0.5";

        // The plan commits to the device: the gate selected the node and a kernel was generated.
        String plan = tEnv.explainSql(sql);
        assertTrue(plan.contains("== GPU Offload =="), plan);
        assertTrue(plan.contains("GPU  subtree 0"), plan);

        // And the job runs anyway, on a machine that has no device, returning the same answers the
        // kernel would have. Before the operator carried both implementations this is the point at
        // which a mis-placed task died.
        int rows = 0;
        try (CloseableIterator<Row> it = tEnv.executeSql(sql).collect()) {
            while (it.hasNext()) {
                Row row = it.next();
                double val = (Double) row.getField(1);
                double scaled = (Double) row.getField(2);
                assertTrue(val > 0.5, "filter let through " + val);
                assertEquals(val * 2.0 + 1.0, scaled, 0.0, "projection is wrong for " + val);
                rows++;
            }
        }
        assertTrue(rows > 0, "the query returned nothing, so nothing was verified");
    }

    @Test
    @DisplayName("an expression with no kernel is reported as such, not as a missing provider")
    void inexpressibleExpressionIsReportedSeparately() {
        TableEnvironment tEnv =
                TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        tEnv.executeSql(
                "CREATE TABLE s (word STRING, n INT) WITH ("
                        + "'connector' = 'datagen', 'number-of-rows' = '10')");
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ACCELERATOR_ENABLED, true);

        String plan = tEnv.explainSql("SELECT word, n + 1 AS m FROM s WHERE n > 0");

        assertTrue(plan.contains("not expressible on device"), plan);
    }
}
