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
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.planner.plan.gpu.GpuOffloadOptions;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The first tests in this repository that exercise the accelerator path rather than its fallback.
 *
 * <p>Every other integration test here takes the CPU branch, because the planner module depends on
 * no provider and {@code AcceleratorProviders.find} therefore returns empty — so the suite proved
 * only that offload degrades cleanly, never that it runs. Review finding E2.
 *
 * <p>{@link TestAcceleratorProvider} closes that without hardware: it accepts the IR and evaluates
 * it on the host. What it proves is the wiring and the IR, not a device.
 */
class AcceleratorPathIT {

    private static final String QUERY =
            "SELECT id, EXP(val) * LN(val) + SIN(val) * COS(val) AS h FROM t WHERE val > 10.0";

    private TableEnvironment tEnv;

    @BeforeEach
    void setUp() {
        tEnv = TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        tEnv.executeSql(
                // Sequences, not random values: two runs of the same query must produce the same
                // rows, or comparing the accelerated and CPU results compares two datasets.
                "CREATE TABLE t (id BIGINT NOT NULL, val DOUBLE NOT NULL) WITH ("
                        + "'connector' = 'datagen', 'number-of-rows' = '50',"
                        + "'fields.id.kind' = 'sequence',"
                        + "'fields.id.start' = '1', 'fields.id.end' = '50',"
                        + "'fields.val.kind' = 'sequence',"
                        + "'fields.val.start' = '1', 'fields.val.end' = '50')");
    }

    @AfterEach
    void tearDown() {
        TestAcceleratorProvider.reset();
    }

    private List<Row> run(String sql) {
        List<Row> rows = new ArrayList<>();
        TableResult result = tEnv.executeSql(sql);
        try (CloseableIterator<Row> it = result.collect()) {
            it.forEachRemaining(rows::add);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
        return rows;
    }

    @Test
    @DisplayName("the accelerator path actually runs, and the provider sees the rows")
    void acceleratorPathRuns() {
        tEnv.getConfig().set(GpuOffloadOptions.ENABLED, true);
        TestAcceleratorProvider.enable(10.0);

        List<Row> rows = run(QUERY);

        assertThat(TestAcceleratorProvider.PLANS_OFFERED.get())
                .as("the provider must have been asked and must have accepted")
                .isPositive();
        assertThat(TestAcceleratorProvider.ROWS_PROCESSED.get())
                .as("rows must have gone through the accelerator operator, not around it")
                .isPositive();
        assertThat(rows).isNotEmpty();
    }

    @Test
    @DisplayName("with no provider willing, the CPU runs and the results are identical")
    void resultsMatchTheCpuPath() {
        tEnv.getConfig().set(GpuOffloadOptions.ENABLED, true);
        TestAcceleratorProvider.enable(10.0);
        List<Row> accelerated = run(QUERY);
        int seen = TestAcceleratorProvider.ROWS_PROCESSED.get();

        TestAcceleratorProvider.reset();
        List<Row> onCpu = run(QUERY);

        assertThat(seen).isPositive();
        assertThat(TestAcceleratorProvider.ROWS_PROCESSED.get())
                .as("the second run must not have touched the provider")
                .isEqualTo(seen);
        assertThat(accelerated).containsExactlyInAnyOrderElementsOf(onCpu);
    }

    @Test
    @DisplayName("a provider claiming to be slower is refused, and the CPU runs instead")
    void slowProviderIsRefused() {
        tEnv.getConfig().set(GpuOffloadOptions.ENABLED, true);
        // 0.54x is the measured figure for the query the planner used to refuse at plan time.
        TestAcceleratorProvider.enable(0.54);

        List<Row> rows = run(QUERY);

        assertThat(TestAcceleratorProvider.PLANS_OFFERED.get())
                .as("the provider is asked...")
                .isPositive();
        assertThat(TestAcceleratorProvider.ROWS_PROCESSED.get())
                .as("...and its answer is rejected, so no row reaches it")
                .isZero();
        assertThat(rows).isNotEmpty();
    }

    @Test
    @DisplayName("the provider declines by default, so its presence reroutes nothing")
    void declinesUnlessEnabled() {
        tEnv.getConfig().set(GpuOffloadOptions.ENABLED, true);

        run(QUERY);

        assertThat(TestAcceleratorProvider.ROWS_PROCESSED.get()).isZero();
    }
}
