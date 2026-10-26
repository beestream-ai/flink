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
import org.apache.flink.table.api.config.ExecutionConfigOptions;
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
 * M5.2: a running total reaches an accelerator, and accumulates what the CPU plan accumulates.
 *
 * <p>Cross-row like the sort and the join, and unlike either of them it is <em>streaming</em>: it
 * holds nothing and carries one number forward. That is the property these cases exist to pin,
 * because it is what makes the batch boundary the interesting place. An operator that started each
 * batch from zero produces a result that is monotonic within every batch, has the right number of
 * rows, and is wrong from the first boundary onwards — so the row count and the shape of the answer
 * both look right while the answer is not.
 *
 * <p>The batch size is therefore set far below the row count on purpose, and the assertion is
 * against a running total computed here rather than against the final value.
 *
 * <p>Runs against {@link TestAcceleratorProvider}, which accumulates on the host, so this is the
 * planner half on a machine with no device.
 */
class AcceleratedOverAggregateIT {

    private static final int ROWS = 200;

    private TableEnvironment tEnv;

    @BeforeEach
    void setUp() {
        tEnv = TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        tEnv.executeSql(
                "CREATE TABLE t (k INT NOT NULL, val DOUBLE NOT NULL) WITH ("
                        + "'connector' = 'datagen', 'number-of-rows' = '"
                        + ROWS
                        + "',"
                        + "'fields.k.kind' = 'sequence',"
                        + "'fields.k.start' = '1', 'fields.k.end' = '"
                        + ROWS
                        + "',"
                        + "'fields.val.kind' = 'sequence',"
                        + "'fields.val.start' = '1', 'fields.val.end' = '"
                        + ROWS
                        + "')");
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ACCELERATOR_ENABLED, true);
        tEnv.getConfig()
                .set(ExecutionConfigOptions.TABLE_EXEC_ACCELERATOR_APPROXIMATE_PROJECTIONS, true);
        // Well under the row count, so the running total has to cross a boundary several times.
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ACCELERATOR_BATCH_SIZE, 16);
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

    private static final String RUNNING =
            "SELECT k, SUM(val) OVER (ORDER BY k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"
                    + " FROM t";

    @Test
    @DisplayName("an unbounded-preceding running total is offered, and the provider runs it")
    void theWindowReachesTheAccelerator() {
        TestAcceleratorProvider.enable(10.0);

        List<Row> rows = run(RUNNING);

        assertThat(TestAcceleratorProvider.PLANS_OFFERED.get())
                .as("a window must be offered; before M5.2 the IR could not hold one")
                .isPositive();
        assertThat(TestAcceleratorProvider.OVER_PLANS_OFFERED.get())
                .as("the window itself must be offered, not only the sort beneath it")
                .isPositive();
        assertThat(TestAcceleratorProvider.ROWS_PROCESSED.get())
                .as("rows must have gone through the accelerated operator, not around it")
                .isPositive();
        assertThat(rows).hasSize(ROWS);
    }

    @Test
    @DisplayName("the running total is right across batch boundaries, not only within them")
    void theTotalCrossesBatches() {
        TestAcceleratorProvider.enable(10.0);

        List<Row> rows = run(RUNNING);

        assertThat(TestAcceleratorProvider.OVER_PLANS_OFFERED.get()).isPositive();
        assertThat(rows).hasSize(ROWS);
        // val = k, so the running total after row k is k(k+1)/2. An operator that restarted its
        // accumulation at each batch would satisfy every other assertion in this class.
        double expected = 0.0;
        for (Row row : rows) {
            int k = ((Number) row.getField(0)).intValue();
            expected += k;
            assertThat(((Number) row.getField(1)).doubleValue())
                    .as("running total at k=" + k)
                    .isEqualTo(expected);
        }
    }

    @Test
    @DisplayName("the accelerated window returns exactly what the CPU plan returns")
    void theAcceleratedWindowAgreesWithTheCpu() {
        List<Row> onCpu = run(RUNNING);

        TestAcceleratorProvider.enable(10.0);
        List<Row> accelerated = run(RUNNING);

        assertThat(TestAcceleratorProvider.OVER_PLANS_OFFERED.get()).isPositive();
        // Order matters here and nowhere else in this suite: a running total is defined by the
        // order it accumulated in, so an order-insensitive assertion would accept a shuffle.
        assertThat(accelerated).containsExactlyElementsOf(onCpu);
    }

    @Test
    @DisplayName("a PARTITION BY window is not offered")
    void aPartitionedWindowIsDeclined() {
        TestAcceleratorProvider.enable(10.0);

        run(
                "SELECT k, SUM(val) OVER (PARTITION BY k ORDER BY k"
                        + " ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t");

        assertThat(TestAcceleratorProvider.OVER_PLANS_OFFERED.get())
                .as("a partitioned running total is a segmented scan, which is not bound")
                .isZero();
    }

    @Test
    @DisplayName("a bounded ROWS frame is not offered")
    void aBoundedFrameIsDeclined() {
        TestAcceleratorProvider.enable(10.0);

        run("SELECT k, SUM(val) OVER (ORDER BY k ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM t");

        assertThat(TestAcceleratorProvider.OVER_PLANS_OFFERED.get())
                .as("a bounded frame is a sliding window, not a prefix sum")
                .isZero();
    }

    @Test
    @DisplayName("a RANGE frame is not offered")
    void aRangeFrameIsDeclined() {
        TestAcceleratorProvider.enable(10.0);

        run(
                "SELECT k, SUM(val) OVER (ORDER BY k"
                        + " RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t");

        assertThat(TestAcceleratorProvider.OVER_PLANS_OFFERED.get())
                .as("a RANGE frame resolves peer groups, which a prefix sum cannot express")
                .isZero();
    }
}
