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
import org.apache.flink.table.api.config.OptimizerConfigOptions;
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
 * M5.5: the two-input seam. A join reaches an accelerator, and joins what the CPU plan joins.
 *
 * <p>The first offloadable node here with two inputs, and everything these cases pin is about that
 * rather than about joining. A one-input operator is handed rows and emits rows; a two-input one is
 * asked <em>which input to read</em>, has to be given them in the order the plan assumed, and has
 * to emit its output in the query's field order rather than in the order it happened to receive
 * them. Each of those is a way the seam can be wrong while every row still comes out somewhere.
 *
 * <p>Runs against {@link TestAcceleratorProvider}, which joins on the host, so this is the planner
 * half on a machine with no device.
 */
class AcceleratedJoinIT {

    private TableEnvironment tEnv;

    @BeforeEach
    void setUp() {
        tEnv = TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        tEnv.executeSql(
                "CREATE TABLE dim (k INT NOT NULL, label BIGINT NOT NULL) WITH ("
                        + "'connector' = 'datagen', 'number-of-rows' = '50',"
                        + "'fields.k.kind' = 'sequence',"
                        + "'fields.k.start' = '1', 'fields.k.end' = '50',"
                        + "'fields.label.kind' = 'sequence',"
                        + "'fields.label.start' = '1000', 'fields.label.end' = '1049')");
        tEnv.executeSql(
                "CREATE TABLE fact (k INT NOT NULL, val DOUBLE NOT NULL) WITH ("
                        + "'connector' = 'datagen', 'number-of-rows' = '200',"
                        + "'fields.k.kind' = 'sequence',"
                        + "'fields.k.start' = '1', 'fields.k.end' = '200',"
                        + "'fields.val.kind' = 'sequence',"
                        + "'fields.val.start' = '1', 'fields.val.end' = '200')");
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ACCELERATOR_ENABLED, true);
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ACCELERATOR_BATCH_SIZE, 16);
        // A broadcast join is out of scope (M5.0), and at these sizes the planner would pick one
        // every time -- so the threshold goes to zero and the shuffle join is what gets built.
        tEnv.getConfig()
                .set(OptimizerConfigOptions.TABLE_OPTIMIZER_BROADCAST_JOIN_THRESHOLD, -1L);
        // Flink 2.3 wraps a shuffle join in an AdaptiveJoin by default, which decides at runtime
        // whether to broadcast. That node is not offloaded -- see BatchExecHashJoin.substitute --
        // so the statically planned join is what these cases are about.
        tEnv.getConfig()
                .set(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_ADAPTIVE_BROADCAST_JOIN_STRATEGY,
                        OptimizerConfigOptions.AdaptiveBroadcastJoinStrategy.NONE);
        tEnv.getConfig()
                .set(
                        OptimizerConfigOptions
                                .TABLE_OPTIMIZER_ADAPTIVE_SKEWED_JOIN_OPTIMIZATION_STRATEGY,
                        OptimizerConfigOptions.AdaptiveSkewedJoinOptimizationStrategy.NONE);
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
    @DisplayName("a single-equality inner join is offered to a provider, and the provider runs it")
    void theJoinReachesTheAccelerator() {
        TestAcceleratorProvider.enable(10.0);

        List<Row> rows =
                run("SELECT f.k, f.val, d.label FROM fact f, dim d WHERE f.k = d.k");

        assertThat(TestAcceleratorProvider.PLANS_OFFERED.get())
                .as("a join must be offered; before M5.5 the IR could not hold one")
                .isPositive();
        assertThat(TestAcceleratorProvider.LAST_WAS_JOIN.get())
                .as("the subtree offered must be the join itself")
                .isTrue();
        assertThat(TestAcceleratorProvider.ROWS_PROCESSED.get())
                .as("probe rows must have gone through the accelerated operator, not around it")
                .isPositive();
        // 50 of the fact table's 200 keys have a match in the dimension table.
        assertThat(rows).hasSize(50);
    }

    @Test
    @DisplayName("the accelerated join returns exactly what the CPU plan returns")
    void theAcceleratedJoinAgreesWithTheCpu() {
        String sql = "SELECT f.k, f.val, d.label FROM fact f, dim d WHERE f.k = d.k";

        List<Row> onCpu = run(sql);

        TestAcceleratorProvider.enable(10.0);
        List<Row> accelerated = run(sql);

        assertThat(TestAcceleratorProvider.LAST_WAS_JOIN.get()).isTrue();
        // Order-insensitive, and here that is right rather than lax: a join states no order, and
        // asserting one would pin an implementation detail of whichever side was built.
        assertThat(accelerated).containsExactlyInAnyOrderElementsOf(onCpu);
    }

    @Test
    @DisplayName("the output carries the query's field order, not the build/probe order")
    void theOutputFollowsTheQuery() {
        TestAcceleratorProvider.enable(10.0);

        // The dimension table is written first here and second in the case above. Whichever side
        // the planner decides to build, `d.label` must still come out where the query put it --
        // which is the one thing a two-input operator can get wrong while emitting every row.
        List<Row> rows = run("SELECT d.label, f.val FROM dim d, fact f WHERE d.k = f.k");

        assertThat(TestAcceleratorProvider.LAST_WAS_JOIN.get()).isTrue();
        assertThat(rows).hasSize(50);
        for (Row row : rows) {
            long label = ((Number) row.getField(0)).longValue();
            double val = ((Number) row.getField(1)).doubleValue();
            // label = 999 + k and val = k, so a row whose halves came from different pairs, or
            // whose fields were emitted in build order, fails here.
            assertThat(label).as("label and val disagree about the key").isEqualTo(999L + (long) val);
        }
    }

    @Test
    @DisplayName("an outer join is not offered at all")
    void anOuterJoinIsDeclined() {
        TestAcceleratorProvider.enable(10.0);

        run("SELECT f.k, d.label FROM fact f LEFT JOIN dim d ON f.k = d.k");

        assertThat(TestAcceleratorProvider.LAST_WAS_JOIN.get())
                .as("an outer join has to emit rows that matched nothing, which the IR cannot say")
                .isFalse();
    }

    @Test
    @DisplayName("a join with a residual condition is not offered")
    void aResidualConditionIsDeclined() {
        TestAcceleratorProvider.enable(10.0);

        run("SELECT f.k, d.label FROM fact f, dim d WHERE f.k = d.k AND f.val > d.label");

        assertThat(TestAcceleratorProvider.LAST_WAS_JOIN.get())
                .as("a residual ON predicate is query-defined code over a pair of rows")
                .isFalse();
    }
}
