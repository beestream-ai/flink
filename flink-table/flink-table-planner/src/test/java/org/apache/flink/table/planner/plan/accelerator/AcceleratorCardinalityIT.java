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

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The cardinality gate (M4.3): a subtask that cannot fill one batch does not set a device up.
 *
 * <p>The planner decides what a subtree <em>can</em> run on, from catalog statistics in the client.
 * Whether there is enough work arriving to repay compiling a kernel and reserving a staging arena
 * is a different question, and the planner is the worst place in the system to ask it. Once the
 * input has been produced the byte count is exact and the parallelism is the one the scheduler
 * actually chose, and {@code AcceleratorCardinalityStrategy} carries that pair to the operator
 * factory before the vertex is scheduled.
 *
 * <p>The query shape is not incidental. An offloadable Calc is never the direct consumer of a
 * blocking result — Flink puts the distribution on the edge into the operator that needs it, here
 * the aggregate, and the Calc follows on a forward edge behind it. So the aggregate's input is what
 * gets measured, and it bounds what the Calc can read rather than naming it. A bound is what the
 * question needs: if the most rows the node could possibly see is under one batch, it cannot fill
 * one.
 *
 * <p>Runs under AdaptiveBatch, which is the default scheduler for batch jobs and the only one that
 * runs stream graph optimization strategies at all.
 */
class AcceleratorCardinalityIT {

    /**
     * An aggregate, then arithmetic worth offloading over its result.
     *
     * <p>The aggregate is there to force a blocking exchange, which is the only thing that produces
     * a result for the strategy to measure.
     */
    private static final String QUERY =
            "SELECT k, EXP(s) * LN(s) + SIN(s) * COS(s) AS h FROM "
                    + "(SELECT id % 7 AS k, SUM(val) AS s FROM t GROUP BY id % 7)";

    @AfterEach
    void tearDown() {
        TestAcceleratorProvider.reset();
    }

    private static StreamTableEnvironment batchEnvironment() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        // Pinned so the bound below is arithmetic a reader can follow, not whatever the machine
        // happened to have cores for.
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.executeSql(
                "CREATE TABLE t (id BIGINT NOT NULL, val DOUBLE NOT NULL) WITH ("
                        + "'connector' = 'datagen', 'number-of-rows' = '50',"
                        + "'fields.id.kind' = 'sequence',"
                        + "'fields.id.start' = '1', 'fields.id.end' = '50',"
                        + "'fields.val.kind' = 'sequence',"
                        + "'fields.val.start' = '1', 'fields.val.end' = '50')");
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ACCELERATOR_ENABLED, true);
        tEnv.getConfig()
                .set(ExecutionConfigOptions.TABLE_EXEC_ACCELERATOR_APPROXIMATE_PROJECTIONS, true);
        return tEnv;
    }

    private static List<Row> run(StreamTableEnvironment tEnv) {
        List<Row> rows = new ArrayList<>();
        TableResult result = tEnv.executeSql(QUERY);
        try (CloseableIterator<Row> it = result.collect()) {
            it.forEachRemaining(rows::add);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
        return rows;
    }

    @Test
    @DisplayName("fifty rows against a batch of 262144 never reach a provider")
    void anInputTooSmallToFillABatchDoesNotOffload() {
        StreamTableEnvironment tEnv = batchEnvironment();
        TestAcceleratorProvider.enable(10.0);

        List<Row> rows = run(tEnv);

        assertThat(TestAcceleratorProvider.PLANS_OFFERED.get())
                .as("a provider must not be asked to serve fifty rows against a 262144-row batch")
                .isZero();
        assertThat(TestAcceleratorProvider.ROWS_PROCESSED.get()).isZero();
        assertThat(rows).as("and the query still answers, on the CPU").hasSize(7);
    }

    /**
     * The same query, with the batch small enough that the measured input clears it.
     *
     * <p>This is also where the parallelism half is pinned, and it is pinned by what has to be true
     * for the test to pass at all. {@code table.exec.resource.default-parallelism} is left at its
     * default of -1 — asserted below, so the assertion cannot rot — and a gate dividing by -1
     * produces a negative row count, which is under any batch size and would refuse. Offloading
     * here is only reachable if the divisor was the parallelism the scheduler settled on.
     */
    @Test
    @DisplayName("the same input offloads once a batch is small enough, using real parallelism")
    void enoughRowsForTheBatchStillOffloads() {
        StreamTableEnvironment tEnv = batchEnvironment();
        assertThat(
                        tEnv.getConfig()
                                .get(
                                        ExecutionConfigOptions
                                                .TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM))
                .as(
                        "the configured parallelism must be unusable as a divisor for this to prove anything")
                .isEqualTo(-1);
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ACCELERATOR_BATCH_SIZE, 4);
        TestAcceleratorProvider.enable(10.0);

        List<Row> rows = run(tEnv);

        assertThat(TestAcceleratorProvider.PLANS_OFFERED.get())
                .as("the gate must let this through, and only real parallelism gets it there")
                .isPositive();
        assertThat(TestAcceleratorProvider.ROWS_PROCESSED.get()).isPositive();
        assertThat(rows).hasSize(7);
    }

    @Test
    @DisplayName("and the rows are the same either way")
    void theGateDoesNotChangeTheAnswer() {
        StreamTableEnvironment gated = batchEnvironment();
        TestAcceleratorProvider.enable(10.0);
        List<Row> onCpu = run(gated);

        TestAcceleratorProvider.reset();
        StreamTableEnvironment offloaded = batchEnvironment();
        offloaded.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ACCELERATOR_BATCH_SIZE, 4);
        TestAcceleratorProvider.enable(10.0);
        List<Row> accelerated = run(offloaded);

        assertThat(TestAcceleratorProvider.ROWS_PROCESSED.get()).isPositive();
        assertThat(accelerated).containsExactlyInAnyOrderElementsOf(onCpu);
    }
}
