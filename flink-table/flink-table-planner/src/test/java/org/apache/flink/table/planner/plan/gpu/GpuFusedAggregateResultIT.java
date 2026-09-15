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
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Does fusing a {@code Calc} into the aggregate above it change the answer?
 *
 * <p>No device is involved here, and that is the point. Fusion collapses two ExecNodes into one
 * operator, so on a machine without a GPU -- which is every machine running this test -- the fused
 * node runs both generated operators chained together. If that chaining is wrong, it is wrong for
 * everyone who enables offload, whatever hardware they have.
 *
 * <p>The comparison is against the same query with offload off, which is the unfused plan Flink
 * would have run. Sums are compared exactly: both arms accumulate in the same order through the
 * same generated aggregate, so anything but equality means the chain lost or duplicated a record.
 */
class GpuFusedAggregateResultIT {

    private static final String HEAVY =
            "EXP(val / 100.0E0) * LN(ABS(val) + 1.0E0) + SIN(val) * COS(val)"
                    + " + POWER(val / 4.0E0, 3.0E0)";

    /**
     * A deterministic source, which matters more than it looks.
     *
     * <p>{@code datagen} with a range is unseeded, so two environments reading it see different
     * numbers and every comparison below fails by a percent or two -- which looks exactly like a
     * chaining bug and is not one. Deriving the values from a sequence makes both arms read the
     * same input, so a difference means what it is supposed to mean.
     *
     * <p>The sequence is an {@code INT} rather than a {@code BIGINT} on purpose. The kernel
     * generator refuses a {@code BIGINT} operand -- values past 2^53 would not survive a double
     * staging buffer -- so a {@code CAST(id AS DOUBLE)} off a {@code BIGINT} makes the Calc
     * ineligible, nothing fuses, and these comparisons quietly test the CPU plan against itself.
     */
    private TableEnvironment env(boolean offload) {
        TableEnvironment tEnv =
                TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        tEnv.executeSql(
                "CREATE TABLE seq (id INT NOT NULL) WITH ("
                        + "'connector' = 'datagen',"
                        + " 'number-of-rows' = '20000',"
                        + " 'fields.id.kind' = 'sequence',"
                        + " 'fields.id.start' = '1',"
                        + " 'fields.id.end' = '20000')");
        tEnv.executeSql(
                "CREATE VIEW t AS SELECT id,"
                        + " CAST(MOD(id, 4) + 1 AS INT) AS k,"
                        + " CAST(id AS DOUBLE) / 1000.0E0 + 0.5E0 AS val"
                        + " FROM seq");
        tEnv.getConfig().set(GpuOffloadOptions.ENABLED, offload);
        // Without this the subtree is below the total-work floor at this row count and neither arm
        // offloads, so the comparison passes by testing the same CPU plan against itself. The
        // first version of this test did exactly that.
        // One subtask, so the accumulation order is fixed. A parallel SUM combines partials in
        // whatever order the subtasks finish, and floating-point addition is not associative, so
        // the last digits move between runs of the *same* plan -- which would drown the thing this
        // is looking for. Reassociation is measured elsewhere; here it is noise.
        tEnv.getConfig()
                .getConfiguration()
                .setString("table.exec.resource.default-parallelism", "1");
        return tEnv;
    }

    private List<Row> run(boolean offload, String sql) throws Exception {
        List<Row> rows = new ArrayList<>();
        try (CloseableIterator<Row> it = env(offload).executeSql(sql).collect()) {
            it.forEachRemaining(rows::add);
        }
        return rows;
    }

    /** Compared as sorted sets: an ungrouped aggregate emits one row, a grouped one no order. */
    private void agrees(String sql) throws Exception {
        List<String> plain = new ArrayList<>();
        run(false, sql).forEach(r -> plain.add(r.toString()));
        List<String> fused = new ArrayList<>();
        run(true, sql).forEach(r -> fused.add(r.toString()));
        plain.sort(null);
        fused.sort(null);
        assertEquals(plain, fused, sql);
    }

    /**
     * The comparison above is only worth anything if the offloaded arm actually fused.
     *
     * <p>Twice while writing these tests it did not, and they passed anyway by comparing the CPU
     * plan against itself: once because the row count was below the total-work floor, once because
     * a {@code CAST} off a {@code BIGINT} made the Calc ineligible. Asserting the plan closes that.
     */
    @Test
    @DisplayName("the offloaded arm really does fuse the pair into one subtree")
    void theArmsActuallyDiffer() {
        String sql = "SELECT SUM(h) FROM (SELECT " + HEAVY + " AS h FROM t)";
        String plan = env(true).explainSql(sql);
        String section = plan.substring(plan.indexOf("== GPU Offload =="));
        assertTrue(section.contains("GPU  subtree"), section);
        assertTrue(section.contains("LocalHashAggregate"), section);
        assertFalse(
                env(false).explainSql(sql).contains("GPU  subtree"),
                "the baseline arm must not offload");
    }

    @Test
    @DisplayName("one SUM over a fused Calc")
    void oneSum() throws Exception {
        agrees("SELECT SUM(h) FROM (SELECT " + HEAVY + " AS h FROM t)");
    }

    @Test
    @DisplayName("several SUMs, which is the shape a single contraction serves")
    void severalSums() throws Exception {
        agrees(
                "SELECT SUM(h), SUM(h * 2.0E0), SUM(h + 1.0E0) FROM (SELECT "
                        + HEAVY
                        + " AS h FROM t)");
    }

    @Test
    @DisplayName("SUM beside COUNT(*), which the device answers from its staged row count")
    void sumAndCount() throws Exception {
        agrees("SELECT SUM(h), COUNT(*) FROM (SELECT " + HEAVY + " AS h FROM t)");
    }

    @Test
    @DisplayName("a filter in the fused Calc, so not every row reaches the contraction")
    void withAFilter() throws Exception {
        agrees(
                "SELECT SUM(h) FROM (SELECT "
                        + HEAVY
                        + " AS h FROM t WHERE val > 5.0E0 AND val < 15.0E0)");
    }

    @Test
    @DisplayName("an empty input still sums to NULL rather than to zero")
    void emptyInput() throws Exception {
        agrees("SELECT SUM(h) FROM (SELECT " + HEAVY + " AS h FROM t WHERE val > 1000.0E0)");
    }

    @Test
    @DisplayName("GROUP BY is not fused, and must still be right")
    void grouped() throws Exception {
        agrees("SELECT k, SUM(h) FROM (SELECT k, " + HEAVY + " AS h FROM t) GROUP BY k");
    }
}
