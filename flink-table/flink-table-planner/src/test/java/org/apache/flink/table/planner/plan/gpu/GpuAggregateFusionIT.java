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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Does the planner see an ungrouped {@code SUM} over a {@code Calc} as one device subtree?
 *
 * <p>This is the recognition half of aggregate offload. The processor already groups adjacent
 * offloadable nodes and sums their costs, so an aggregate that opts in joins the {@code Calc}
 * feeding it without any new grouping logic -- which is the point of costing a subtree rather than
 * a node.
 *
 * <p>The costs make that necessary rather than merely tidy. Contracting a row is a multiply and an
 * add, so an aggregate alone never clears a floor of 96 and never should; it is the expression
 * underneath that pays for the offload, and the aggregate rides along.
 */
class GpuAggregateFusionIT {

    private TableEnvironment tEnv;

    @BeforeEach
    void setUp() {
        tEnv = TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        tEnv.executeSql(
                "CREATE TABLE t (id BIGINT NOT NULL, k INT NOT NULL, val DOUBLE NOT NULL) WITH ("
                        + "'connector' = 'datagen', 'number-of-rows' = '2000000')");
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ACCELERATOR_ENABLED, true);
    }

    private String section(String sql) {
        String plan = tEnv.explainSql(sql);
        return plan.substring(plan.indexOf("== GPU Offload =="));
    }

    private static final String HEAVY =
            "EXP(val) * LN(val) + SIN(val) * COS(val) + POWER(val, 3.0)";

    @Test
    @DisplayName("an ungrouped SUM joins the Calc that feeds it, as one subtree")
    void sumJoinsTheCalc() {
        String s = section("SELECT SUM(h) FROM (SELECT " + HEAVY + " AS h FROM t)");
        assertTrue(s.contains("GPU  subtree"), s);
        assertTrue(
                s.contains("LocalHashAggregate"), "the aggregate should be in the subtree:\n" + s);
    }

    @Test
    @DisplayName("several SUMs too, which on a device is one contraction rather than several")
    void severalSums() {
        String s =
                section(
                        "SELECT SUM(h), SUM(h * 2.0E0), SUM(h + 1.0E0) FROM (SELECT "
                                + HEAVY
                                + " AS h FROM t)");
        assertTrue(s.contains("GPU  subtree"), s);
        assertTrue(s.contains("LocalHashAggregate"), s);
    }

    /**
     * Prints the sections, so what the planner grouped is inspectable rather than only asserted.
     */
    @Test
    @DisplayName("show the rendered sections")
    void showOutput() {
        for (String sql :
                new String[] {
                    "SELECT SUM(h) FROM (SELECT " + HEAVY + " AS h FROM t)",
                    "SELECT SUM(h), SUM(h * 2.0E0) FROM (SELECT " + HEAVY + " AS h FROM t)",
                    "SELECT k, SUM(h) FROM (SELECT k, " + HEAVY + " AS h FROM t) GROUP BY k",
                    "SELECT MIN(h) FROM (SELECT " + HEAVY + " AS h FROM t)",
                }) {
            System.out.println("---------- " + sql);
            System.out.println(section(sql));
        }
    }

    /**
     * A grouped aggregate is selected since M5.3.
     *
     * <p>It was refused while the only device route imagined for it was a sparse matrix-vector
     * product needing a grouping matrix nobody built. A bound library has the primitive instead,
     * and what reaches it is a key column and a value column -- so the reason the old assertion
     * pinned no longer exists. Whether it then runs on a device is settled on the TaskManager, as
     * it is for everything else.
     */
    @Test
    @DisplayName("GROUP BY joins the Calc that feeds it, as one subtree")
    void groupByJoinsTheCalc() {
        String s =
                section("SELECT k, SUM(h) FROM (SELECT k, " + HEAVY + " AS h FROM t) GROUP BY k");
        assertTrue(s.contains("GPU  subtree"), s);
        assertTrue(
                s.contains("LocalHashAggregate"), "the aggregate should be in the subtree:\n" + s);
        assertFalse(s.contains("grouping matrix"), s);
    }

    @Test
    @DisplayName("MIN has no contraction, and the reason names it")
    void minIsRefused() {
        String s = section("SELECT MIN(h) FROM (SELECT " + HEAVY + " AS h FROM t)");
        assertTrue(s.contains("no device contraction"), s);
    }

    @Test
    @DisplayName("an aggregate alone never clears the floor, which is why it must ride along")
    void aggregateAloneDoesAlmostNoWork() {
        String s = section("SELECT SUM(val) FROM t");
        // An ungrouped SUM is one accumulate per row and nothing else. It is eligible -- there is
        // no floor left to fall below -- but the work it reports is a single operation, which is
        // exactly what a provider needs in order to decline it.
        assertTrue(s.contains("PLUSx1"), s);
    }
}
