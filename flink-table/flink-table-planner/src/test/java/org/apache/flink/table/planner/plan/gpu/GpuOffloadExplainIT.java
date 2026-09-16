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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** EXPLAIN must be able to answer "why did my query not run on the GPU?". */
class GpuOffloadExplainIT {

    private static final String CHEAP =
            "SELECT id, val * 2.0 + 1.0 AS scaled FROM t WHERE val > 0.5";
    private static final String HEAVY =
            "SELECT id, EXP(val) * LN(val) + SIN(val) * COS(val) + POWER(val, 3.0) AS h "
                    + "FROM t WHERE val > 0.5";

    private TableEnvironment tEnv;

    @BeforeEach
    void setUp() {
        tEnv = TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        tEnv.executeSql(
                "CREATE TABLE t (id BIGINT NOT NULL, val DOUBLE NOT NULL) WITH ("
                        + "'connector' = 'datagen', 'number-of-rows' = '10')");
    }

    @Test
    @DisplayName("no section at all when the feature is off")
    void silentWhenDisabled() {
        String plan = tEnv.explainSql(HEAVY);
        assertFalse(
                plan.contains("GPU Offload"),
                "a feature that is off must not change EXPLAIN output:\n" + plan);
    }

    /** Prints both plans, so the human-facing output is inspectable rather than only asserted. */
    @Test
    @DisplayName("show the rendered section")
    void showOutput() {
        tEnv.getConfig().set(GpuOffloadOptions.ENABLED, true);
        for (String sql : new String[] {CHEAP, HEAVY}) {
            String plan = tEnv.explainSql(sql);
            System.out.println("---------- " + sql);
            System.out.println(plan.substring(plan.indexOf("== Optimized Execution Plan ==")));
        }
    }

    @Test
    @DisplayName("a cheap query reports its work, and says where the decision is made")
    void rejectionIsExplained() {
        tEnv.getConfig().set(GpuOffloadOptions.ENABLED, true);
        String plan = tEnv.explainSql(CHEAP);

        assertTrue(plan.contains("== GPU Offload =="), plan);
        // A cheap expression is eligible now: whether three operations pay depends on a device the
        // planner cannot see, so EXPLAIN reports the work and says where the decision happens.
        assertTrue(plan.contains("TIMESx1"), plan);
        assertTrue(plan.contains("TaskManager"), plan);
    }

    @Test
    @DisplayName("a selected node reports GPU even where the client has no device")
    void selectionIsReportedIndependentlyOfTheClient() {
        tEnv.getConfig().set(GpuOffloadOptions.ENABLED, true);
        String plan = tEnv.explainSql(HEAVY);

        assertTrue(plan.contains("== GPU Offload =="), plan);
        assertTrue(plan.contains("subtree 0"), plan);
        // flink-table-planner has no dependency on the GPU runtime, so nothing here can reach a
        // device -- and that is now irrelevant to the verdict. The section reports the gate's
        // decision, which is a property of the query; whether a device is present is a property of
        // whichever TaskManager gets the task, and is settled there. A plan that said CPU because
        // the *client* had no provider was answering for the wrong machine.
        assertTrue(plan.contains("GPU  subtree 0"), plan);
        assertTrue(plan.contains("eligible"), plan);
        assertFalse(plan.contains("no accelerator provider"), plan);
    }

    @Test
    @DisplayName("an inexpressible type is reported as such, not as merely too cheap")
    void inexpressibleIsExplained() {
        tEnv.getConfig().set(GpuOffloadOptions.ENABLED, true);
        tEnv.executeSql(
                "CREATE TABLE s (word STRING, n INT) WITH ("
                        + "'connector' = 'datagen', 'number-of-rows' = '10')");
        String plan = tEnv.explainSql("SELECT word, n + 1 AS m FROM s WHERE n > 0");

        assertTrue(plan.contains("not expressible on device"), plan);
    }

    @Test
    @DisplayName("a nullable column is eligible too, and costs a byte more to carry")
    void nullableColumnIsEligibleAndCostsMore() {
        tEnv.getConfig().set(GpuOffloadOptions.ENABLED, true);
        // Same heavy expression, same shape, same row count -- the only difference is the DDL.
        tEnv.executeSql(
                "CREATE TABLE nullable_t (id BIGINT, val DOUBLE) WITH ("
                        + "'connector' = 'datagen', 'number-of-rows' = '10')");

        // Refused until M2.9, with EXPLAIN naming NOT NULL as the fix. That reading was accurate
        // and the cost was nearly everything -- see M2.8. The planner now reports eligibility and
        // the provider decides whether it can represent the null.
        String nullable = tEnv.explainSql(HEAVY.replace("FROM t", "FROM nullable_t"));
        assertTrue(nullable.contains("GPU  subtree 0"), nullable);
        assertFalse(nullable.contains("nullable operand"), nullable);

        String declared = tEnv.explainSql(HEAVY);
        assertTrue(declared.contains("GPU  subtree 0"), declared);

        // NOT NULL remains the cheaper path, and EXPLAIN shows why: no validity to move. Two
        // columns here -- a BIGINT and a DOUBLE -- so 16 bytes of values and two of validity.
        assertTrue(nullable.contains("in=18B/row"), nullable);
        assertTrue(declared.contains("in=16B/row"), declared);
    }
}
