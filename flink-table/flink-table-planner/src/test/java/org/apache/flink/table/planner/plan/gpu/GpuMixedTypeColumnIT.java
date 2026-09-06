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

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Which column types actually reach a kernel.
 *
 * <p>The thirteen-shape probe this project's coverage claim comes from declares one numeric column,
 * {@code val DOUBLE}, so every shape it reports as offloaded reads a double. That left the mixed
 * cases below unprobed, and they were the ones that staged a four-byte column through {@code
 * getDouble} and read whatever followed it.
 *
 * <p>These assert reachability, not results: a plan saying {@code GPU} is what makes the staging
 * path live. Whether the values that come back are right is a device question, and the gather half
 * of it is pinned by {@code RowGatherTypeTest}.
 */
class GpuMixedTypeColumnIT {

    private TableEnvironment tEnv;

    @BeforeEach
    void setUp() {
        tEnv = TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        tEnv.executeSql(
                "CREATE TABLE mixed (id BIGINT, i INT, f FLOAT, d DOUBLE) WITH ("
                        + "'connector' = 'datagen', 'number-of-rows' = '2000000')");
        tEnv.getConfig().set(GpuOffloadOptions.ENABLED, true);
    }

    private void assertOffloads(String label, String sql) {
        String plan = tEnv.explainSql(sql);
        String section = plan.substring(plan.indexOf("== GPU Offload =="));
        assertTrue(
                section.contains("GPU  subtree"),
                label + " should offload but the plan says:\n" + section);
    }

    @Test
    @DisplayName("a DOUBLE column reaches a kernel -- the case the shape probe covered")
    void doubleColumn() {
        assertOffloads(
                "double",
                "SELECT id, EXP(d) * LN(d) + SIN(d) * COS(d) + POWER(d, 3.0) AS h FROM mixed");
    }

    @Test
    @DisplayName("an INT column reaches a kernel whenever the result is not itself an INT")
    void integerColumnWithADoubleResult() {
        assertOffloads(
                "int",
                "SELECT id, EXP(i) * LN(i) + SIN(i) * COS(i) + POWER(i, 3.0) AS h FROM mixed");
    }

    @Test
    @DisplayName("a FLOAT column reaches a kernel")
    void floatColumn() {
        assertOffloads(
                "float",
                "SELECT id, EXP(f) * LN(f) + SIN(f) * COS(f) + POWER(f, 3.0) AS h FROM mixed");
    }

    @Test
    @DisplayName("and all three at once, which is the shape that mixes buffer widths")
    void allThreeWidths() {
        assertOffloads(
                "mixed",
                "SELECT id, EXP(i) * LN(f) + SIN(d) * COS(i) + POWER(f, 3.0) AS h FROM mixed");
    }

    @Test
    @DisplayName("a FLOAT column can also be projected through beside a computed one")
    void floatPassesThrough() {
        assertOffloads(
                "float pass-through",
                "SELECT f, EXP(d) * LN(d) + SIN(d) * COS(d) + POWER(d, 3.0) AS h FROM mixed");
    }

    /**
     * The staging path can now write a {@code FLOAT} result, and nothing reaches it.
     *
     * <p>Two independent things stand in the way, and only one of them was the buffer. The cost
     * estimator refuses a narrowing cast outright, and a {@code FLOAT} result arises otherwise only
     * from {@code FLOAT}-only arithmetic, which is a weight of one per operator and does not clear
     * a floor of 96 in any expression anyone would write.
     *
     * <p>Asserted as a refusal rather than deleted, so that relaxing the cast rule is a deliberate
     * act: what {@code CAST(&hellip; AS FLOAT)} should do when the value overflows a float is a
     * semantic question this change did not answer.
     */
    @Test
    @DisplayName("a FLOAT result is still refused, by the estimator rather than by staging")
    void floatResultIsRefusedByTheEstimator() {
        String plan =
                tEnv.explainSql(
                        "SELECT id, CAST(EXP(d) * LN(d) + SIN(d) * COS(d) + POWER(d, 3.0) AS FLOAT)"
                                + " AS h FROM mixed");
        assertTrue(
                plan.contains("unsupported cast to FLOAT"),
                "expected the estimator to give the reason:\n" + plan);
    }

    /**
     * Prints the sections, so what the planner decided is inspectable rather than only asserted.
     */
    @Test
    @DisplayName("show the rendered sections")
    void showOutput() {
        for (String sql :
                new String[] {
                    "SELECT id, EXP(d) * LN(d) + SIN(d) * COS(d) + POWER(d, 3.0) AS h FROM mixed",
                    "SELECT id, EXP(i) * LN(i) + SIN(i) * COS(i) + POWER(i, 3.0) AS h FROM mixed",
                    "SELECT id, EXP(f) * LN(f) + SIN(f) * COS(f) + POWER(f, 3.0) AS h FROM mixed",
                    "SELECT id, i * 2 AS n FROM mixed",
                    "SELECT id, f * f AS sq FROM mixed",
                    "SELECT f, EXP(d) * LN(d) + SIN(d) * COS(d) + POWER(d, 3.0) AS h FROM mixed",
                    "SELECT id, CAST(EXP(d) * LN(d) + SIN(d) * COS(d) + POWER(d, 3.0) AS FLOAT) AS h FROM mixed",
                }) {
            String plan = tEnv.explainSql(sql);
            System.out.println("---------- " + sql);
            System.out.println(plan.substring(plan.indexOf("== GPU Offload ==")));
        }
    }
}
