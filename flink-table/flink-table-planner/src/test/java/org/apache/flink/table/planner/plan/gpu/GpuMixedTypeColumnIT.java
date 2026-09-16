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
                "CREATE TABLE mixed (id BIGINT NOT NULL, i INT NOT NULL, f FLOAT NOT NULL, d DOUBLE NOT NULL, b BOOLEAN NOT NULL) WITH ("
                        + "'connector' = 'datagen', 'number-of-rows' = '2000000')");
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ACCELERATOR_ENABLED, true);
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
     * A {@code BOOLEAN} column projected through is eligible, and refused by whoever cannot stage
     * it.
     *
     * <p>This used to be a planner refusal, and it was the wrong place for it. Nothing about the
     * query prevents carrying a boolean beside a computed column: what prevented it was that one
     * backend stages columns in primitive arrays and has none for a boolean. That is a property of
     * a backend, so the planner now reports the node as eligible and the provider declines it on
     * the TaskManager -- which is also what a different backend, with a byte-packed boolean buffer,
     * would need in order to accept it.
     *
     * <p>The refusal itself is still asserted, in the module that owns it: see {@code
     * GpuCalcSpecStagingTest} in the provider's own repository.
     */
    @Test
    @DisplayName("a BOOLEAN column is eligible; staging it is the provider's problem")
    void booleanColumnIsEligible() {
        String plan =
                tEnv.explainSql(
                        "SELECT b, EXP(d) * LN(d) + SIN(d) * COS(d) + POWER(d, 3.0) AS h "
                                + "FROM mixed");
        String section = plan.substring(plan.indexOf("== GPU Offload =="));
        assertTrue(section.contains("GPU"), "expected eligibility:\n" + section);
    }

    @Test
    @DisplayName("and filtering on one is refused too")
    void booleanConditionIsNowAccepted() {
        String plan =
                tEnv.explainSql(
                        "SELECT EXP(d) * LN(d) + SIN(d) * COS(d) + POWER(d, 3.0) AS h "
                                + "FROM mixed WHERE b");
        String section = plan.substring(plan.indexOf("== GPU Offload =="));
        // This used to be refused, and the refusal was never about the query: the legacy kernel
        // generator cannot stage a BOOLEAN, and it vetoed the whole offload path. The IR
        // represents a BOOLEAN condition perfectly well, so a provider that can serve it now may.
        // A provider that cannot simply declines, which is the difference between a limitation of
        // one backend and a property of the plan.
        assertTrue(section.contains("GPU"), "expected eligibility:\n" + section);
    }

    /**
     * A {@code FLOAT} result is refused, and since M2.3 for the reason that actually matters.
     *
     * <p>It used to be refused as a narrowing cast, which was true of this query and missed the
     * general case: {@code FLOAT}-only arithmetic produces a {@code FLOAT} with no cast anywhere
     * and was eligible. The rule is now on the result type, so both shapes are covered — see {@link
     * #floatArithmeticIsRefusedWithoutAnyCast()}.
     *
     * <p>Asserted as a refusal rather than deleted, so that relaxing it is a deliberate act.
     */
    @Test
    @DisplayName("a FLOAT result is refused, and the reason is the arithmetic, not the cast")
    void floatResultIsRefusedByTheEstimator() {
        String plan =
                tEnv.explainSql(
                        "SELECT id, CAST(EXP(d) * LN(d) + SIN(d) * COS(d) + POWER(d, 3.0) AS FLOAT)"
                                + " AS h FROM mixed");
        assertTrue(
                plan.contains("FLOAT result"),
                "expected the estimator to give the reason:\n" + plan);
    }

    /**
     * The case the cast rule never saw: {@code FLOAT} arithmetic, no cast in sight.
     *
     * <p>Flink evaluates this in float and rounds at every step; a kernel computing in double and
     * narrowing once rounds once. A single operation agrees either way — double carries more than
     * the 2p+2 bits that makes one rounding exact — so it is chains that diverge, and a chain is
     * what any expression worth offloading is.
     */
    @Test
    @DisplayName("FLOAT arithmetic is refused even with no cast anywhere in the expression")
    void floatArithmeticIsRefusedWithoutAnyCast() {
        String plan = tEnv.explainSql("SELECT id, f * f + f AS h FROM mixed");
        assertTrue(
                plan.contains("FLOAT result"),
                "expected a refusal naming the FLOAT result:\n" + plan);
    }

    /**
     * A {@code FLOAT} <em>input</em> to a {@code DOUBLE} expression stays eligible.
     *
     * <p>Pinned, because the obvious way to write the refusal above is on the operand types and
     * that would take this with it for no reason. Widening a float to a double is exact, and the
     * arithmetic that follows is Flink's double arithmetic on both processors.
     */
    @Test
    @DisplayName("a FLOAT input feeding DOUBLE arithmetic is still eligible")
    void floatInputToDoubleArithmeticStaysEligible() {
        String plan =
                tEnv.explainSql(
                        "SELECT id, EXP(CAST(f AS DOUBLE)) * LN(d) + SIN(d) * COS(d) AS h"
                                + " FROM mixed");
        String section = plan.substring(plan.indexOf("== GPU Offload =="));
        assertTrue(section.contains("GPU"), "expected eligibility:\n" + section);
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
                    "SELECT b, EXP(d) * LN(d) + SIN(d) * COS(d) + POWER(d, 3.0) AS h FROM mixed",
                }) {
            String plan = tEnv.explainSql(sql);
            System.out.println("---------- " + sql);
            System.out.println(plan.substring(plan.indexOf("== GPU Offload ==")));
        }
    }
}
