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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Where a nullable column is decided, now that the planner no longer decides it.
 *
 * <p>Written for M2.8, whose question was how much the refusal costs. Measured over Flink's own SQL
 * test DDLs — 1,344 tables, 2,495 numeric columns — <b>4.6% of numeric columns are declared {@code
 * NOT NULL}</b>, and 93% of tables declare none at all. An expression is refused if any operand is
 * nullable, so a two-operand expression over columns drawn at random from that corpus is
 * offloadable about 0.2% of the time.
 *
 * <p>This class pins the other half: what, if anything, rescues a nullable column. The answer today
 * is nothing except the DDL. Calcite does not narrow nullability through a filter, so a predicate
 * that makes a null impossible does not change the column's type and the expression is still
 * refused.
 *
 * <p>These are assertions about <em>current</em> behaviour, not about desirable behaviour. If one
 * starts failing it most likely means someone taught the estimator to use the query's predicates —
 * which is the cheap half of the fix M2.9 exists for. Re-run the corpus census before deciding what
 * is left to do; the script and the numbers are recorded in {@code TASKS.md} under M2.8.
 */
class GpuNullRefusalIT {

    /** Expensive enough that cost is never the reason for a refusal. */
    private static final String EXPR = "EXP(a) * LN(a) + SIN(a) * COS(a) + POWER(a, 3.0)";

    private TableEnvironment tEnv;

    @BeforeEach
    void setUp() {
        tEnv = TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        tEnv.executeSql(
                "CREATE TABLE nullable (a DOUBLE, b DOUBLE) WITH ("
                        + "'connector' = 'datagen', 'number-of-rows' = '10')");
        tEnv.executeSql(
                "CREATE TABLE declared (a DOUBLE NOT NULL, b DOUBLE NOT NULL) WITH ("
                        + "'connector' = 'datagen', 'number-of-rows' = '10')");
        tEnv.getConfig().set(GpuOffloadOptions.ENABLED, true);
    }

    private String offloadSection(String sql) {
        String plan = tEnv.explainSql(sql);
        return plan.substring(plan.indexOf("== GPU Offload =="));
    }

    private void assertEligible(String label, String sql) {
        String section = offloadSection(sql);
        assertThat(section)
                .as("%s should be eligible:%n%s", label, section)
                .contains("GPU  subtree");
        assertThat(section)
                .as("and no longer refused by the planner for nullability:%n%s", section)
                .doesNotContain("nullable operand");
    }

    @Test
    @DisplayName("a nullable column is eligible; whether it can be served is the provider's call")
    void nullableColumnIsEligible() {
        assertEligible("nullable column", "SELECT " + EXPR + " AS h FROM nullable");
    }

    @Test
    @DisplayName("and so is one behind an ordinary predicate")
    void nullableBehindPredicateIsEligible() {
        // Pinned because it used to be refused, and because it is the case that would change if
        // anyone implements M2.13: Calcite still types `a` nullable here, so the subtree reaching
        // the provider still says so. That is a missed optimisation now rather than a refusal.
        assertEligible("a > 0", "SELECT " + EXPR + " AS h FROM nullable WHERE a > 0");
    }

    /**
     * {@code IS NOT NULL} is offloadable, which closes the oddity M2.9 left behind.
     *
     * <p>Between M2.9 and M2.11 this was refused — not for nullability, which had already been
     * relaxed, but because {@code IS NOT NULL} had no {@code AccelFunction} at all. Writing the
     * predicate a user would reach for made their query <em>less</em> offloadable than omitting it.
     *
     * <p>It is trivial once validity exists and was impossible before: the kernel already reads a
     * bit saying whether each staged value is there, so {@code IS NOT NULL} is that bit and {@code
     * IS NULL} is its complement. Neither can itself be null, which is what makes them usable in a
     * {@code WHERE} clause over a nullable column.
     */
    @Test
    @DisplayName("IS NOT NULL is offloadable now that there is a validity bit for it to read")
    void isNotNullPredicateIsEligible() {
        assertEligible("IS NOT NULL", "SELECT " + EXPR + " AS h FROM nullable WHERE a IS NOT NULL");
    }

    @Test
    @DisplayName("and so is IS NULL")
    void isNullPredicateIsEligible() {
        assertEligible("IS NULL", "SELECT " + EXPR + " AS h FROM nullable WHERE a IS NULL");
    }

    @Test
    @DisplayName("COALESCE is still refused, and for its own reason: there is no IR for it")
    void coalesceIsStillUnsupported() {
        String section =
                offloadSection(
                        "SELECT " + EXPR.replace("a", "COALESCE(a, 1.0)") + " AS h FROM nullable");
        // Worth keeping separate from nullability. A user told to declare NOT NULL reaches for
        // this, and it fails for want of IR rather than for want of a null representation -- so
        // supporting nulls does not fix it, and it was never the cheap independent workaround it
        // looked like.
        assertThat(section).contains("no accelerator IR for operator COALESCE");
    }

    @Test
    @DisplayName("a declared NOT NULL column is eligible too, and is still the cheaper path")
    void declaredNotNullIsEligible() {
        assertEligible("NOT NULL", "SELECT " + EXPR + " AS h FROM declared");
    }

    @Test
    @DisplayName("the work profile charges for the validity a nullable column has to carry")
    void nullableColumnCostsMoreToTransfer() {
        // One DOUBLE column either way, so the only difference is the validity byte. A provider
        // weighs bytes against operations, and a nullable column genuinely costs more to move.
        assertThat(offloadSection("SELECT " + EXPR + " AS h FROM nullable")).contains("in=9B/row");
        assertThat(offloadSection("SELECT " + EXPR + " AS h FROM declared")).contains("in=8B/row");
    }
}
