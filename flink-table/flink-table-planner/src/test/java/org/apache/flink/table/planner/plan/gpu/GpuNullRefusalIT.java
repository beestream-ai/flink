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
 * Exactly what the null refusal excludes, and what a user can do about it.
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

    private void assertRefusedForNullability(String label, String sql) {
        String section = offloadSection(sql);
        assertThat(section)
                .as("%s should be refused for nullability:%n%s", label, section)
                .contains("nullable operand");
    }

    @Test
    @DisplayName("a nullable column is refused, and the reason names the fix")
    void nullableColumnIsRefused() {
        String section = offloadSection("SELECT " + EXPR + " AS h FROM nullable");
        assertThat(section).contains("nullable operand");
        assertThat(section)
                .as("the message has to tell the user what to do, since nothing in SQL will")
                .contains("NOT NULL");
    }

    @Test
    @DisplayName("IS NOT NULL does not rescue it: Calcite keeps the column's declared type")
    void isNotNullPredicateDoesNotRescue() {
        assertRefusedForNullability(
                "IS NOT NULL", "SELECT " + EXPR + " AS h FROM nullable WHERE a IS NOT NULL");
    }

    @Test
    @DisplayName("nor does a predicate that implies non-null, such as a > 0")
    void impliedNonNullPredicateDoesNotRescue() {
        assertRefusedForNullability("a > 0", "SELECT " + EXPR + " AS h FROM nullable WHERE a > 0");
    }

    @Test
    @DisplayName("nor does pushing the filter into a subquery")
    void subqueryDoesNotRescue() {
        assertRefusedForNullability(
                "subquery",
                "SELECT " + EXPR + " AS h FROM (SELECT * FROM nullable WHERE a IS NOT NULL)");
    }

    @Test
    @DisplayName("COALESCE, the obvious workaround, is refused for a different reason entirely")
    void coalesceIsNotSupportedEither() {
        String section =
                offloadSection(
                        "SELECT " + EXPR.replace("a", "COALESCE(a, 1.0)") + " AS h FROM nullable");
        // Worth pinning separately from the nullability refusal: a user told to declare NOT NULL
        // who cannot change the DDL will reach for this, and it fails for want of IR rather than
        // for want of a null representation. Two different fixes.
        assertThat(section).contains("no accelerator IR for operator COALESCE");
    }

    @Test
    @DisplayName("only the DDL rescues it")
    void declaredNotNullIsEligible() {
        assertThat(offloadSection("SELECT " + EXPR + " AS h FROM declared"))
                .contains("GPU  subtree");
    }
}
