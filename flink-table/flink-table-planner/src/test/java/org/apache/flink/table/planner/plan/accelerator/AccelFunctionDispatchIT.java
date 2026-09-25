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
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.functions.ScalarFunction;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Every function the IR claims to know is still reachable from SQL, and nothing else is.
 *
 * <p>This exists because of how the previous bug failed. Dispatch was on the operator's name, and
 * the guard that made a name safe tested {@code getClass().getName().startsWith(
 * "org.apache.calcite.sql.fun.")} — which is true of most of Calcite's operator table and false of
 * {@code SqlStdOperatorTable.SQRT}, a plain {@code SqlFunction} in the parent package. Every
 * transcendental silently stopped being offloadable. Nothing failed; the feature just quietly did
 * less, and no test noticed because no test asked "is SQRT still reachable?".
 *
 * <p>M2.4 replaced the name with the operator's identity, which removes that class of bug and
 * introduces its own: an entry keyed on the wrong object matches nothing, with exactly the same
 * silence. Which table each operator comes from is genuinely not guessable — most are Calcite's,
 * {@code LEAST} and {@code GREATEST} are Flink function definitions behind a bridging operator, and
 * {@code LOG2} and {@code TANH} are in Flink's Calcite-side table and in neither of the other two.
 *
 * <p>So this walks the list from the outside, through real SQL, and asserts each one is
 * offloadable. An entry that stops resolving fails here rather than going quiet.
 */
class AccelFunctionDispatchIT {

    private TableEnvironment tEnv;

    @BeforeEach
    void setUp() {
        tEnv = TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        tEnv.executeSql(
                "CREATE TABLE t (a DOUBLE NOT NULL, b DOUBLE NOT NULL) WITH ("
                        + "'connector' = 'datagen', 'number-of-rows' = '10')");
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ACCELERATOR_ENABLED, true);
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ACCELERATOR_APPROXIMATE_PROJECTIONS, true);
    }

    /**
     * One call per mapped function, wrapped in enough arithmetic to be worth costing.
     *
     * <p>The wrapping matters: a bare {@code SELECT SIN(a)} is one operation over sixteen bytes and
     * the planner may reasonably say so. What is under test is whether the operator resolves, not
     * whether one call is worth moving, so each is put inside an expression that plainly is.
     */
    @ParameterizedTest(name = "{0} is offloadable")
    @ValueSource(
            strings = {
                "ABS(a)",
                "SQRT(a)",
                "EXP(a)",
                "LN(a)",
                "LOG2(a)",
                "POWER(a, 2.0)",
                "SIN(a)",
                "COS(a)",
                "TAN(a)",
                "ASIN(a)",
                "ACOS(a)",
                "ATAN(a)",
                "ATAN2(a, b)",
                "TANH(a)",
                "SIGN(a)",
                "FLOOR(a)",
                "CEIL(a)",
                "LEAST(a, b)",
                "GREATEST(a, b)",
                "a + b",
                "a - b",
                "a * b",
                "a / b",
                "-a"
            })
    void functionIsOffloadable(String expression) {
        String plan =
                tEnv.explainSql(
                        "SELECT EXP(a) * LN(a) + SIN(a) * COS(a) + ("
                                + expression
                                + ") AS h"
                                + " FROM t WHERE a > 1.0");
        String section = plan.substring(plan.indexOf("== GPU Offload =="));
        assertThat(section)
                .as(
                        "%s stopped resolving, so it is silently no longer offloadable:%n%s",
                        expression, section)
                .contains("GPU  subtree");
    }

    /**
     * A user function that impersonates a built-in is a different object, so it maps to nothing.
     */
    public static class Sqrt extends ScalarFunction {
        public double eval(double v) {
            return v; // deliberately not a square root
        }
    }

    @Test
    @DisplayName("a catalog function registered as SQRT is refused, not run as the builtin")
    void catalogFunctionImpersonatingABuiltinIsRefused() {
        // Registered under the built-in's own name, which is the whole point: the name is
        // identical and the semantics are not. Under the old name-keyed dispatch this is the
        // query that returned a square root the user never asked for -- review finding A3.
        tEnv.createTemporarySystemFunction("SQRT", Sqrt.class);

        String plan =
                tEnv.explainSql(
                        "SELECT EXP(a) * LN(a) + SIN(a) * COS(a) + SQRT(a) AS h"
                                + " FROM t WHERE a > 1.0");
        String section = plan.substring(plan.indexOf("== GPU Offload =="));

        assertThat(section)
                .as(
                        "a user function must not be offloaded as the builtin it is named after:%n%s",
                        section)
                .contains("CPU");
        assertThat(section)
                .as(
                        "and the reason should name the operator, not something incidental:%n%s",
                        section)
                .contains("SQRT");
    }
}
