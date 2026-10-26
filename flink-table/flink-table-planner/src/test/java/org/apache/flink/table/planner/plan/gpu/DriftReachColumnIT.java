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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Drift is tracked per column, and the column is what decides whether a query may offload.
 *
 * <h2>The shape this exists for</h2>
 *
 * <p>{@code SELECT k, SUM(f(x)) ... GROUP BY k} is the canonical analytical query, and until this
 * analysis became column-aware every instance of it was refused. The reasoning was sound but too
 * coarse: a grouped aggregate does decide which rows come back, so a node-granular walk sees a
 * drifting value reaching a row-deciding consumer and stops. What it could not see is that the
 * groups come from {@code k} alone — {@code k} is a column of the scan, nothing rounds it, and a
 * value that feeds only {@code SUM} cannot move a row into a different group. It changes what a
 * group sums to, and that is a value.
 *
 * <p>The refusal was not academic. It is the reason the projection in that query ran on the CPU
 * while the aggregate above it ran on a device, which is the worst of both: the expensive half
 * stayed where it was and the cheap half paid a round trip to move.
 *
 * <h2>Why the refusing tests are the important ones</h2>
 *
 * <p>Making the analysis finer can only make it accept more, so the risk is entirely on the accept
 * side. The three refusal cases below are the boundary: the same query with the drifting value
 * moved into the grouping key, into a {@code WHERE}, and into an {@code ORDER BY}. Each must still
 * be refused, and each would be silently accepted by an off-by-one in the column mapping.
 */
class DriftReachColumnIT {

    /** A transcendental, so the value is one a device is not required to round as Flink does. */
    private static final String DRIFTS = "SIN(val) * COS(val) + EXP(val)";

    private TableEnvironment tEnv;

    @BeforeEach
    void setUp() {
        tEnv = TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        tEnv.executeSql(
                "CREATE TABLE t (k INT NOT NULL, val DOUBLE NOT NULL) WITH ("
                        + "'connector' = 'datagen', 'number-of-rows' = '10')");
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ACCELERATOR_ENABLED, true);
        tEnv.getConfig()
                .set(ExecutionConfigOptions.TABLE_EXEC_ACCELERATOR_APPROXIMATE_PROJECTIONS, true);
    }

    private String explain(String sql) {
        return tEnv.explainSql(sql);
    }

    /** The refusal wording the processor emits when drift reaches a row-deciding consumer. */
    private static final String REFUSED = "reaches a consumer that decides which rows";

    @Test
    @DisplayName("a drifting value under SUM offloads, because the grouping key does not drift")
    void driftUnderAnAggregateCallIsOffered() {
        String plan = explain("SELECT k, SUM(" + DRIFTS + ") AS s FROM t GROUP BY k");

        assertThat(plan)
                .withFailMessage(
                        "the projection feeds only SUM, so no row can land in a different group:\n"
                                + plan)
                .doesNotContain(REFUSED);
        assertThat(plan).contains("== GPU Offload ==").contains("GPU  subtree 0");
    }

    @Test
    @DisplayName("the drift survives a two-phase aggregate, an exchange and a projection above it")
    void driftIsTrackedAcrossTheWholeChain() {
        // Local aggregate, hash exchange, global aggregate, a projection, then a second aggregate.
        // Every one of those re-indexes the row, and before the mapping existed the walk gave up
        // at the first of them and refused everything downstream of it.
        String plan =
                explain(
                        "SELECT COUNT(*) AS n, SUM(s) AS total FROM ("
                                + "SELECT k, SUM("
                                + DRIFTS
                                + ") AS s FROM t GROUP BY k)");

        assertThat(plan)
                .withFailMessage("nothing in this query groups, filters or orders by s:\n" + plan)
                .doesNotContain(REFUSED);
    }

    @Test
    @DisplayName("a drifting value used AS the grouping key is still refused")
    void driftInTheGroupingKeyIsRefused() {
        // The same query with the drift moved from the aggregate call into GROUP BY. Now it does
        // decide membership: two values that differ in the last bit are two groups, not one.
        String plan =
                explain(
                        "SELECT d, COUNT(*) AS n FROM (SELECT "
                                + DRIFTS
                                + " AS d FROM t) GROUP BY d");

        assertThat(plan)
                .withFailMessage("a drifting grouping key changes which rows come back:\n" + plan)
                .contains(REFUSED);
    }

    @Test
    @DisplayName("a drifting value in a WHERE above the projection is still refused")
    void driftReachingAFilterIsRefused() {
        String plan =
                explain(
                        "SELECT k, SUM(d) AS s FROM (SELECT k, "
                                + DRIFTS
                                + " AS d FROM t) WHERE d > 0.5 GROUP BY k");

        assertThat(plan)
                .withFailMessage(
                        "a predicate over a drifting value selects different rows:\n" + plan)
                .contains(REFUSED);
    }

    @Test
    @DisplayName("a drifting value in an ORDER BY is still refused")
    void driftReachingASortIsRefused() {
        String plan =
                explain(
                        "SELECT k, s FROM (SELECT k, SUM("
                                + DRIFTS
                                + ") AS s FROM t GROUP BY k) ORDER BY s");

        assertThat(plan)
                .withFailMessage("a sort key that drifts changes the order of the answer:\n" + plan)
                .contains(REFUSED);
    }

    @Test
    @DisplayName("an ORDER BY on a column that does not drift is offered")
    void sortingOnACleanKeyIsOffered() {
        // The counterpart of the test above, and the reason this distinction is worth drawing: a
        // sort reorders rows, it does not invent or remove them, so which rows come back is
        // unchanged unless the ORDER BY itself reads a drifting column. Refusing every downstream
        // sort would refuse every query that sorts its own output -- which is most of them.
        String plan =
                explain(
                        "SELECT k, s FROM (SELECT k, SUM("
                                + DRIFTS
                                + ") AS s FROM t GROUP BY k) ORDER BY k");

        assertThat(plan)
                .withFailMessage("k is a scan column; nothing rounds it:\n" + plan)
                .doesNotContain(REFUSED);
        assertThat(plan).contains("== GPU Offload ==").contains("GPU  subtree 0");
    }

    @Test
    @DisplayName("a LIMIT the optimiser pushed below the projection does not refuse it")
    void aLimitBelowTheProjectionIsHarmless() {
        // Worth pinning because the obvious reading is the opposite. A LIMIT over a drifting value
        // would decide which rows come back -- but Calcite does not leave it there: a projection
        // does not change row count, so the limit sinks below it and the plan becomes
        // Calc <- Limit <- Scan. By the time the drifting value exists the rows are already
        // chosen, and the analysis is right to accept.
        //
        // The dangerous shape is ORDER BY over a drifting column, which cannot be pushed down and
        // is refused -- see driftReachingASortIsRefused.
        String plan = explain("SELECT d FROM (SELECT " + DRIFTS + " AS d FROM t) LIMIT 5");

        assertThat(plan).contains("Limit");
        assertThat(plan)
                .withFailMessage(
                        "the limit runs before the projection, so nothing drifts:\n" + plan)
                .doesNotContain(REFUSED);
    }
}
