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
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * M5.3: a grouped aggregate reaches an accelerator, and answers what the CPU plan answers.
 *
 * <p>The node under test is Flink's <em>local</em> aggregate, fused with the projection feeding it.
 * Being local is what makes it workable: it is partial by definition, so a provider may aggregate a
 * batch at a time and emit as it goes, and the merge half above the shuffle combines whatever
 * arrives. Nothing here carries state between batches.
 *
 * <p>Runs against {@link TestAcceleratorProvider}, which interprets the IR on the host, so it
 * asserts the planner half of M5.3 on a machine with no device. Whether a real device computes the
 * same thing is the provider repository's business.
 */
class AcceleratedGroupedAggregateIT {

    private static final String HEAVY =
            "EXP(val) * LN(val) + SIN(val) * COS(val) + POWER(val, 3.0)";

    /** Keyed on a column, summed over a computed expression -- the shape cuDF would serve. */
    private static final String QUERY =
            "SELECT k, SUM(h) AS total FROM (SELECT k, " + HEAVY + " AS h FROM t) GROUP BY k";

    private TableEnvironment tEnv;

    @BeforeEach
    void setUp() {
        tEnv = TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        tEnv.executeSql(
                // Sequences, so two runs of the same query group the same rows.
                "CREATE TABLE t (k INT NOT NULL, val DOUBLE NOT NULL) WITH ("
                        + "'connector' = 'datagen', 'number-of-rows' = '400',"
                        + "'fields.k.kind' = 'sequence',"
                        + "'fields.k.start' = '1', 'fields.k.end' = '8',"
                        + "'fields.val.kind' = 'sequence',"
                        + "'fields.val.start' = '1', 'fields.val.end' = '400')");
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ACCELERATOR_ENABLED, true);
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ACCELERATOR_APPROXIMATE_PROJECTIONS, true);
        // The device route for a grouped aggregate lives on the fused node, so fusion is what
        // reaches it. M4.5 turned the default off because the fused pair had nowhere to run;
        // this is the case that gives it somewhere.
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ACCELERATOR_FUSE_AGGREGATE, true);
    }

    @AfterEach
    void tearDown() {
        TestAcceleratorProvider.reset();
    }

    private List<Row> run(String sql) {
        List<Row> rows = new ArrayList<>();
        TableResult result = tEnv.executeSql(sql);
        try (CloseableIterator<Row> it = result.collect()) {
            it.forEachRemaining(rows::add);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
        return rows;
    }

    @Test
    @DisplayName("the grouped aggregate is offered to a provider, and the provider runs it")
    void theGroupedAggregateReachesTheAccelerator() {
        TestAcceleratorProvider.enable(10.0);

        List<Row> rows = run(QUERY);

        assertThat(TestAcceleratorProvider.PLANS_OFFERED.get())
                .as("a grouped aggregate must be offered; before M5.3 the IR refused to hold one")
                .isPositive();
        assertThat(TestAcceleratorProvider.LAST_WAS_AGGREGATE.get())
                .as("the subtree offered must be the aggregate, not the projection under it")
                .isTrue();
        assertThat(TestAcceleratorProvider.ROWS_PROCESSED.get())
                .as("rows must have gone through the accelerated operator, not around it")
                .isPositive();
        assertThat(rows).as("one row per key").hasSize(8);
    }

    @Test
    @DisplayName("and answers exactly what the CPU plan answers")
    void resultsMatchTheCpuPlan() {
        TestAcceleratorProvider.enable(10.0);
        List<Row> accelerated = run(QUERY);
        int seen = TestAcceleratorProvider.ROWS_PROCESSED.get();

        TestAcceleratorProvider.reset();
        List<Row> onCpu = run(QUERY);

        assertThat(seen).as("the first run must have been accelerated").isPositive();
        assertThat(TestAcceleratorProvider.ROWS_PROCESSED.get())
                .as("the second run must not have touched the provider")
                .isEqualTo(seen);
        assertThat(accelerated).containsExactlyInAnyOrderElementsOf(onCpu);
    }

    /**
     * COUNT(*) costs nothing on a device: the operator already knows how many rows it staged.
     *
     * <p>Kept separate from SUM because the IR gives it no input field, and a provider that assumed
     * every call reads one would fail only here.
     */
    @Test
    @DisplayName("COUNT(*) beside a SUM is grouped the same way")
    void countStarIsOffloadedToo() {
        TestAcceleratorProvider.enable(10.0);
        String sql =
                "SELECT k, SUM(h) AS total, COUNT(*) AS n FROM (SELECT k, "
                        + HEAVY
                        + " AS h FROM t) GROUP BY k";

        List<Row> accelerated = run(sql);
        int seen = TestAcceleratorProvider.ROWS_PROCESSED.get();

        TestAcceleratorProvider.reset();
        List<Row> onCpu = run(sql);

        assertThat(seen).isPositive();
        assertThat(accelerated).containsExactlyInAnyOrderElementsOf(onCpu);
    }

    @Test
    @DisplayName("a compiled plan does not offload a grouped aggregate either")
    void compiledPlansAreNotOffloaded() {
        TestAcceleratorProvider.enable(10.0);
        tEnv.executeSql("CREATE TABLE snk (k INT, total DOUBLE) WITH ('connector' = 'blackhole')");

        String json = tEnv.compilePlanSql("INSERT INTO snk " + QUERY).asJsonString();

        assertThat(json).doesNotContain("AcceleratedGroup");
        assertThat(TestAcceleratorProvider.PLANS_OFFERED.get()).isZero();
    }
}
