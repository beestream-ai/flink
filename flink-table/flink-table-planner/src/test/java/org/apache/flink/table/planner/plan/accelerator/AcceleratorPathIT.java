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
import org.apache.flink.table.planner.plan.gpu.GpuOffloadOptions;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.runtime.gpu.GpuOrCpuCalcOperatorFactory.ACCELERATED;
import static org.apache.flink.table.runtime.gpu.GpuOrCpuCalcOperatorFactory.DECISION;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The first tests in this repository that exercise the accelerator path rather than its fallback.
 *
 * <p>Every other integration test here takes the CPU branch, because the planner module depends on
 * no provider and {@code AcceleratorProviders.find} therefore returns empty — so the suite proved
 * only that offload degrades cleanly, never that it runs. Review finding E2.
 *
 * <p>{@link TestAcceleratorProvider} closes that without hardware: it accepts the IR and evaluates
 * it on the host. What it proves is the wiring and the IR, not a device.
 */
class AcceleratorPathIT {

    private static final String QUERY =
            "SELECT id, EXP(val) * LN(val) + SIN(val) * COS(val) AS h FROM t WHERE val > 10.0";

    /** The same shape, but the column carried past the kernel is nullable and always null. */
    private static final String NULLABLE_QUERY =
            "SELECT tag, EXP(val) * LN(val) + SIN(val) * COS(val) AS h "
                    + "FROM t_nullable WHERE val > 10.0";

    /**
     * A FLOAT expression chosen so float and double arithmetic disagree in the first figure, not
     * the fourth.
     *
     * <p>{@code (f + 1e7) - 1e7} is catastrophic cancellation: a float carries 24 bits, so adding
     * 1e7 to a small value discards it entirely and subtracting 1e7 back returns a coarse multiple
     * of the exponent step. In double the same expression returns the value. If the device were
     * ever to compute this in double and narrow at the end, the disagreement would be impossible to
     * miss — which is the point of choosing it.
     */
    private static final String FLOAT_QUERY =
            "SELECT id, (fval + CAST(10000000.0 AS FLOAT)) - CAST(10000000.0 AS FLOAT) AS h "
                    + "FROM t_float";

    private TableEnvironment tEnv;

    @BeforeEach
    void setUp() {
        AccelInterpreterOperator.PUBLISHED.clear();
        tEnv = TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        tEnv.executeSql(
                // Sequences, not random values: two runs of the same query must produce the same
                // rows, or comparing the accelerated and CPU results compares two datasets.
                "CREATE TABLE t (id BIGINT NOT NULL, val DOUBLE NOT NULL) WITH ("
                        + "'connector' = 'datagen', 'number-of-rows' = '50',"
                        + "'fields.id.kind' = 'sequence',"
                        + "'fields.id.start' = '1', 'fields.id.end' = '50',"
                        + "'fields.val.kind' = 'sequence',"
                        + "'fields.val.start' = '1', 'fields.val.end' = '50')");
        tEnv.executeSql(
                // The same, with a nullable pass-through column that is null in every row.
                //
                // null-rate 1.0 rather than something in between, because the comparison below is
                // between two separate jobs over a regenerated source: at any other rate the nulls
                // land in different places each run and the two result sets differ for a reason
                // that has nothing to do with offload. At 1.0 the column is deterministic.
                "CREATE TABLE t_nullable ("
                        + "id BIGINT NOT NULL, tag BIGINT, val DOUBLE NOT NULL) WITH ("
                        + "'connector' = 'datagen', 'number-of-rows' = '50',"
                        + "'fields.id.kind' = 'sequence',"
                        + "'fields.id.start' = '1', 'fields.id.end' = '50',"
                        + "'fields.tag.null-rate' = '1.0',"
                        + "'fields.val.kind' = 'sequence',"
                        + "'fields.val.start' = '1', 'fields.val.end' = '50')");
        tEnv.executeSql(
                "CREATE TABLE t_float (id BIGINT NOT NULL, fval FLOAT NOT NULL) WITH ("
                        + "'connector' = 'datagen', 'number-of-rows' = '50',"
                        + "'fields.id.kind' = 'sequence',"
                        + "'fields.id.start' = '1', 'fields.id.end' = '50',"
                        + "'fields.fval.kind' = 'sequence',"
                        + "'fields.fval.start' = '1', 'fields.fval.end' = '50')");
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
    @DisplayName("the accelerator path actually runs, and the provider sees the rows")
    void acceleratorPathRuns() {
        tEnv.getConfig().set(GpuOffloadOptions.ENABLED, true);
        TestAcceleratorProvider.enable(10.0);

        List<Row> rows = run(QUERY);

        assertThat(TestAcceleratorProvider.PLANS_OFFERED.get())
                .as("the provider must have been asked and must have accepted")
                .isPositive();
        assertThat(TestAcceleratorProvider.ROWS_PROCESSED.get())
                .as("rows must have gone through the accelerator operator, not around it")
                .isPositive();
        assertThat(rows).isNotEmpty();
    }

    @Test
    @DisplayName("with no provider willing, the CPU runs and the results are identical")
    void resultsMatchTheCpuPath() {
        tEnv.getConfig().set(GpuOffloadOptions.ENABLED, true);
        TestAcceleratorProvider.enable(10.0);
        List<Row> accelerated = run(QUERY);
        int seen = TestAcceleratorProvider.ROWS_PROCESSED.get();

        TestAcceleratorProvider.reset();
        List<Row> onCpu = run(QUERY);

        assertThat(seen).isPositive();
        assertThat(TestAcceleratorProvider.ROWS_PROCESSED.get())
                .as("the second run must not have touched the provider")
                .isEqualTo(seen);
        assertThat(accelerated).containsExactlyInAnyOrderElementsOf(onCpu);
    }

    /**
     * A null in a pass-through column stays a null.
     *
     * <p>Pass-through columns never reach a device: they are staged host-side and re-emitted, which
     * is both cheaper and the only way to carry a type a kernel cannot represent. That staging is
     * where a null goes missing, because the natural way to write it is a primitive array, and a
     * {@code long[]} has no null — it has a zero. Nothing about the query says so, and the result
     * is a row that silently reads 0 where the CPU plan says NULL.
     *
     * <p>Distinct from M2.2, which refuses nullable operands of a <em>call</em>. That refusal is
     * correct and stays: a kernel has no null to compute with. A column merely carried past the
     * kernel needs no null semantics at all, only somewhere to record that it was one.
     */
    @Test
    @DisplayName("a null in a pass-through column survives the accelerator as a null, not a zero")
    void nullPassThroughSurvives() {
        tEnv.getConfig().set(GpuOffloadOptions.ENABLED, true);
        TestAcceleratorProvider.enable(4.0);

        List<Row> accelerated = run(NULLABLE_QUERY);

        assertThat(TestAcceleratorProvider.ROWS_PROCESSED.get())
                .as("the accelerator must have run, or this asserts nothing")
                .isPositive();
        assertThat(accelerated).isNotEmpty();
        assertThat(accelerated)
                .allSatisfy(
                        row ->
                                assertThat(row.getField(0))
                                        .as("pass-through column must be null, not 0")
                                        .isNull());
    }

    @Test
    @DisplayName("and the CPU produces exactly the same rows for the same query")
    void nullPassThroughMatchesTheCpuPath() {
        tEnv.getConfig().set(GpuOffloadOptions.ENABLED, true);
        TestAcceleratorProvider.enable(4.0);
        List<Row> accelerated = run(NULLABLE_QUERY);
        int seen = TestAcceleratorProvider.ROWS_PROCESSED.get();

        TestAcceleratorProvider.reset();
        List<Row> onCpu = run(NULLABLE_QUERY);

        assertThat(seen).isPositive();
        // reset() disables the provider but deliberately leaves the counters alone, so "the
        // second run went around it" reads as "the count did not move".
        assertThat(TestAcceleratorProvider.ROWS_PROCESSED.get())
                .as("the second run must not have touched the provider")
                .isEqualTo(seen);
        assertThat(accelerated).containsExactlyInAnyOrderElementsOf(onCpu);
    }

    /**
     * A FLOAT expression runs on the CPU, and the accelerator is never even asked to try.
     *
     * <p>M2.3 offers two ways to be correct here — generate float arithmetic throughout, or refuse
     * — and this project refuses. Flink rounds float arithmetic at every step; a kernel computing
     * the chain in double and narrowing once rounds once, and for a cancelling expression like this
     * one the two answers are not close. The refusal is in the estimator, so the provider is not
     * offered the subtree at all.
     */
    @Test
    @DisplayName("a FLOAT expression is left on the CPU rather than offloaded")
    void floatExpressionStaysOnTheCpu() {
        tEnv.getConfig().set(GpuOffloadOptions.ENABLED, true);
        TestAcceleratorProvider.enable(10.0);

        List<Row> accelerated = run(FLOAT_QUERY);

        assertThat(TestAcceleratorProvider.ROWS_PROCESSED.get())
                .as("a FLOAT expression must not reach the accelerator")
                .isZero();

        TestAcceleratorProvider.reset();
        assertThat(accelerated)
                .as("and the rows are the CPU's, because the CPU is what ran")
                .containsExactlyInAnyOrderElementsOf(run(FLOAT_QUERY));
    }

    /**
     * The slot's managed memory decides the batch size; the configured value is only a ceiling.
     *
     * <p>Before M3.1 the provider was told whatever {@code table.exec.gpu-offload.batch-size} said
     * — a number chosen once, in a config file, by someone who could not know how much memory the
     * slot running the task would have or how wide its rows would be. The staging it sized was
     * invisible to Flink besides.
     *
     * <p>Asserted from the provider's side because that is the only place it shows. The default
     * ceiling is 262,144 rows; a MiniCluster slot's share of managed memory does not stretch to
     * that many rows of this width, so the number the provider is handed must be smaller.
     */
    @Test
    @DisplayName("the batch size comes from the slot's budget, not from the configured ceiling")
    void batchSizeComesFromTheSlotBudget() {
        tEnv.getConfig().set(GpuOffloadOptions.ENABLED, true);
        TestAcceleratorProvider.enable(10.0);

        run(QUERY);

        assertThat(TestAcceleratorProvider.LAST_HAD_MANAGED_MEMORY.get())
                .as(
                        "the transformation must declare managed memory, or there is no budget to size"
                                + " a batch from")
                .isTrue();
        assertThat(TestAcceleratorProvider.LAST_MAX_BATCH_SIZE.get())
                .as("the provider must be handed the slot's answer, not the config's")
                .isPositive()
                .isLessThan(tEnv.getConfig().get(GpuOffloadOptions.BATCH_SIZE));
    }

    /**
     * Which path the subtask took is published, because nothing else can say.
     *
     * <p>EXPLAIN reports what the planner decided, and since costing moved to the TaskManager that
     * is a different question — the plan says eligible, the subtask says served or not. Until this
     * existed the answer lived only in a log line on the machine that made it, which is how {@code
     * GpuOffloadExample} came to announce a query "ran on the GPU" while the TaskManager log
     * recorded a decline. Review finding E2.
     */
    @Test
    @DisplayName("the gauge reads 1 when a provider served the subtask")
    void acceleratedSubtaskReportsItself() {
        tEnv.getConfig().set(GpuOffloadOptions.ENABLED, true);
        TestAcceleratorProvider.enable(10.0);

        run(QUERY);

        assertThat(AccelInterpreterOperator.PUBLISHED.get(ACCELERATED))
                .as("published: %s", AccelInterpreterOperator.PUBLISHED)
                .isEqualTo(1);
        assertThat((String) AccelInterpreterOperator.PUBLISHED.get(DECISION))
                .as("the reason travels with the verdict, or the gauge only raises questions")
                .contains("claims");
    }

    @Test
    @DisplayName("a provider claiming to be slower is refused, and the CPU runs instead")
    void slowProviderIsRefused() {
        tEnv.getConfig().set(GpuOffloadOptions.ENABLED, true);
        // 0.54x is the measured figure for the query the planner used to refuse at plan time.
        TestAcceleratorProvider.enable(0.54);

        List<Row> rows = run(QUERY);

        assertThat(TestAcceleratorProvider.PLANS_OFFERED.get())
                .as("the provider is asked...")
                .isPositive();
        assertThat(TestAcceleratorProvider.ROWS_PROCESSED.get())
                .as("...and its answer is rejected, so no row reaches it")
                .isZero();
        assertThat(rows).isNotEmpty();
    }

    @Test
    @DisplayName("the provider declines by default, so its presence reroutes nothing")
    void declinesUnlessEnabled() {
        tEnv.getConfig().set(GpuOffloadOptions.ENABLED, true);

        run(QUERY);

        assertThat(TestAcceleratorProvider.ROWS_PROCESSED.get()).isZero();
    }
}
