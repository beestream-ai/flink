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
 * M5.4: a sort reaches an accelerator, and orders what the CPU plan orders.
 *
 * <p>The first offloadable node here that is neither per-row nor partial. A Calc can start work on
 * the first record and a local aggregate may emit a key more than once; a sort can do neither, so
 * the operator has to hold its whole input and emit at end of input. These cases exist to pin that
 * the seam reaches such a node at all, and that the order that comes out is the order Flink's own
 * sort produces.
 *
 * <p>Runs against {@link TestAcceleratorProvider}, which orders on the host, so this is the planner
 * half on a machine with no device.
 */
class AcceleratedSortIT {

    private TableEnvironment tEnv;

    @BeforeEach
    void setUp() {
        tEnv = TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        tEnv.executeSql(
                // Both sequences span the same range on purpose: datagen stops at the shortest
                // one, so a 1..20 key beside a 1..200 value yields twenty rows and not two
                // hundred. Sequences rather than random values because the two runs below have to
                // sort identical data to be comparable at all.
                "CREATE TABLE t (k INT NOT NULL, val DOUBLE NOT NULL) WITH ("
                        + "'connector' = 'datagen', 'number-of-rows' = '200',"
                        + "'fields.k.kind' = 'sequence',"
                        + "'fields.k.start' = '1', 'fields.k.end' = '200',"
                        + "'fields.val.kind' = 'sequence',"
                        + "'fields.val.start' = '1', 'fields.val.end' = '200')");
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ACCELERATOR_ENABLED, true);
        // A sort's input arrives over a blocking exchange, which is exactly where M4.3's
        // cardinality gate applies: a subtask that cannot fill one batch is refused, because
        // setting a device up costs the same whether the batch is full or holds eleven rows. At
        // the default batch size these 200 rows would be refused and nothing would reach a
        // provider -- so the batch size comes down rather than the row count going up, which
        // would make the test slow to say the same thing.
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ACCELERATOR_BATCH_SIZE, 16);
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
    @DisplayName("a single-key sort is offered to a provider, and the provider runs it")
    void theSortReachesTheAccelerator() {
        TestAcceleratorProvider.enable(10.0);

        List<Row> rows = run("SELECT k, val FROM t ORDER BY k");

        assertThat(TestAcceleratorProvider.PLANS_OFFERED.get())
                .as("a sort must be offered; before M5.4 the IR could not hold one")
                .isPositive();
        assertThat(TestAcceleratorProvider.LAST_WAS_SORT.get())
                .as("the subtree offered must be the sort itself")
                .isTrue();
        assertThat(TestAcceleratorProvider.ROWS_PROCESSED.get())
                .as("rows must have gone through the accelerated operator, not around it")
                .isPositive();
        assertThat(rows).hasSize(200);
    }

    @Test
    @DisplayName("and orders exactly as the CPU plan orders, position for position")
    void theOrderMatchesTheCpuPlan() {
        TestAcceleratorProvider.enable(10.0);
        List<Row> accelerated = run("SELECT k, val FROM t ORDER BY k");
        int seen = TestAcceleratorProvider.ROWS_PROCESSED.get();

        TestAcceleratorProvider.reset();
        List<Row> onCpu = run("SELECT k, val FROM t ORDER BY k");

        assertThat(seen).as("the first run must have been accelerated").isPositive();
        assertThat(TestAcceleratorProvider.ROWS_PROCESSED.get())
                .as("the second run must not have touched the provider")
                .isEqualTo(seen);
        // containsExactly, not containsExactlyInAnyOrder: for a sort the order IS the result, and
        // an order-insensitive assertion would pass against an operator that sorted nothing.
        assertThat(accelerated).containsExactlyElementsOf(onCpu);
    }

    @Test
    @DisplayName("descending is carried through, not silently ascending")
    void descendingIsCarried() {
        TestAcceleratorProvider.enable(10.0);
        List<Row> accelerated = run("SELECT k, val FROM t ORDER BY k DESC");

        TestAcceleratorProvider.reset();
        List<Row> onCpu = run("SELECT k, val FROM t ORDER BY k DESC");

        assertThat(accelerated).containsExactlyElementsOf(onCpu);
        assertThat((Integer) accelerated.get(0).getField(0))
                .as("a descending sort must start at the largest key")
                .isEqualTo(200);
    }

    @Test
    @DisplayName("a two-key sort is refused, because composing keys needs a comparator")
    void twoKeysAreRefused() {
        TestAcceleratorProvider.enable(10.0);

        run("SELECT k, val FROM t ORDER BY k, val");

        assertThat(TestAcceleratorProvider.LAST_WAS_SORT.get())
                .as("a multi-key sort must not reach a provider as a sort")
                .isFalse();
    }

    @Test
    @DisplayName("a non-integer sort key is refused, which is the binding's limit not a device's")
    void aDoubleKeyIsRefused() {
        TestAcceleratorProvider.enable(10.0);

        run("SELECT k, val FROM t ORDER BY val");

        assertThat(TestAcceleratorProvider.LAST_WAS_SORT.get())
                .as("only an INT key is ordered today; the shim takes one INT32 column")
                .isFalse();
    }
}
