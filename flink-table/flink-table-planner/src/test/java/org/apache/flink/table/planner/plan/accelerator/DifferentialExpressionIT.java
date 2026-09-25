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
import static org.assertj.core.api.Assertions.fail;

/**
 * Differential testing: the same query, both execution paths, compared.
 *
 * <p>This is the property a second execution backend must have, and the one the suite could not
 * assert before — result equivalence was checked only inside benchmark {@code main()} methods,
 * outside any test and outside CI. Review finding E1.
 *
 * <h2>Exact, not approximate</h2>
 *
 * <p>The comparison demands bit equality, because both paths do the same double arithmetic with
 * {@code java.lang.Math} on the same inputs. A difference here is a defect in the IR, the
 * conversion or the interpreter — not rounding. A device-backed provider reassociates and would
 * need a stated tolerance; that belongs with the device-gated profile of task M0.7, not here, where
 * a tolerance would only hide bugs this test exists to find.
 *
 * <p>NaN compares equal to NaN deliberately: {@code SQRT(-1)} is a legitimate result and both paths
 * must produce it identically.
 */
class DifferentialExpressionIT {

    /** Expressions per query. Batched so a thousand cases cost twenty jobs, not two thousand. */
    private static final int EXPRESSIONS_PER_QUERY = 50;

    private static final int QUERIES = 24;

    /**
     * A source the optimiser cannot fold away.
     *
     * <p>An inline {@code VALUES} fixture was the obvious choice and was useless: Calcite constant-
     * folds an expression over literals at plan time, so no Calc survives to accelerate and the
     * harness silently compared nothing. Sequences give real runtime values instead.
     *
     * <p>The ranges are chosen to reach the awkward cases with the vocabulary available: {@code v}
     * spans zero and negatives, so SQRT and LN produce NaN; {@code w} starts at zero, so division
     * produces infinities.
     */
    private static final String FIXTURE =
            "CREATE TABLE t ("
                    + "  v DOUBLE NOT NULL, w DOUBLE NOT NULL, i INT NOT NULL, id BIGINT NOT NULL"
                    + ") WITH ("
                    + "  'connector' = 'datagen', 'number-of-rows' = '8',"
                    + "  'fields.v.kind' = 'sequence', 'fields.v.start' = '-3', 'fields.v.end' = '4',"
                    + "  'fields.w.kind' = 'sequence', 'fields.w.start' = '0', 'fields.w.end' = '7',"
                    + "  'fields.i.kind' = 'sequence', 'fields.i.start' = '-2', 'fields.i.end' = '5',"
                    + "  'fields.id.kind' = 'sequence', 'fields.id.start' = '1', 'fields.id.end' = '8')";

    private TableEnvironment tEnv;

    @BeforeEach
    void setUp() {
        tEnv = TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        tEnv.executeSql(FIXTURE);
    }

    @AfterEach
    void tearDown() {
        TestAcceleratorProvider.reset();
    }

    @Test
    @DisplayName("generated expressions agree exactly on both paths")
    void generatedExpressionsAgree() {
        int compared = 0;
        int skipped = 0;
        String firstSkip = null;
        for (int q = 0; q < QUERIES; q++) {
            long seed = 20260915L + q;
            try {
                compared += compareOneQuery(seed, false);
            } catch (UnplannableQuery unplannable) {
                // Flink itself could not plan the generated query, on the CPU path, with the
                // accelerator disabled. That is a limitation of the planner rather than a
                // disagreement between the two paths, so it is not this harness's failure to
                // report -- but it is counted, because a harness that silently skipped everything
                // would look identical to one that passed.
                skipped++;
                if (firstSkip == null) {
                    firstSkip = unplannable.getMessage();
                }
            }
        }
        System.out.println(
                "differential: compared "
                        + compared
                        + " values, skipped "
                        + skipped
                        + "/"
                        + QUERIES
                        + " unplannable queries");
        if (firstSkip != null) {
            System.out.println("first unplannable query:\n" + firstSkip);
        }
        assertThat(skipped)
                .as("most generated queries must be plannable, or this proves nothing")
                .isLessThan(QUERIES / 2);
        assertThat(compared)
                .as("the harness must actually have compared something")
                .isGreaterThan(1000);
    }

    /** Flink could not plan the query at all — nothing to do with the accelerator. */
    private static final class UnplannableQuery extends RuntimeException {
        UnplannableQuery(String sql, Throwable cause) {
            super(sql, cause);
        }
    }

    @Test
    @DisplayName("a deliberately wrong accelerator is caught, so green means something")
    void detectsAnInjectedFault() {
        AssertionError caught = null;
        for (int q = 0; q < QUERIES && caught == null; q++) {
            try {
                compareOneQuery(20260915L + q, true);
            } catch (AssertionError expected) {
                caught = expected;
            } catch (UnplannableQuery skip) {
                // try the next seed
            }
        }
        if (caught == null) {
            fail("the harness did not notice an accelerator computing TIMES as addition");
        }
        assertThat(caught.getMessage()).contains("seed=");
        assertThat(caught.getMessage()).contains("accelerated=");
    }

    /**
     * Runs one generated query both ways and compares. Returns the number of values compared.
     *
     * @param corrupt inject the fault, to test the harness itself
     */
    private int compareOneQuery(long seed, boolean corrupt) {
        AccelExpressionGenerator generator = new AccelExpressionGenerator(seed);
        StringBuilder sql = new StringBuilder("SELECT id");
        List<String> expressions = new ArrayList<>();
        for (int i = 0; i < EXPRESSIONS_PER_QUERY; i++) {
            String expression = generator.expression(3);
            expressions.add(expression);
            sql.append(", ").append(expression).append(" AS c").append(i);
        }
        sql.append(" FROM t WHERE ").append(generator.condition());
        String query = sql.toString();

        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ACCELERATOR_ENABLED, false);
        final List<Row> onCpu;
        try {
            onCpu = run(query);
        } catch (AssertionError plannerLimitation) {
            throw new UnplannableQuery(query, plannerLimitation);
        }

        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ACCELERATOR_ENABLED, true);

        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ACCELERATOR_APPROXIMATE_PROJECTIONS, true);
        TestAcceleratorProvider.enable(10.0);
        if (corrupt) {
            TestAcceleratorProvider.corruptArithmetic();
        }
        List<Row> accelerated = run(query);
        int rowsSeen = TestAcceleratorProvider.ROWS_PROCESSED.get();
        int plansOffered = TestAcceleratorProvider.PLANS_OFFERED.get();
        TestAcceleratorProvider.reset();

        if (rowsSeen == 0) {
            String plan = tEnv.explainSql(query);
            int gi = plan.indexOf("== GPU Offload ==");
            String reason = "(no section)";
            if (gi >= 0) {
                for (String line : plan.substring(gi).split("\n")) {
                    String trimmed = line.trim();
                    if (!trimmed.isEmpty()
                            && !trimmed.startsWith("==")
                            && !trimmed.startsWith("CPU")
                            && !trimmed.startsWith("GPU")) {
                        reason = trimmed;
                        break;
                    }
                }
            }
            System.out.println("  seed=" + seed + " NOT ACCELERATED: " + reason);
            // Nothing was accelerated, so there is nothing to compare. Legitimate -- the generator
            // can produce an expression the estimator refuses -- but it must not be mistaken for a
            // pass, so it contributes zero comparisons.
            return 0;
        }
        if (onCpu.size() != accelerated.size()) {
            throw new AssertionError(
                    "row count differs: cpu="
                            + onCpu.size()
                            + " accelerated="
                            + accelerated.size()
                            + "\nseed="
                            + seed
                            + "\n"
                            + query);
        }
        // Batch execution does not promise an order, and the two runs need not agree on one.
        // id is unique in the fixture, so sorting by it compares like with like.
        onCpu.sort(java.util.Comparator.comparingLong(row -> (Long) row.getField(0)));
        accelerated.sort(java.util.Comparator.comparingLong(row -> (Long) row.getField(0)));

        int compared = 0;
        for (int r = 0; r < onCpu.size(); r++) {
            Row expected = onCpu.get(r);
            Row actual = accelerated.get(r);
            for (int c = 0; c < expected.getArity(); c++) {
                Object a = expected.getField(c);
                Object b = actual.getField(c);
                if (!same(a, b)) {
                    throw new AssertionError(
                            "value differs at row "
                                    + r
                                    + " column "
                                    + c
                                    + ": cpu="
                                    + a
                                    + " accelerated="
                                    + b
                                    + "\nseed="
                                    + seed
                                    + "\nexpression="
                                    + (c == 0 ? "id (pass-through)" : expressions.get(c - 1))
                                    + "\n"
                                    + query);
                }
                compared++;
            }
        }
        return compared;
    }

    /**
     * Bit equality, with NaN equal to itself.
     *
     * <p>{@code Double.compare} gives exactly that, and additionally separates {@code -0.0} from
     * {@code 0.0} — which is wanted: the two paths should agree on the sign of zero too.
     */
    private static boolean same(Object a, Object b) {
        if (a == null || b == null) {
            return a == b;
        }
        if (a instanceof Double && b instanceof Double) {
            return Double.compare((Double) a, (Double) b) == 0;
        }
        return a.equals(b);
    }

    private List<Row> run(String sql) {
        List<Row> rows = new ArrayList<>();
        try {
            TableResult result = tEnv.executeSql(sql);
            try (CloseableIterator<Row> it = result.collect()) {
                it.forEachRemaining(rows::add);
            }
        } catch (Exception | LinkageError e) {
            // Naming the query is the whole point: a generated failure that does not say what
            // generated it is a failure nobody can act on.
            throw new AssertionError("query failed:\n" + sql, e);
        }
        return rows;
    }
}
