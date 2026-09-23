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
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The semantic release gate: every value SQL can produce awkwardly, compared on both paths.
 *
 * <h2>Why this exists beside {@link DifferentialExpressionIT}</h2>
 *
 * <p>That one generates expressions at random and compares thousands of values, which is the right
 * way to find the disagreement nobody thought of. It is the wrong way to guarantee that a
 * <em>particular</em> hazard is covered: its fixture spans eight sequential values, so whether a
 * given run ever evaluated {@code -0.0}, or {@code Infinity - Infinity}, or a cast that overflows,
 * is a property of the seed.
 *
 * <p>This one is the complement. Every case is written down, named, and adversarial on purpose. A
 * reviewer can read the list and say what is and is not covered, which is what a release gate has
 * to allow and a random harness cannot.
 *
 * <h2>The rule each case is held to</h2>
 *
 * <p><b>Identical results, or a refusal.</b> Bit equality, with NaN equal to itself and {@code
 * -0.0} distinct from {@code 0.0} — the two paths should agree on the sign of a zero as much as on
 * anything else. A provider declining the subtree passes: refusal is the expected outcome for most
 * of SQL and is the safe one. What must never happen is a different answer.
 *
 * <p>Refusal passing creates the obvious hole — a provider that refuses everything would sail
 * through — so {@link #adversarialValuesAgreeOnBothPaths()} also requires that a substantial
 * majority of the cases actually ran on the accelerator, and prints which did not.
 *
 * <h2>What a device provider changes about this</h2>
 *
 * <p>Nothing about the rule, and a great deal about the difficulty. The provider on this classpath
 * is an interpreter doing {@code java.lang.Math} on the same doubles, so exact agreement is
 * reachable; a real device reassociates, uses a different transcendental implementation, and may
 * not agree bit for bit on {@code EXP} at all. That is the point of writing the cases down here
 * rather than in the provider: the same list can be run against any provider, and where a device
 * cannot meet it, the honest answers are to declare a tolerance for the affected functions or to
 * refuse them — not to loosen the gate.
 *
 * <p><b>Not covered, and deliberately:</b> floating-point reassociation in {@code SUM} over a
 * {@code DOUBLE} column. Whether SQL {@code SUM(DOUBLE)} is order-defined in Flink is an open
 * question, and until it is answered a test asserting either equality or a bound would be asserting
 * a decision nobody has made. See {@code FLIP-DRAFT.md} §5.3.
 */
class AcceleratorConformanceIT {

    /**
     * Sequences, not literals, and the ranges are chosen rather than round.
     *
     * <p>Literals would be folded at plan time — Calcite evaluates an expression over constants
     * before any {@code Calc} exists, so the harness would compare two constant-folded plans and
     * prove nothing. Every case below therefore reaches its awkward value <em>through</em> a
     * column: {@code v} spans zero and negatives so {@code SQRT} and {@code LN} reach NaN, {@code
     * w} starts at zero so division reaches the infinities, and {@code 0.0 * v} is how a negative
     * zero is produced without writing one down.
     */
    private static final String FIXTURE =
            "CREATE TABLE t ("
                    + "  v DOUBLE NOT NULL, w DOUBLE NOT NULL,"
                    + "  i INT NOT NULL, j INT NOT NULL, id BIGINT NOT NULL"
                    + ") WITH ("
                    + "  'connector' = 'datagen', 'number-of-rows' = '8',"
                    + "  'fields.v.kind' = 'sequence', 'fields.v.start' = '-3', 'fields.v.end' = '4',"
                    + "  'fields.w.kind' = 'sequence', 'fields.w.start' = '0', 'fields.w.end' = '7',"
                    + "  'fields.i.kind' = 'sequence', 'fields.i.start' = '-4', 'fields.i.end' = '3',"
                    + "  'fields.j.kind' = 'sequence', 'fields.j.start' = '1', 'fields.j.end' = '8',"
                    + "  'fields.id.kind' = 'sequence', 'fields.id.start' = '1', 'fields.id.end' = '8')";

    /**
     * A nullable column whose nullness is decided, not sampled.
     *
     * <p>{@code null-rate} is applied per row from a {@code Random} the test cannot seed, so a rate
     * between 0 and 1 would put different nulls in front of the two runs and compare nothing. A
     * rate of exactly 1 always nulls, and a {@code sequence} field never does — datagen rejects
     * {@code null-rate} on one outright. So the union of the two tables is half null,
     * deterministically, and the two runs see the same rows.
     */
    private static final String NULL_FIXTURE =
            "CREATE VIEW n AS\n"
                    + "  SELECT id, a, b FROM absent\n"
                    + "  UNION ALL\n"
                    + "  SELECT id, a, b FROM present";

    private static final String ABSENT =
            "CREATE TABLE absent (id BIGINT NOT NULL, a DOUBLE, b DOUBLE NOT NULL) WITH ("
                    + "  'connector' = 'datagen', 'number-of-rows' = '4',"
                    + "  'fields.id.kind' = 'sequence', 'fields.id.start' = '1', 'fields.id.end' = '4',"
                    + "  'fields.a.null-rate' = '1.0',"
                    + "  'fields.b.kind' = 'sequence', 'fields.b.start' = '1', 'fields.b.end' = '4')";

    private static final String PRESENT =
            "CREATE TABLE present (id BIGINT NOT NULL, a DOUBLE, b DOUBLE NOT NULL) WITH ("
                    + "  'connector' = 'datagen', 'number-of-rows' = '4',"
                    + "  'fields.id.kind' = 'sequence', 'fields.id.start' = '5', 'fields.id.end' = '8',"
                    + "  'fields.a.kind' = 'sequence', 'fields.a.start' = '-1', 'fields.a.end' = '2',"
                    + "  'fields.b.kind' = 'sequence', 'fields.b.start' = '5', 'fields.b.end' = '8')";

    private TableEnvironment tEnv;

    @BeforeEach
    void setUp() {
        tEnv = TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        tEnv.executeSql(FIXTURE);
        tEnv.executeSql(ABSENT);
        tEnv.executeSql(PRESENT);
        tEnv.executeSql(NULL_FIXTURE);
    }

    @AfterEach
    void tearDown() {
        TestAcceleratorProvider.reset();
    }

    /** The cases, in the order a reader should meet them. */
    private static Map<String, String> cases() {
        Map<String, String> cases = new LinkedHashMap<>();

        // --- NaN: produced, propagated, and compared -----------------------------------------
        cases.put("NaN from SQRT of a negative", "SELECT id, SQRT(v) AS c FROM t");
        cases.put("NaN from LN of a non-positive", "SELECT id, LN(v) AS c FROM t");
        cases.put("NaN propagates through arithmetic", "SELECT id, SQRT(v) + 1.0 AS c FROM t");
        cases.put("NaN times zero is still NaN", "SELECT id, SQRT(v) * 0.0 AS c FROM t");
        cases.put(
                "NaN in a negated comparison against a decimal literal",
                "SELECT id, v AS c FROM t WHERE SQRT(v) <= 0.0");
        cases.put("NaN is not equal to itself", "SELECT id, v AS c FROM t WHERE SQRT(v) = SQRT(v)");
        cases.put("LEAST with a NaN operand", "SELECT id, LEAST(SQRT(v), 1.0) AS c FROM t");
        cases.put("GREATEST with a NaN operand", "SELECT id, GREATEST(SQRT(v), 1.0) AS c FROM t");
        cases.put("ABS of a NaN", "SELECT id, ABS(SQRT(v)) AS c FROM t");
        cases.put("SIGN of a NaN", "SELECT id, SIGN(SQRT(v)) AS c FROM t");

        // --- The two comparison flavours, which are not the same comparison -------------------
        //
        // A bare literal is DECIMAL, and Flink compares an approximate numeric against a decimal
        // with a total order where NaN is greatest. Written as a DOUBLE, the same predicate is
        // IEEE and drops the NaN rows. Both are pinned: the accelerator has to agree with Flink
        // about which one it is looking at, and the difference is invisible in the SQL.
        cases.put(
                "NaN against a decimal literal, where NaN is greatest",
                "SELECT id, v AS c FROM t WHERE SQRT(v) > 0.0");
        cases.put(
                "NaN against a DOUBLE literal, where every comparison is false",
                "SELECT id, v AS c FROM t WHERE SQRT(v) > CAST(0.0 AS DOUBLE)");
        cases.put("NaN below a decimal literal", "SELECT id, v AS c FROM t WHERE SQRT(v) < 1.0");
        cases.put(
                "NaN below a DOUBLE literal",
                "SELECT id, v AS c FROM t WHERE SQRT(v) < CAST(1.0 AS DOUBLE)");
        cases.put(
                "NaN at or above a decimal literal",
                "SELECT id, v AS c FROM t WHERE SQRT(v) >= 1.0");
        cases.put(
                "NaN at or below a decimal literal",
                "SELECT id, v AS c FROM t WHERE SQRT(v) <= 1.0");
        cases.put("NaN equal to a decimal literal", "SELECT id, v AS c FROM t WHERE SQRT(v) = 1.0");
        cases.put(
                "NaN not equal to a decimal literal",
                "SELECT id, v AS c FROM t WHERE SQRT(v) <> 1.0");
        cases.put(
                "negative zero against a decimal zero, which orders below it",
                "SELECT id, v AS c FROM t WHERE 0.0 * v < 0.0");
        cases.put(
                "negative zero equal to a decimal zero",
                "SELECT id, v AS c FROM t WHERE 0.0 * v = 0.0");
        cases.put(
                "negative zero at or above a decimal zero",
                "SELECT id, v AS c FROM t WHERE 0.0 * v >= 0.0");
        cases.put(
                "negative zero against a DOUBLE zero, where the signs do not matter",
                "SELECT id, v AS c FROM t WHERE 0.0 * v < CAST(0.0 AS DOUBLE)");
        cases.put(
                "an infinity against a decimal literal",
                "SELECT id, v AS c FROM t WHERE 1.0 / w > 1.0");

        // --- Infinities ------------------------------------------------------------------------
        cases.put("positive infinity from division by zero", "SELECT id, 1.0 / w AS c FROM t");
        cases.put("negative infinity from division by zero", "SELECT id, -1.0 / w AS c FROM t");
        cases.put("infinity minus infinity is NaN", "SELECT id, 1.0 / w - 1.0 / w AS c FROM t");
        cases.put("infinity times zero is NaN", "SELECT id, (1.0 / w) * 0.0 AS c FROM t");
        cases.put("zero divided by zero", "SELECT id, w / w AS c FROM t");
        cases.put("overflow to infinity through EXP", "SELECT id, EXP(v * 1000.0) AS c FROM t");
        cases.put("underflow to zero through EXP", "SELECT id, EXP(v * -1000.0) AS c FROM t");
        cases.put("LN of zero is negative infinity", "SELECT id, LN(w) AS c FROM t");

        // --- Signed zero -----------------------------------------------------------------------
        cases.put("negative zero from multiplying by a negative", "SELECT id, 0.0 * v AS c FROM t");
        cases.put("the sign of a negative zero", "SELECT id, SIGN(0.0 * v) AS c FROM t");
        cases.put("dividing by a negative zero", "SELECT id, 1.0 / (0.0 * v) AS c FROM t");
        cases.put("ABS clears the sign of a zero", "SELECT id, ABS(0.0 * v) AS c FROM t");

        // --- Integer arithmetic: truncation direction and overflow ----------------------------
        cases.put("integer division truncates toward zero", "SELECT id, i / j AS c FROM t");
        cases.put(
                "integer division with a negative dividend", "SELECT id, (i - 10) / j AS c FROM t");
        cases.put("MOD takes the sign of the dividend", "SELECT id, MOD(i, j) AS c FROM t");
        cases.put(
                "MOD with a larger divisor returns the dividend",
                "SELECT id, MOD(i, j + 100) AS c FROM t");
        cases.put("integer negation of the whole range", "SELECT id, -i AS c FROM t");
        cases.put(
                "integer multiplication that can overflow",
                "SELECT id, i * j * 1000000 AS c FROM t");

        // --- Casts ------------------------------------------------------------------------------
        cases.put(
                "cast DOUBLE to INT truncates toward zero",
                "SELECT id, CAST(v * 1.5 AS INT) AS c FROM t");
        cases.put("cast a negative DOUBLE to INT", "SELECT id, CAST(v * -1.5 AS INT) AS c FROM t");
        cases.put(
                "cast INT to DOUBLE then divide", "SELECT id, CAST(i AS DOUBLE) / 3.0 AS c FROM t");
        cases.put(
                "cast a DOUBLE far outside the INT range",
                "SELECT id, CAST(v * 1.0E10 AS INT) AS c FROM t");
        cases.put("cast a NaN to INT", "SELECT id, CAST(SQRT(v) AS INT) AS c FROM t");
        cases.put("cast an infinity to INT", "SELECT id, CAST(1.0 / w AS INT) AS c FROM t");
        cases.put("cast BIGINT to DOUBLE", "SELECT id, CAST(id AS DOUBLE) * 0.5 AS c FROM t");

        // --- Rounding ----------------------------------------------------------------------------
        cases.put("FLOOR of a negative", "SELECT id, FLOOR(v / 2.0) AS c FROM t");
        cases.put("CEIL of a negative", "SELECT id, CEIL(v / 2.0) AS c FROM t");
        cases.put("FLOOR at an exact integer", "SELECT id, FLOOR(v) AS c FROM t");
        cases.put("FLOOR of a NaN", "SELECT id, FLOOR(SQRT(v)) AS c FROM t");
        cases.put("CEIL of an infinity", "SELECT id, CEIL(1.0 / w) AS c FROM t");

        // --- Transcendentals at their awkward arguments
        // -------------------------------------------
        cases.put("POWER with a zero exponent", "SELECT id, POWER(v, 0.0) AS c FROM t");
        cases.put(
                "POWER with a negative base and a fractional exponent",
                "SELECT id, POWER(v, 0.5) AS c FROM t");
        cases.put("POWER of zero to a negative exponent", "SELECT id, POWER(w, -1.0) AS c FROM t");
        cases.put("ASIN outside its domain", "SELECT id, ASIN(v) AS c FROM t");
        cases.put("ACOS outside its domain", "SELECT id, ACOS(v) AS c FROM t");
        cases.put("ATAN2 at the origin", "SELECT id, ATAN2(w, w) AS c FROM t");
        cases.put("TANH saturates", "SELECT id, TANH(v * 100.0) AS c FROM t");
        cases.put("LOG2 of a non-positive", "SELECT id, LOG2(v) AS c FROM t");

        // --- Three-valued logic, over a column that is really sometimes absent
        // ---------------------
        cases.put("null propagates through arithmetic", "SELECT id, a + 1.0 AS c FROM n");
        cases.put("null propagates through a function", "SELECT id, SQRT(a) AS c FROM n");
        cases.put("null in a comparison drops the row", "SELECT id, b AS c FROM n WHERE a > 0.0");
        cases.put(
                "null in a negated comparison also drops the row",
                "SELECT id, b AS c FROM n WHERE a <= 0.0");
        cases.put(
                "IS NULL selects exactly the absent ones",
                "SELECT id, b AS c FROM n WHERE a IS NULL");
        cases.put(
                "IS NOT NULL selects exactly the rest",
                "SELECT id, b AS c FROM n WHERE a IS NOT NULL");
        cases.put("unknown OR true is true", "SELECT id, b AS c FROM n WHERE a > 0.0 OR b > 0.0");
        cases.put(
                "unknown AND false is false",
                "SELECT id, b AS c FROM n WHERE a > 0.0 AND b > 100.0");
        cases.put("NOT of unknown is unknown", "SELECT id, b AS c FROM n WHERE NOT (a > 0.0)");
        cases.put("a null operand makes LEAST null", "SELECT id, LEAST(a, b) AS c FROM n");
        cases.put("a null operand makes GREATEST null", "SELECT id, GREATEST(a, b) AS c FROM n");

        return cases;
    }

    /**
     * Cases where the accelerator is known to give a different answer from Flink's own CPU path.
     *
     * <p>A quarantine, not an exemption. Each entry is a defect with a written cause; the suite
     * fails on any divergence <em>not</em> listed here, so the list can only grow deliberately.
     * When one is fixed, its case starts agreeing and {@link #knownDivergencesAreStillDivergent()}
     * fails — which is how a fix announces itself rather than being noticed years later.
     */
    private static final Map<String, String> KNOWN_DIVERGENCES = knownDivergences();

    private static Map<String, String> knownDivergences() {
        // Empty, and it should stay that way. The one entry it held --- NaN compared against a
        // bare decimal literal --- is now fixed rather than tolerated; see totalOrder() in
        // AccelInterpreter for what Flink actually does there and why matching it was not
        // optional.
        return new LinkedHashMap<>();
    }

    @Test
    @DisplayName("every quarantined name is a case that still exists")
    void quarantineNamesRealCases() {
        // A typo in the list would silently exempt nothing and hide nothing, which is worse than
        // either: the run would look clean while the case it meant to cover was still failing.
        assertThat(cases().keySet()).containsAll(KNOWN_DIVERGENCES.keySet());
    }

    @Test
    @DisplayName("every adversarial case agrees exactly, or is refused")
    void adversarialValuesAgreeOnBothPaths() {
        Map<String, String> cases = cases();
        List<String> refused = new ArrayList<>();
        List<String> unplannable = new ArrayList<>();
        Map<String, String> diverged = new LinkedHashMap<>();
        int accelerated = 0;
        int valuesCompared = 0;

        for (Map.Entry<String, String> one : cases.entrySet()) {
            String name = one.getKey();
            String sql = one.getValue();

            tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ACCELERATOR_ENABLED, false);
            List<Row> onCpu;
            try {
                onCpu = run(sql);
            } catch (Throwable plannerOrRuntime) {
                // Flink itself cannot do this on the CPU. Not a conformance question, but recorded
                // rather than dropped: a suite that silently skipped half its cases would look
                // exactly like one that passed them.
                unplannable.add(name + " -- " + rootMessage(plannerOrRuntime));
                continue;
            }

            tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ACCELERATOR_ENABLED, true);
            TestAcceleratorProvider.enable(10.0);
            List<Row> onAccelerator = run(sql);
            int rowsSeen = TestAcceleratorProvider.ROWS_PROCESSED.get();
            TestAcceleratorProvider.reset();

            if (rowsSeen == 0) {
                refused.add(name);
                // Still compared. A refusal means the CPU ran both times, so the two must agree
                // trivially -- and if they do not, something other than the accelerator is wrong.
            } else {
                accelerated++;
            }
            String divergence = describeDivergence(onCpu, onAccelerator);
            if (divergence != null) {
                diverged.put(name, divergence + "\n      " + sql);
            }
            valuesCompared += Math.min(onCpu.size(), onAccelerator.size()) * 2;
        }

        System.out.printf(
                "conformance: %d cases, %d accelerated, %d refused, %d unplannable, %d values"
                        + " compared%n",
                cases.size(), accelerated, refused.size(), unplannable.size(), valuesCompared);
        if (!refused.isEmpty()) {
            System.out.println("  refused by the provider (a passing outcome):");
            refused.forEach(r -> System.out.println("    " + r));
        }
        if (!unplannable.isEmpty()) {
            System.out.println("  Flink could not run these on the CPU either:");
            unplannable.forEach(r -> System.out.println("    " + r));
        }

        if (!diverged.isEmpty()) {
            System.out.println("  DIVERGED -- the two paths gave different answers:");
            diverged.forEach((k, v) -> System.out.println("    " + k + ": " + v));
        }

        // Set equality, not containment, and both directions matter. A new name here is a defect
        // nobody has looked at. A missing one means a quarantined defect has started agreeing --
        // which is good news that must be noticed, because a quarantine that outlives its defect
        // is a hole in the gate that looks exactly like a passing test.
        assertThat(diverged.keySet())
                .as(
                        "divergences must be exactly the written-down ones; see KNOWN_DIVERGENCES"
                                + " for what each accepted entry means and why")
                .containsExactlyInAnyOrderElementsOf(KNOWN_DIVERGENCES.keySet());

        assertThat(valuesCompared)
                .as("the suite must actually have compared something")
                .isGreaterThan(200);
        assertThat(accelerated)
                .as(
                        "a provider that refuses everything must not pass this gate; %d of %d"
                                + " cases reached the accelerator",
                        accelerated, cases.size())
                .isGreaterThan(cases.size() / 2);
    }

    @Test
    @DisplayName("the comparison notices a provider that computes the wrong answer")
    void detectsAnInjectedFault() {
        // Without this, every assertion above could be vacuous: a comparison that never fails
        // proves nothing about the cases it ran.
        String sql = "SELECT id, v * 2.0 AS c FROM t";

        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ACCELERATOR_ENABLED, false);
        List<Row> onCpu = run(sql);

        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ACCELERATOR_ENABLED, true);
        TestAcceleratorProvider.enable(10.0);
        TestAcceleratorProvider.corruptArithmetic();
        List<Row> corrupted = run(sql);
        assertThat(TestAcceleratorProvider.ROWS_PROCESSED.get())
                .as("the fault has to have been given something to corrupt")
                .isGreaterThan(0);
        TestAcceleratorProvider.reset();

        boolean noticed;
        try {
            compare("injected fault", sql, onCpu, corrupted);
            noticed = false;
        } catch (AssertionError expected) {
            noticed = true;
        }
        assertThat(noticed)
                .as("the comparison did not notice an accelerator computing TIMES as addition")
                .isTrue();
    }

    // -------------------------------------------------------------------------------------------

    /**
     * The first disagreement between two result sets, or null when there is none.
     *
     * <p>Returned rather than thrown so that one divergence does not hide the rest: a conformance
     * run that stops at its first finding takes as many runs to enumerate the damage as there are
     * findings.
     */
    private static String describeDivergence(List<Row> onCpu, List<Row> accelerated) {
        if (onCpu.size() != accelerated.size()) {
            return "row count differs: cpu=" + onCpu.size() + " accelerated=" + accelerated.size();
        }
        onCpu.sort(Comparator.comparingLong(row -> (Long) row.getField(0)));
        accelerated.sort(Comparator.comparingLong(row -> (Long) row.getField(0)));
        for (int r = 0; r < onCpu.size(); r++) {
            Row expected = onCpu.get(r);
            Row actual = accelerated.get(r);
            for (int c = 0; c < expected.getArity(); c++) {
                if (!same(expected.getField(c), actual.getField(c))) {
                    return "row "
                            + r
                            + " column "
                            + c
                            + ": cpu="
                            + render(expected.getField(c))
                            + " accelerated="
                            + render(actual.getField(c));
                }
            }
        }
        return null;
    }

    /** Compares two result sets value by value. Returns how many values were compared. */
    private static int compare(String name, String sql, List<Row> onCpu, List<Row> accelerated) {
        if (onCpu.size() != accelerated.size()) {
            throw new AssertionError(
                    "row count differs for \""
                            + name
                            + "\": cpu="
                            + onCpu.size()
                            + " accelerated="
                            + accelerated.size()
                            + "\n"
                            + sql);
        }
        // Batch execution promises no order and the two runs need not pick the same one. id is
        // unique across both fixtures, so sorting by it compares like with like.
        onCpu.sort(Comparator.comparingLong(row -> (Long) row.getField(0)));
        accelerated.sort(Comparator.comparingLong(row -> (Long) row.getField(0)));

        int compared = 0;
        for (int r = 0; r < onCpu.size(); r++) {
            Row expected = onCpu.get(r);
            Row actual = accelerated.get(r);
            for (int c = 0; c < expected.getArity(); c++) {
                Object a = expected.getField(c);
                Object b = actual.getField(c);
                if (!same(a, b)) {
                    throw new AssertionError(
                            "\""
                                    + name
                                    + "\" differs at row "
                                    + r
                                    + " column "
                                    + c
                                    + ": cpu="
                                    + render(a)
                                    + " accelerated="
                                    + render(b)
                                    + "\n"
                                    + sql);
                }
                compared++;
            }
        }
        return compared;
    }

    /**
     * Bit equality, with NaN equal to itself and {@code -0.0} distinct from {@code 0.0}.
     *
     * <p>{@code Double.compare} gives exactly that, which is what a conformance gate wants: two
     * backends that disagree about the sign of a zero disagree, even though {@code ==} would not
     * say so.
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

    /** {@code toString} loses the sign of a zero, which is the one thing worth seeing here. */
    private static String render(Object value) {
        if (value instanceof Double) {
            double d = (Double) value;
            if (d == 0.0) {
                return Double.doubleToRawLongBits(d) == 0L ? "0.0" : "-0.0";
            }
        }
        return String.valueOf(value);
    }

    private static String rootMessage(Throwable t) {
        Throwable root = t;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        String message = root.getMessage();
        return message == null ? root.getClass().getSimpleName() : message.split("\n")[0];
    }

    private List<Row> run(String sql) {
        List<Row> rows = new ArrayList<>();
        TableResult result = tEnv.executeSql(sql);
        try (CloseableIterator<Row> it = result.collect()) {
            it.forEachRemaining(rows::add);
        } catch (Exception e) {
            throw new IllegalStateException("query failed:\n" + sql, e);
        }
        return rows;
    }
}
