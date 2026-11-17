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

package org.apache.flink.table.accelerator;

import org.apache.flink.annotation.PublicEvolving;

import java.io.Serializable;
import java.util.Arrays;

/**
 * How much work a subtree performs, counted rather than weighted.
 *
 * <h2>Why a histogram and not a single number</h2>
 *
 * <p>Collapsing an expression to "3458 weighted operations per row" requires a table of weights,
 * and that table <em>is</em> a model of one device: a GPU's cost ratio between a square root and a
 * multiply is not a CPU's, and neither is an FPGA's. Shipping a weighted scalar therefore puts a
 * calibrated hardware constant inside Flink, where it cannot be right for every accelerator and
 * cannot be recalibrated without changing Flink.
 *
 * <p>So Flink counts occurrences and stops. A provider applies its own weights, because it is the
 * only party that knows what its hardware charges for each operation.
 *
 * <h2>Bytes matter as much as operations</h2>
 *
 * <p>{@link #inputBytesPerRow()} against the operation counts is arithmetic intensity, which is
 * what actually decides whether moving work to a device pays: an accelerator wins in proportion to
 * arithmetic per byte read. A provider can decline a subtree that is bandwidth-bound for its
 * hardware without Flink needing any concept of bandwidth.
 */
@PublicEvolving
public final class AccelWorkProfile implements Serializable {

    private static final long serialVersionUID = 1L;

    /** {@link #estimatedRows()} when the planner had no estimate. */
    public static final long UNKNOWN_ROWS = -1L;

    private final int[] opsPerRow;
    private final int inputBytesPerRow;
    private final int outputBytesPerRow;
    private final long estimatedRows;

    /**
     * Whether this subtree must compute floating-point arithmetic the way Flink's CPU operator
     * does, rather than as fast as the accelerator can.
     *
     * <p>An accelerator is free to evaluate {@code a * b + c} as one fused operation with a single
     * rounding. That is a more accurate answer than Flink's two roundings and a <em>different</em>
     * one, and where the difference can reach a filter, a join condition, a grouping key, a sort
     * key or a limit it decides which rows the query returns. The same SQL would then answer
     * differently depending on which machine the scheduler picked, which is the one thing offload
     * may not do.
     *
     * <p>The planner decides this, not the provider: whether a computed column reaches such an
     * operator is a question about the surrounding graph, and only {@code GpuOffloadProcessor} can
     * see it. A provider that cannot honour the request must decline the subtree rather than
     * offload it anyway -- returning a fast answer that differs from the CPU's is the failure this
     * flag exists to prevent.
     *
     * <p>False does not mean "fusion is wanted". It means nothing downstream can observe the
     * difference, so the provider may choose.
     */
    private final boolean strictArithmetic;

    /**
     * Calls to a user's scalar function, which the planner counts but cannot weigh.
     *
     * <p>{@link #opsPerRow} is indexed by {@link AccelFunction}, and a user's function is none of
     * them: what it costs is in a method body the planner does not read, by design -- weighing it
     * would mean a bytecode library in {@code flink-table-planner}. So the call is reported and
     * the weighing is left to the provider, which reads the body anyway to decide whether it can
     * lower it at all.
     *
     * <p>Kept out of {@link #totalOpsPerRow()} for the same reason: that figure feeds a ceiling on
     * expression size, and a number nobody measured has no business moving a ceiling. It is here
     * so that a subtree whose only work is a user function is not mistaken for one with no work.
     */
    private final int userCalls;

    public AccelWorkProfile(
            int[] opsPerRow, int inputBytesPerRow, int outputBytesPerRow, long estimatedRows) {
        this(opsPerRow, inputBytesPerRow, outputBytesPerRow, estimatedRows, false);
    }

    public AccelWorkProfile(
            int[] opsPerRow,
            int inputBytesPerRow,
            int outputBytesPerRow,
            long estimatedRows,
            boolean strictArithmetic) {
        if (opsPerRow.length != AccelFunction.values().length) {
            throw new IllegalArgumentException(
                    "opsPerRow must have one slot per AccelFunction: expected "
                            + AccelFunction.values().length
                            + ", got "
                            + opsPerRow.length);
        }
        this.opsPerRow = opsPerRow.clone();
        this.inputBytesPerRow = inputBytesPerRow;
        this.outputBytesPerRow = outputBytesPerRow;
        this.estimatedRows = estimatedRows;
        this.strictArithmetic = strictArithmetic;
        this.userCalls = 0;
    }

    private AccelWorkProfile(AccelWorkProfile from, int userCalls) {
        this.opsPerRow = from.opsPerRow.clone();
        this.inputBytesPerRow = from.inputBytesPerRow;
        this.outputBytesPerRow = from.outputBytesPerRow;
        this.estimatedRows = from.estimatedRows;
        this.strictArithmetic = from.strictArithmetic;
        this.userCalls = userCalls;
    }

    /** The same work, with one more call to a user's scalar function counted. */
    public AccelWorkProfile withUserCall() {
        return new AccelWorkProfile(this, userCalls + 1);
    }

    /** How many calls to a user's scalar function a row makes. See {@link #userCalls}. */
    public int userCalls() {
        return userCalls;
    }

    /** Whether a row does anything at all -- built-in arithmetic or a user's function. */
    public boolean hasWork() {
        return totalOpsPerRow() > 0 || userCalls > 0;
    }

    /** See {@link #strictArithmetic}. */
    public boolean requiresStrictArithmetic() {
        return strictArithmetic;
    }

    /** The same work, but requiring the CPU's rounding. */
    public AccelWorkProfile withStrictArithmetic() {
        return strictArithmetic
                ? this
                : new AccelWorkProfile(
                        opsPerRow, inputBytesPerRow, outputBytesPerRow, estimatedRows, true);
    }

    /** How many times this subtree applies {@code function} to each input row. */
    public int count(AccelFunction function) {
        return opsPerRow[function.ordinal()];
    }

    /**
     * The work of two subtrees that run as one.
     *
     * <p>Operations add; bytes per row do not. A group's members see the same rows, so the widest
     * input width is what the group actually stages, and the last member's output is what leaves
     * it. Row counts take the larger estimate, or unknown if either is unknown -- guessing there
     * would be worse than admitting ignorance.
     */
    public AccelWorkProfile plus(AccelWorkProfile other) {
        int[] sum = opsPerRow.clone();
        for (int i = 0; i < sum.length; i++) {
            sum[i] += other.opsPerRow[i];
        }
        long rows;
        if (estimatedRows == UNKNOWN_ROWS || other.estimatedRows == UNKNOWN_ROWS) {
            rows = UNKNOWN_ROWS;
        } else {
            rows = Math.max(estimatedRows, other.estimatedRows);
        }
        return new AccelWorkProfile(
                new AccelWorkProfile(
                        sum,
                        Math.max(inputBytesPerRow, other.inputBytesPerRow),
                        other.outputBytesPerRow,
                        rows,
                        // Strictness is a requirement, so it survives grouping: if either member
                        // of a group must round the way the CPU does, the kernel they compile
                        // into must.
                        strictArithmetic || other.strictArithmetic),
                userCalls + other.userCalls);
    }

    /** Every count, indexed by {@link AccelFunction#ordinal()}. */
    public int[] opsPerRow() {
        return opsPerRow.clone();
    }

    /** Total operations per row, for a provider that wants the crude figure. */
    public int totalOpsPerRow() {
        int total = 0;
        for (int count : opsPerRow) {
            total += count;
        }
        return total;
    }

    /**
     * Bytes of input a row contributes to the subtree — the referenced columns only.
     *
     * <p>A column that is merely projected through is not counted: it never needs to reach the
     * accelerator, so charging its width against the transfer would misprice the subtree.
     */
    public int inputBytesPerRow() {
        return inputBytesPerRow;
    }

    public int outputBytesPerRow() {
        return outputBytesPerRow;
    }

    /**
     * The planner's cardinality estimate, or {@link #UNKNOWN_ROWS}.
     *
     * <p>Advisory. It is a plan-time guess that the adaptive scheduler may contradict, so a
     * provider should use it to size buffers rather than to refuse work.
     */
    public long estimatedRows() {
        return estimatedRows;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("AccelWorkProfile[");
        boolean first = true;
        for (AccelFunction function : AccelFunction.values()) {
            int count = opsPerRow[function.ordinal()];
            if (count > 0) {
                if (!first) {
                    sb.append(' ');
                }
                sb.append(function).append('x').append(count);
                first = false;
            }
        }
        return sb.append(", in=")
                .append(inputBytesPerRow)
                .append("B/row, out=")
                .append(outputBytesPerRow)
                .append("B/row, rows=")
                .append(estimatedRows == UNKNOWN_ROWS ? "unknown" : estimatedRows)
                .append(strictArithmetic ? ", strict arithmetic" : "")
                .append(']')
                .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof AccelWorkProfile)) {
            return false;
        }
        AccelWorkProfile other = (AccelWorkProfile) o;
        return Arrays.equals(opsPerRow, other.opsPerRow)
                && inputBytesPerRow == other.inputBytesPerRow
                && outputBytesPerRow == other.outputBytesPerRow
                && estimatedRows == other.estimatedRows
                && strictArithmetic == other.strictArithmetic;
    }

    @Override
    public int hashCode() {
        return 31 * Arrays.hashCode(opsPerRow) + Long.hashCode(estimatedRows);
    }
}
