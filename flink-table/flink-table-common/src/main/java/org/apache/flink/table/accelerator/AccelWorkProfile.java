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

    public AccelWorkProfile(
            int[] opsPerRow, int inputBytesPerRow, int outputBytesPerRow, long estimatedRows) {
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
    }

    /** How many times this subtree applies {@code function} to each input row. */
    public int count(AccelFunction function) {
        return opsPerRow[function.ordinal()];
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
                && estimatedRows == other.estimatedRows;
    }

    @Override
    public int hashCode() {
        return 31 * Arrays.hashCode(opsPerRow) + Long.hashCode(estimatedRows);
    }
}
