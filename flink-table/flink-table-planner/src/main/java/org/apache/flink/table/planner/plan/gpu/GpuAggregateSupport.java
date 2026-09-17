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

import org.apache.flink.table.accelerator.AccelFunction;
import org.apache.flink.table.accelerator.AccelWorkProfile;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Which aggregates a device can compute, and what one row of them costs.
 *
 * <p>A {@code Calc} is elementwise, so the best a library can offer it is BLAS-1 and a generated
 * kernel beats that by fusing the whole expression into one pass. An aggregate is the other shape:
 * it <em>contracts</em> over rows, which is the operation GEMM and GEMV exist for. That is why this
 * is worth reaching at all, and it is also why the vocabulary here is small and closed rather than
 * open-ended like the expression generator's.
 *
 * <h2>What is admitted, and why so little</h2>
 *
 * <ul>
 *   <li><b>{@code SUM}</b> over a numeric column. Several at once are one contraction: {@code
 *       SUM(e1), ..., SUM(ek)} is {@code ones' * M} for the {@code rows x k} matrix the Calc wrote,
 *       which is a single GEMV. This is the fused multiple reduction that {@code @Reduce} cannot
 *       express, and the reason the library route is the only viable one.
 *   <li><b>{@code COUNT(*)}</b>, which is free: the operator knows how many rows it staged.
 *   <li><b>Nothing else.</b> {@code MIN} and {@code MAX} have no BLAS entry point and would need a
 *       generated reduction; {@code AVG} is a {@code SUM} the planner has usually already
 *       rewritten; {@code DISTINCT}, order statistics and string aggregation need sorting or
 *       hashing, which no bound library provides.
 * </ul>
 *
 * <h2>GROUP BY</h2>
 *
 * <p>Admitted since M5.3. It was refused while the only device route imagined for it was a sparse
 * matrix-vector product needing a grouping matrix nobody built; a bound library has the primitive
 * instead, and what reaches it is a key column and a value column.
 *
 * <p>The grouping costs nothing extra here, and that is not an oversight. A group-by is a library
 * call whose cost is a property of the library rather than of the expression, and what decides
 * whether the pair is worth moving is the <em>Calc</em> underneath it. The provider prices its own
 * primitive on the TaskManager, which is where that knowledge lives.
 */
public final class GpuAggregateSupport {

    /**
     * Operations charged for contracting one row into one accumulator.
     *
     * <p>One accumulate per row per sum. Small on purpose: an aggregate on its own does almost no
     * arithmetic, and what makes the pair worth offloading is the <em>Calc</em> underneath it. The
     * processor combines a subtree's work precisely so a cheap contraction travels with the
     * expensive expression that feeds it rather than being judged alone.
     */
    static final int OPS_PER_SUM = 1;

    private GpuAggregateSupport() {}

    /**
     * Costs an aggregate node, or explains what about it a device cannot do.
     *
     * @param grouping the grouping key indexes, which may be empty
     * @param auxGrouping auxiliary grouping columns
     * @param aggCalls the aggregate calls to compute
     */
    public static RowCost estimate(int[] grouping, int[] auxGrouping, AggregateCall[] aggCalls) {
        if (auxGrouping.length > 0) {
            // Auxiliary grouping carries columns through that the key functionally determines.
            // Correct on a device, but the IR has no way to say "carry this", so refusing is the
            // honest answer rather than dropping the columns.
            return RowCost.ineligible("auxiliary grouping columns are not expressed on device");
        }
        if (aggCalls.length == 0) {
            return RowCost.ineligible("no aggregate calls to compute");
        }

        int sums = 0;
        for (AggregateCall call : aggCalls) {
            if (call.isDistinct()) {
                return RowCost.ineligible("DISTINCT aggregates need hashing, not a contraction");
            }
            if (call.hasFilter()) {
                return RowCost.ineligible("FILTER on an aggregate is not expressed on device");
            }
            if (call.getAggregation().getKind() == SqlKind.COUNT && call.getArgList().isEmpty()) {
                // COUNT(*) costs nothing: the staged row count already answers it.
                continue;
            }
            if (call.getAggregation().getKind() != SqlKind.SUM) {
                return RowCost.ineligible(
                        call.getAggregation().getName() + " has no device contraction");
            }
            if (call.getArgList().size() != 1) {
                return RowCost.ineligible("SUM over more than one column is not a contraction");
            }
            if (!isContractibleType(call.getType().getSqlTypeName())) {
                return RowCost.ineligible(
                        "SUM produces "
                                + call.getType().getSqlTypeName()
                                + ", which is not staged");
            }
            sums++;
        }
        if (sums == 0) {
            // COUNT(*) alone: correct on a device and pointless there, since the answer is the row
            // count and reading the input is the whole job.
            return RowCost.ineligible("only COUNT(*), which needs no device");
        }
        // A contraction is one accumulate per row per sum. Counted, not weighted: what that
        // accumulate costs is the provider's business.
        int[] ops = new int[AccelFunction.values().length];
        ops[AccelFunction.PLUS.ordinal()] = sums * OPS_PER_SUM;
        return RowCost.of(
                new AccelWorkProfile(ops, 8 * sums, 8 * sums, AccelWorkProfile.UNKNOWN_ROWS));
    }

    /** The accumulator types a staged contraction can produce. */
    private static boolean isContractibleType(SqlTypeName type) {
        return type == SqlTypeName.DOUBLE
                || type == SqlTypeName.FLOAT
                || type == SqlTypeName.REAL
                || type == SqlTypeName.INTEGER
                || type == SqlTypeName.BIGINT;
    }
}
