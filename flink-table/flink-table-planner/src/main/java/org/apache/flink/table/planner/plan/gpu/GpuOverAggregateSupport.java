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
import org.apache.flink.table.planner.plan.nodes.exec.spec.OverSpec;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.SqlKind;

import java.util.OptionalInt;

/**
 * Which {@code OVER} windows a device could accumulate, and what accumulating one costs.
 *
 * <p>The refusals here are narrower than {@link GpuSortSupport}'s and for a different reason. A
 * sort refuses shapes a device would order <em>differently</em>; this refuses shapes that are a
 * different primitive altogether. A bounded {@code ROWS} frame is a sliding window, a {@code RANGE}
 * frame has to resolve peer groups, {@code NTILE} is not an accumulation — none of them is the scan
 * this node means, and calling them all "window aggregation" is what makes the ceiling look
 * reachable.
 */
public final class GpuOverAggregateSupport {

    /**
     * Additions one row costs, which for a running total is one.
     *
     * <p>Flat, unlike the sort's and the join's. A prefix sum does exactly one add per row however
     * many rows there are — that is the whole of why §T5 measured its arithmetic intensity at 0.13
     * operations per byte, the lowest of the four candidates. Charged as one rather than zero so
     * that the subtree is not read as having no work at all, and not inflated to make it look
     * better: the honest number here is one, and the cost model is what has to cope with that.
     */
    static final int ADDITIONS_PER_ROW = 1;

    /** Costs an {@code OVER} window, or says why it cannot be accumulated on a device. */
    public static RowCost estimate(
            OverSpec overSpec, RowType inputType, RowType outputType, long estimatedRows) {
        if (overSpec.getPartition().getFieldIndices().length != 0) {
            // A partitioned running total is a segmented scan, which is a different primitive and
            // one this project has neither bound nor measured.
            return RowCost.ineligible("a PARTITION BY window is not offloaded");
        }
        if (overSpec.getGroups().size() != 1) {
            return RowCost.ineligible(
                    overSpec.getGroups().size() + " window groups; only one is offloaded");
        }
        OverSpec.GroupSpec group = overSpec.getGroups().get(0);
        if (group.getAggCalls().size() != 1) {
            return RowCost.ineligible(
                    group.getAggCalls().size() + " aggregate calls in one window, not 1");
        }
        if (!group.isRows()) {
            // A RANGE frame includes every row with the same ORDER BY value as the current one, so
            // its result is not a prefix of the input order; a prefix sum cannot express it.
            return RowCost.ineligible("a RANGE frame is not offloaded; ROWS only");
        }
        if (!group.getLowerBound().isUnbounded() || !group.getUpperBound().isCurrentRow()) {
            return RowCost.ineligible(
                    "only ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW is offloaded");
        }
        AggregateCall call = group.getAggCalls().get(0);
        if (call.getAggregation().getKind() != SqlKind.SUM || call.getArgList().size() != 1) {
            return RowCost.ineligible(call.getAggregation() + " is not offloaded; SUM only");
        }
        int valueField = call.getArgList().get(0);
        if (valueField >= inputType.getFieldCount()) {
            // An aggregate over a constant rather than an input column: the over-aggregate widens
            // its input with the window's literals and the argument may index into that extension.
            return RowCost.ineligible("a window over a constant is not offloaded");
        }
        LogicalType valueType = inputType.getTypeAt(valueField);
        if (valueType.getTypeRoot() != LogicalTypeRoot.DOUBLE || valueType.isNullable()) {
            return RowCost.ineligible(
                    "a running total is offloaded over a DOUBLE NOT NULL, not " + valueType);
        }

        int[] ops = new int[AccelFunction.values().length];
        ops[AccelFunction.PLUS.ordinal()] = ADDITIONS_PER_ROW;
        int inWidth = rowWidth(inputType);
        // Carried even though this node needs no cardinality of its own: a subtree's profile
        // merges to UNKNOWN_ROWS if any member is unknown, and this node sits directly above the
        // Sort its own ORDER BY created -- which does need one.
        return RowCost.of(new AccelWorkProfile(ops, inWidth, rowWidth(outputType), estimatedRows));
    }

    /** Which input field the single {@code SUM} accumulates, when {@link #estimate} allowed it. */
    public static OptionalInt valueFieldOf(OverSpec overSpec) {
        if (overSpec.getGroups().size() != 1) {
            return OptionalInt.empty();
        }
        OverSpec.GroupSpec group = overSpec.getGroups().get(0);
        if (group.getAggCalls().size() != 1
                || group.getAggCalls().get(0).getArgList().size() != 1) {
            return OptionalInt.empty();
        }
        return OptionalInt.of(group.getAggCalls().get(0).getArgList().get(0));
    }

    /** Bytes a row occupies, for whoever is turning a memory budget into a row count. */
    private static int rowWidth(RowType type) {
        int width = 0;
        for (LogicalType field : type.getChildren()) {
            switch (field.getTypeRoot()) {
                case TINYINT:
                case SMALLINT:
                    width += 2;
                    break;
                case INTEGER:
                case FLOAT:
                case DATE:
                    width += 4;
                    break;
                case BIGINT:
                case DOUBLE:
                    width += 8;
                    break;
                default:
                    width += 16;
                    break;
            }
        }
        return Math.max(1, width);
    }

    private GpuOverAggregateSupport() {}
}
