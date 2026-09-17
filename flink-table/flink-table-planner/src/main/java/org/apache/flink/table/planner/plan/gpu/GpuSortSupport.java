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
import org.apache.flink.table.planner.plan.nodes.exec.spec.SortSpec;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

/**
 * Which sorts a device could order, and what ordering one costs.
 *
 * <p>Separate from {@code BatchExecSort} for the same reason {@link GpuAggregateSupport} is
 * separate from the aggregate: the planner asks so a node it cannot serve is reported as CPU in
 * {@code EXPLAIN} rather than looking offloaded until the job runs, and the answer depends on
 * nothing but the spec.
 */
public final class GpuSortSupport {

    /**
     * Comparisons a row costs, as a stand-in for the ordering itself.
     *
     * <p>A sort is the first operator here whose work is not arithmetic, and the cost model counts
     * arithmetic. Pricing it at zero — which is what counting its expressions gives, since it has
     * none — would make every sort look free and be declined by whatever compares the estimate
     * against a floor. So the estimate is the honest one for a comparison sort: {@code n log n}
     * comparisons over {@code n} rows is {@code log2(n)} comparisons a row.
     *
     * <p>Approximate on purpose. What it has to get right is the <em>shape</em> — that a sort's
     * cost per row grows with the input rather than being fixed — because a provider reading this
     * profile is being asked whether ordering this many rows is worth a device, and at a thousand
     * rows it is not while at forty million it may be.
     */
    static int comparisonsPerRow(long rows) {
        if (rows <= 1) {
            // Includes the unknown case, which is the common one: a Sort sitting above an Exchange
            // often has no row estimate captured. Zero would be the wrong answer to record for it
            // -- the subtree decision reads zero weighted operations as "no arithmetic to offload"
            // and declines before any provider is asked. One comparison a row is the floor a sort
            // of any input costs, so it is both true and the least this can claim; a provider with
            // a real row count will price the work properly on the machine that has one.
            return 1;
        }
        return Math.max(1, 64 - Long.numberOfLeadingZeros(rows - 1));
    }

    /**
     * Costs a sort, or says why it cannot be ordered on a device.
     *
     * <p>The refusals are narrow and each is a case where a device would give a different order
     * rather than a slower one — which for a sort is a wrong answer, not a degraded one.
     */
    public static RowCost estimate(SortSpec sortSpec, RowType inputType, long estimatedRows) {
        if (sortSpec.getFieldSize() == 0) {
            return RowCost.ineligible("a sort with no keys orders nothing");
        }
        if (sortSpec.getFieldSize() > 1) {
            // Composing keys means a comparator, and a comparator means query-defined code per
            // comparison -- which is exactly what a library primitive cannot take. A provider
            // would decline it, so the planner does not offer it.
            return RowCost.ineligible(
                    "only a single sort key is offloaded; "
                            + sortSpec.getFieldSize()
                            + " keys need a comparator");
        }
        LogicalType keyType = inputType.getTypeAt(sortSpec.getFieldSpec(0).getFieldIndex());
        if (!isOrderableOnDevice(keyType)) {
            return RowCost.ineligible(
                    "a sort key of type " + keyType + " is not ordered on device");
        }
        int[] ops = new int[AccelFunction.values().length];
        // Charged as comparisons rather than as any particular function: there is no expression
        // here to attribute them to, and LESS_THAN is what a comparison is.
        ops[AccelFunction.LESS_THAN.ordinal()] = comparisonsPerRow(estimatedRows);
        int width = rowWidth(inputType);
        return RowCost.of(new AccelWorkProfile(ops, width, width, estimatedRows));
    }

    /**
     * Whether a device can order this key type.
     *
     * <p>Integers only for now, and that is a property of the binding rather than of devices: cuDF
     * orders far more than this, and the shim exposes one entry point taking an {@code INT32}
     * column. Widening it is more shim, not a different design.
     */
    private static boolean isOrderableOnDevice(LogicalType type) {
        LogicalTypeRoot root = type.getTypeRoot();
        return root == LogicalTypeRoot.INTEGER;
    }

    /** Bytes a row occupies, for whoever is turning a memory budget into a row count. */
    private static int rowWidth(RowType inputType) {
        int width = 0;
        for (LogicalType field : inputType.getChildren()) {
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
                    // Anything variable-width is counted at a nominal fixed cost. The figure only
                    // has to be honest enough to stop a provider believing a wide row is cheap;
                    // the provider makes the sizing decision with what it actually has.
                    width += 16;
                    break;
            }
        }
        return Math.max(1, width);
    }

    private GpuSortSupport() {}
}
