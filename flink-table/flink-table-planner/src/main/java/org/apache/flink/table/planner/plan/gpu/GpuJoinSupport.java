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
import org.apache.flink.table.planner.plan.nodes.exec.spec.JoinSpec;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

/**
 * Which joins a device could serve, and what serving one costs.
 *
 * <p>The same shape {@link GpuSortSupport} has and for the same reason: the planner asks so that a
 * node it cannot serve is reported as CPU in {@code EXPLAIN} rather than looking offloaded until
 * the job runs.
 */
public final class GpuJoinSupport {

    /**
     * Comparisons one probe row costs, as a stand-in for the probe itself.
     *
     * <p>A probe is a binary search into the sorted build side, so {@code log2(build)} comparisons
     * a probe row — the same reasoning {@link GpuSortSupport#comparisonsPerRow} uses, against the
     * other input's cardinality rather than its own. Charged because a join, like a sort, has no
     * expressions to count and would otherwise price as having no work at all.
     */
    static int comparisonsPerRow(long buildRows) {
        if (buildRows <= 1) {
            return 1;
        }
        return Math.max(1, 64 - Long.numberOfLeadingZeros(buildRows - 1));
    }

    /**
     * Costs a join, or says why it cannot be served on a device.
     *
     * <p>Each refusal is a shape where a device would give a different answer rather than a slower
     * one, or one the IR cannot describe at all.
     */
    public static RowCost estimate(
            JoinSpec joinSpec,
            RowType buildType,
            RowType probeType,
            RowType outputType,
            boolean leftIsBuild,
            boolean isBroadcast,
            long buildRows,
            long probeRows) {

        if (isBroadcast) {
            // M5.0's decision, not a limitation: a 1.47x ceiling is not worth a second code path,
            // and the shuffle join is where the headroom measured.
            return RowCost.ineligible("a broadcast join is out of scope");
        }
        if (joinSpec.getJoinType() != FlinkJoinType.INNER) {
            // An outer join has to emit rows that matched nothing, which is not a per-row map over
            // the matches: it needs the complement of the match set, and the IR says nothing about
            // one.
            return RowCost.ineligible(joinSpec.getJoinType() + " is not offloaded; INNER only");
        }
        if (joinSpec.getNonEquiCondition().isPresent()) {
            // A residual ON predicate is query-defined code over a pair of rows, which is the one
            // thing neither a library primitive nor this IR can take.
            return RowCost.ineligible("a join with a residual condition is not offloaded");
        }
        if (joinSpec.getLeftKeys().length != 1) {
            return RowCost.ineligible(
                    "only a single equality is offloaded; "
                            + joinSpec.getLeftKeys().length
                            + " keys need a composite comparator");
        }
        LogicalType buildKey = buildType.getTypeAt(buildKeyOf(joinSpec, leftIsBuild));
        LogicalType probeKey = probeType.getTypeAt(probeKeyOf(joinSpec, leftIsBuild));
        if (!isJoinableOnDevice(buildKey) || !isJoinableOnDevice(probeKey)) {
            return RowCost.ineligible(
                    "a join key of type " + buildKey + " / " + probeKey + " is not joined on device");
        }
        if (buildKey.isNullable() || probeKey.isNullable()) {
            // SQL's equality is not true for two nulls, so a null key matches nothing; a device
            // that has no null mask cannot tell, and would match them to each other. NOT NULL in
            // the DDL is the sanctioned answer, as it is everywhere else here.
            return RowCost.ineligible("a nullable join key is not offloaded");
        }

        int[] ops = new int[AccelFunction.values().length];
        ops[AccelFunction.LESS_THAN.ordinal()] = comparisonsPerRow(buildRows);
        return RowCost.of(
                new AccelWorkProfile(
                        ops, rowWidth(probeType), rowWidth(outputType), probeRows));
    }

    /**
     * Which field of the build row the single equality names.
     *
     * <p>{@code JoinSpec} speaks left and right, the transformation speaks build and probe, and
     * only the exec node knows which is which — so it says, rather than this inferring it from
     * row types that may well be identical.
     */
    public static int buildKeyOf(JoinSpec joinSpec, boolean leftIsBuild) {
        return leftIsBuild ? joinSpec.getLeftKeys()[0] : joinSpec.getRightKeys()[0];
    }

    /** Which field of the probe row the single equality names. */
    public static int probeKeyOf(JoinSpec joinSpec, boolean leftIsBuild) {
        return leftIsBuild ? joinSpec.getRightKeys()[0] : joinSpec.getLeftKeys()[0];
    }

    /**
     * Whether a device can equate this key type.
     *
     * <p>Integers only, and that is the binding rather than the device: the shim's join and its
     * ordering both take an {@code INT32} column. Widening it is more shim.
     */
    private static boolean isJoinableOnDevice(LogicalType type) {
        return type.getTypeRoot() == LogicalTypeRoot.INTEGER;
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

    private GpuJoinSupport() {}
}
