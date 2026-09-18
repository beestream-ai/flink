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
import org.apache.flink.table.types.logical.RowType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Inner-joins two inputs on one equality. V6.
 *
 * <p>The first node here with two inputs, and that is the whole of what is new. {@link AccelSort}
 * already established that a node may be neither per-row nor partial; this adds the other axis, and
 * everything downstream of the IR — the factory, the fallback, the operator — has to grow a second
 * input to carry it.
 *
 * <h2>Build and probe, not left and right</h2>
 *
 * <p>{@link #inputs()} returns the <em>build</em> side first and the <em>probe</em> side second,
 * which is the order Flink's own join transformation puts them in and not necessarily the order the
 * query wrote them. Which of the two the planner chose to build is a costing decision made from
 * statistics, and a provider has no business second-guessing it: the build side is the one that has
 * to be resident, and this node says which that is by putting it first.
 *
 * <p>{@link #buildIsLeft()} is therefore about the <em>output</em> and nothing else. A join emits
 * the left row's fields followed by the right row's, in the order the query named them, so an
 * operator gathering a matched pair has to know which of its two inputs was written first.
 *
 * <h2>One equality, and no residual condition</h2>
 *
 * <p>For the same reason {@link AccelSort} carries one key: composing several means a comparator,
 * and a residual {@code ON} predicate means evaluating query-defined code over a <em>pair</em> of
 * rows. Both are things a device can be given only as generated code over a shape this IR does not
 * describe, so the planner declines them here rather than offering them and having them refused.
 *
 * <h2>Why a build cardinality is on the node</h2>
 *
 * <p>{@link AccelWorkProfile} holds one row count, and a join has two. The profile's is the probe
 * side, because that is what the per-row work is per; the build side's is here, because whether the
 * join can be served at all is a question about how much has to stay resident, and that is this
 * number rather than that one.
 */
@PublicEvolving
public final class AccelJoin extends AccelNode {

    private static final long serialVersionUID = 1L;

    /** {@link #estimatedBuildRows()} when the planner had no estimate. */
    public static final long UNKNOWN_ROWS = -1L;

    private final int buildKeyField;
    private final int probeKeyField;
    private final boolean buildIsLeft;
    private final long estimatedBuildRows;
    private final AccelNode build;
    private final AccelNode probe;
    private final RowType outputType;

    public AccelJoin(
            int buildKeyField,
            int probeKeyField,
            boolean buildIsLeft,
            long estimatedBuildRows,
            AccelNode build,
            AccelNode probe,
            RowType outputType) {
        this.buildKeyField = buildKeyField;
        this.probeKeyField = probeKeyField;
        this.buildIsLeft = buildIsLeft;
        this.estimatedBuildRows = estimatedBuildRows;
        this.build = build;
        this.probe = probe;
        this.outputType = outputType;
    }

    /** The field of the build row the equality is on. */
    public int buildKeyField() {
        return buildKeyField;
    }

    /** The field of the probe row the equality is on. */
    public int probeKeyField() {
        return probeKeyField;
    }

    /** Whether the build side is the query's left input, which decides the output field order. */
    public boolean buildIsLeft() {
        return buildIsLeft;
    }

    /** Rows the build side is expected to hold, or {@link #UNKNOWN_ROWS}. */
    public long estimatedBuildRows() {
        return estimatedBuildRows;
    }

    /** The build side first, the probe side second. */
    @Override
    public List<AccelNode> inputs() {
        return Collections.unmodifiableList(Arrays.asList(build, probe));
    }

    @Override
    public RowType outputType() {
        return outputType;
    }

    @Override
    public String toString() {
        return "Join(build.f"
                + buildKeyField
                + " = probe.f"
                + probeKeyField
                + ", build="
                + (buildIsLeft ? "left" : "right")
                + ", ~"
                + (estimatedBuildRows == UNKNOWN_ROWS ? "?" : estimatedBuildRows)
                + " build rows)";
    }
}
