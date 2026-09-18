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

package org.apache.flink.table.runtime.accelerator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.accelerator.AccelNode;
import org.apache.flink.table.accelerator.AccelWorkProfile;
import org.apache.flink.table.types.logical.RowType;

import java.io.Serializable;

/**
 * What travels in the JobGraph for an offloaded join, alongside the operator Flink generated.
 *
 * <p>The same five things {@link AcceleratedCalcSpec} carries, and a separate type rather than a
 * reuse of it. The fields happening to coincide is not a reason to share: the name is what a reader
 * meets first, and a join arriving in something called a Calc spec would be the sort of small lie
 * that costs an afternoon. Both are held by {@link AcceleratorPlan}-carrying factories that do
 * genuinely different things with them — one builds a one-input operator and one a two-input one —
 * and that difference is the whole of M5.5.
 *
 * <p>Everything the two inputs differ in is already in the subtree: {@code AccelJoin} names both
 * row types, both key fields, and which side the build is. Nothing about the second input needs to
 * be repeated here, and repeating it would give two places to disagree.
 */
@Internal
public final class AcceleratedJoinSpec implements Serializable {

    private static final long serialVersionUID = 1L;

    private final AccelNode subtree;
    private final AccelWorkProfile work;
    private final RowType outputType;
    private final int maxBatchSize;
    private final double minSpeedup;

    public AcceleratedJoinSpec(
            AccelNode subtree,
            AccelWorkProfile work,
            RowType outputType,
            int maxBatchSize,
            double minSpeedup) {
        this.subtree = subtree;
        this.work = work;
        this.outputType = outputType;
        this.maxBatchSize = maxBatchSize;
        this.minSpeedup = minSpeedup;
    }

    /** The {@code AccelJoin} offered to a provider. */
    public AccelNode subtree() {
        return subtree;
    }

    /**
     * What the probe costs per row, and how many probe rows there are.
     *
     * <p>The probe side, not the build side: the profile describes per-row work, and the join's
     * per-row work is done once per probe row. The build side's cardinality is on the node.
     */
    public AccelWorkProfile work() {
        return work;
    }

    /** The joined row, left fields then right fields, in the order the query named them. */
    public RowType outputType() {
        return outputType;
    }

    /** The configured ceiling on a probe batch, from {@code table.exec.accelerator.batch-size}. */
    public int maxBatchSize() {
        return maxBatchSize;
    }

    /** What Flink requires a provider to claim before preferring it. */
    public double minSpeedup() {
        return minSpeedup;
    }

    @Override
    public String toString() {
        return "AcceleratedJoinSpec[" + subtree + ", " + work + ", batch=" + maxBatchSize + "]";
    }
}
