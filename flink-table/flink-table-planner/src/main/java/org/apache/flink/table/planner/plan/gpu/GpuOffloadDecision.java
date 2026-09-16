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

import org.apache.flink.table.accelerator.AccelWorkProfile;

import org.apache.calcite.rex.RexNode;

import javax.annotation.Nullable;

import java.util.List;

/**
 * The eligibility gate: can this subtree be represented for an accelerator, and how much work does
 * it do per row.
 *
 * <h2>What this class no longer does</h2>
 *
 * <p>It used to decide whether offloading would <em>pay</em>, by comparing a weighted score against
 * two floors — {@code min-row-cost} defaulting to 96, and {@code min-total-work} defaulting to ten
 * million — both derived from measurements on one RTX 4070 over PCIe 4.
 *
 * <p>Those numbers were a model of one device living inside Flink. They could not be right for a
 * different card, let alone an FPGA, and recalibrating them meant changing Flink. Worse, the
 * planner runs in the client: it cannot know which machine the scheduler will choose, so it was
 * predicting the behaviour of hardware it could not see.
 *
 * <p>So the judgement moved to where the hardware is. Flink now says what the work <em>is</em> —
 * counted per operation, in {@link AccelWorkProfile} — and a provider on the TaskManager says what
 * its device would charge for it. Expressibility stays here, because that is a property of the
 * query and knowable from the plan alone.
 */
public final class GpuOffloadDecision {

    /** Master switch. Off by default: this is opt-in. */
    public static final String ENABLED_KEY = "table.exec.accelerator.enabled";

    /**
     * Outcome of the gate. {@code reason} is always populated, including when eligible, so that
     * EXPLAIN can report why a node was or was not selected.
     *
     * <p>Written out longhand rather than as a record because flink-table-planner compiles at
     * {@code -source 11}.
     */
    public static final class Verdict {
        private final boolean offload;
        private final @Nullable AccelWorkProfile work;
        private final String reason;

        Verdict(boolean offload, @Nullable AccelWorkProfile work, String reason) {
            this.offload = offload;
            this.work = work;
            this.reason = reason;
        }

        /**
         * Whether this subtree is <em>eligible</em>. Not whether it will run on a device: that is
         * settled per subtask, on the TaskManager, against a provider that can see the hardware.
         */
        public boolean offload() {
            return offload;
        }

        /** The work per row, or null when ineligible. */
        public @Nullable AccelWorkProfile work() {
            return work;
        }

        /** Total operations per row; 0 when ineligible. Convenience for logs and EXPLAIN. */
        public int opsPerRow() {
            return work == null ? 0 : work.totalOpsPerRow();
        }

        public String reason() {
            return reason;
        }

        @Override
        public String toString() {
            return (offload ? "eligible" : "cpu") + "[" + reason + "]";
        }
    }

    /** Decides for one node's expressions. */
    public Verdict decide(List<? extends RexNode> expressions) {
        return verdict(GpuCostEstimator.estimateAll(expressions));
    }

    /**
     * Decides for a whole matched subtree by combining its nodes' work.
     *
     * <p>Grouping is still worth doing, and for the reason it always was: one task graph instead of
     * one per node, with intermediates never returning to the host. What changed is that grouping
     * no longer exists to push a score over a threshold.
     */
    public Verdict forSubtree(List<RowCost> nodeCosts) {
        RowCost total = RowCost.ZERO;
        for (RowCost cost : nodeCosts) {
            total = total.plus(cost);
        }
        return verdict(total);
    }

    /**
     * Turns a selected verdict into a fallback one, keeping the work so EXPLAIN still shows what
     * the node was worth alongside why it did not run on an accelerator.
     */
    public static Verdict fallback(Verdict selected, String reason) {
        return new Verdict(false, selected.work(), "selected but fell back: " + reason);
    }

    private Verdict verdict(RowCost cost) {
        if (!cost.isEligible()) {
            return new Verdict(false, null, "not expressible on device: " + cost.rejection());
        }
        AccelWorkProfile work = cost.work();
        if (work == null || work.totalOpsPerRow() == 0) {
            // Pure projection: nothing to compute, so nothing to move.
            return new Verdict(false, work, "no arithmetic to offload");
        }
        return new Verdict(
                true,
                work,
                "eligible: "
                        + work
                        + "; whether it pays is decided on the TaskManager, against the device");
    }
}
