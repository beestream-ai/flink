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
import org.apache.flink.table.accelerator.AcceleratorDeclineReason;

import java.util.Optional;

/**
 * Decides whether to run a subtree on an accelerator. Runs on the TaskManager, once per subtask.
 *
 * <h2>Why here and not in the planner</h2>
 *
 * <p>This is the first point at which the device is observable. The planner runs in the client and
 * cannot know which machine the scheduler chose, so the floors it used to apply — 96 weighted
 * operations per row, ten million in total — were predictions about hardware it could not see, and
 * calibrated against one particular card besides.
 *
 * <p>The planner therefore ships what the work <em>is</em>, counted per operation. A provider here
 * says what its device would charge for it. This class compares the two.
 *
 * <p>Because it runs per subtask, two subtasks of one job may decide differently. In a
 * heterogeneous cluster that is the correct outcome, not an inconsistency.
 */
@Internal
public final class AcceleratorChoice {

    private AcceleratorChoice() {}

    /** Why the accelerator was or was not chosen. Carried into the log and, later, into metrics. */
    public static final class Outcome {

        private final Optional<AcceleratorPlan> plan;
        private final AcceleratorDeclineReason code;
        private final String reason;

        private Outcome(
                Optional<AcceleratorPlan> plan, AcceleratorDeclineReason code, String reason) {
            this.plan = plan;
            this.code = code;
            this.reason = reason;
        }

        public boolean accelerated() {
            return plan.isPresent();
        }

        /** Present only when {@link #accelerated()}. */
        public Optional<AcceleratorPlan> plan() {
            return plan;
        }

        public String reason() {
            return reason;
        }

        /**
         * The same verdict as a value, for whatever has to count these rather than read one.
         *
         * <p>{@link #reason()} carries the numbers and the provider's name; this carries only what
         * kind of answer it was, which is the part a metric can aggregate.
         */
        public AcceleratorDeclineReason code() {
            return code;
        }

        @Override
        public String toString() {
            return (accelerated() ? "accelerator" : "cpu") + ": " + reason;
        }
    }

    /**
     * Asks the provider, then applies Flink's policy to its answer.
     *
     * @param provider the provider found on this TaskManager
     * @param subtree what to run
     * @param work what it does per row, counted
     * @param minSpeedup how much faster than the CPU a provider must claim to be before Flink
     *     prefers it. A <em>policy</em> constant — how much margin Flink wants before taking an
     *     unfamiliar execution path — not a device constant. It means the same thing on every
     *     accelerator, which is exactly what the floors it replaces did not. <b>Zero disables the
     *     profitability gate</b>: the provider's cost estimate is not consulted at all, and its
     *     expressibility refusals are the only thing that declines a subtree.
     * @param estimatedRows the planner's cardinality estimate, or {@link
     *     AccelWorkProfile#UNKNOWN_ROWS}
     */
    public static Outcome choose(
            AcceleratorProvider provider,
            AccelNode subtree,
            AccelWorkProfile work,
            double minSpeedup,
            long estimatedRows) {

        final Optional<AcceleratorPlan> offered;
        try {
            offered = provider.accept(subtree, work);
        } catch (Throwable declined) {
            // The SPI forbids throwing, but a provider is third-party code and a task that has a
            // perfectly good CPU implementation must not die because one misbehaved.
            return cpu(
                    AcceleratorDeclineReason.PROVIDER_THREW,
                    "provider " + provider.name() + " threw (" + declined + ")");
        }
        if (offered == null || !offered.isPresent()) {
            return cpu(
                    AcceleratorDeclineReason.PROVIDER_DECLINED,
                    "provider " + provider.name() + " declined the subtree");
        }

        AcceleratorCost cost = offered.get().cost();

        // The profitability gate, off by default.
        //
        // Both checks below rest on the provider's own cost constants, and a provider's constants
        // are fixed when it is built: nanoseconds per operation, per byte transferred, and for
        // compiling one kernel, all measured on whatever machine the author had. They do not
        // survive a change of device. The same jar on a laptop and on an A100 server prices the
        // identical query identically, and is wrong in both directions at once -- FP64 alone
        // differs by more than an order of magnitude between those two cards, so a model
        // calibrated on the laptop declines work the server would win comfortably.
        //
        // Until a provider measures its own rates on the machine it is running on, refusing on
        // those numbers refuses on the author's hardware rather than the operator's. The two
        // errors are not symmetric: a wrong decline silently discards the whole speed-up, which
        // has been measured here at 3.5x to 7x for one over-conservative floor, while the worst
        // measured wrong offload was about 2.1x slower than a vectorised CPU core. Neither is a
        // correctness question -- the answer is identical either way.
        //
        // Zero means the gate is off. What it does NOT disable is anything a provider refuses
        // because it cannot express an operator: those refusals are about what a device can
        // compute, not about what it is worth, and they are the provider's to make.
        if (minSpeedup <= 0.0) {
            return new Outcome(
                    offered,
                    AcceleratorDeclineReason.ACCELERATED,
                    String.format(
                            "provider %s accepted the subtree; the profitability gate is off"
                                    + " (%s = 0)",
                            provider.name(), "table.exec.accelerator.min-speedup"));
        }

        if (cost.speedupOverCpu() < minSpeedup) {
            return cpu(
                    AcceleratorDeclineReason.BELOW_MIN_SPEEDUP,
                    String.format(
                            "provider %s claims %.2fx, below the %.2fx Flink requires",
                            provider.name(), cost.speedupOverCpu(), minSpeedup));
        }
        if (!repaysFixedCost(cost, work, estimatedRows)) {
            return cpu(
                    AcceleratorDeclineReason.FIXED_COST_NOT_REPAID,
                    String.format(
                            "provider %s claims %.2fx, but %d rows do not repay %d ms of setup",
                            provider.name(),
                            cost.speedupOverCpu(),
                            estimatedRows,
                            cost.fixedCostNanos() / 1_000_000));
        }
        return new Outcome(
                offered,
                AcceleratorDeclineReason.ACCELERATED,
                String.format(
                        "provider %s claims %.2fx over CPU",
                        provider.name(), cost.speedupOverCpu()));
    }

    /**
     * Whether there are enough rows to earn back what the provider charges before the first one.
     *
     * <p>Deliberately crude, and deliberately generous. The row count is the planner's estimate,
     * which the adaptive scheduler may contradict — revisiting it where the produced bytes are
     * actually known is task M4.3. An unknown count does not refuse: refusing on no evidence would
     * silently disable the feature wherever the planner has no estimate.
     */
    private static boolean repaysFixedCost(
            AcceleratorCost cost, AccelWorkProfile work, long estimatedRows) {
        if (estimatedRows == AccelWorkProfile.UNKNOWN_ROWS || cost.fixedCostNanos() == 0) {
            return true;
        }
        // The provider's own measured rate where it has one, and the stand-in otherwise.
        //
        // The stand-in is one nanosecond per counted operation per row, used only to ask whether
        // the saving is of the same order as the setup. It is not a calibration, and for anything
        // but arithmetic it is not even the right order: a sort's counted operations are
        // comparisons inside a key-normalising, spilling sorter, which measure about twenty
        // nanoseconds each rather than one. Priced at the stand-in, a 4M-row sort claims to save
        // 76 ms against 300 ms of setup and is refused -- while the operator it was refused in
        // favour of takes 1.9 seconds.
        double cpuNanosPerRow =
                cost.cpuNanosPerRow() > 0.0
                        ? cost.cpuNanosPerRow()
                        : Math.max(1, work.totalOpsPerRow());
        double cpuNanos = (double) estimatedRows * cpuNanosPerRow;
        double saved = cpuNanos * (1.0 - 1.0 / cost.speedupOverCpu());
        return saved > cost.fixedCostNanos();
    }

    private static Outcome cpu(AcceleratorDeclineReason code, String reason) {
        return new Outcome(Optional.empty(), code, reason);
    }

    /** The verdicts made before a provider is even asked, so that they carry a code too. */
    public static Outcome declined(AcceleratorDeclineReason code, String reason) {
        return cpu(code, reason);
    }
}
