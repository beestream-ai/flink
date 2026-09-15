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

import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.table.accelerator.AccelFunction;
import org.apache.flink.table.accelerator.AccelInput;
import org.apache.flink.table.accelerator.AccelIrVersion;
import org.apache.flink.table.accelerator.AccelNode;
import org.apache.flink.table.accelerator.AccelWorkProfile;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.RowType;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** The decision the planner is no longer allowed to make. */
class AcceleratorChoiceTest {

    private static final double MIN_SPEEDUP = 1.3;

    private static final AccelNode SUBTREE =
            new AccelInput(RowType.of(new DoubleType(false), new DoubleType(false)));

    private static AccelWorkProfile work(int sqrtCount) {
        int[] counts = new int[AccelFunction.values().length];
        counts[AccelFunction.SQRT.ordinal()] = sqrtCount;
        return new AccelWorkProfile(counts, 16, 8, 2_000_000L);
    }

    @Test
    @DisplayName("a fast device wins")
    void fastDeviceWins() {
        AcceleratorChoice.Outcome outcome =
                AcceleratorChoice.choose(
                        provider(new AcceleratorCost(11.4, 20_000_000L, 900_000L)),
                        SUBTREE,
                        work(20),
                        MIN_SPEEDUP,
                        2_000_000L);

        assertThat(outcome.accelerated()).isTrue();
        assertThat(outcome.reason()).contains("11.40x");
    }

    @Test
    @DisplayName("a slow device loses, and the reason names both numbers")
    void slowDeviceLoses() {
        // 0.54x is the measured figure for val * 2.0 + 1.0. The planner now lets this query
        // through; this is where it is stopped, by a provider that can see the hardware.
        AcceleratorChoice.Outcome outcome =
                AcceleratorChoice.choose(
                        provider(new AcceleratorCost(0.54, 1L, 1L)),
                        SUBTREE,
                        work(1),
                        MIN_SPEEDUP,
                        2_000_000L);

        assertThat(outcome.accelerated()).isFalse();
        assertThat(outcome.reason()).contains("0.54x");
        assertThat(outcome.reason()).contains("1.30x");
    }

    @Test
    @DisplayName("a device that is faster but not enough faster loses")
    void marginallyFasterLoses() {
        AcceleratorChoice.Outcome outcome =
                AcceleratorChoice.choose(
                        provider(new AcceleratorCost(1.1, 1L, 1L)),
                        SUBTREE,
                        work(5),
                        MIN_SPEEDUP,
                        2_000_000L);

        assertThat(outcome.accelerated()).isFalse();
    }

    @Test
    @DisplayName("a declining provider is an ordinary answer")
    void decliningIsOrdinary() {
        AcceleratorChoice.Outcome outcome =
                AcceleratorChoice.choose(
                        provider(null), SUBTREE, work(20), MIN_SPEEDUP, 2_000_000L);

        assertThat(outcome.accelerated()).isFalse();
        assertThat(outcome.reason()).contains("declined");
    }

    @Test
    @DisplayName("a provider that throws does not take the task with it")
    void throwingProviderFallsBack() {
        AcceleratorProvider broken =
                new StubProvider(null) {
                    @Override
                    public Optional<AcceleratorPlan> accept(
                            AccelNode subtree, AccelWorkProfile work) {
                        throw new UnsatisfiedLinkError("libaccel.so not found");
                    }
                };

        AcceleratorChoice.Outcome outcome =
                AcceleratorChoice.choose(broken, SUBTREE, work(20), MIN_SPEEDUP, 2_000_000L);

        assertThat(outcome.accelerated()).isFalse();
        assertThat(outcome.reason()).contains("threw");
    }

    @Test
    @DisplayName("a fast device still loses when too few rows repay its setup")
    void tooFewRowsToRepaySetup() {
        AcceleratorChoice.Outcome outcome =
                AcceleratorChoice.choose(
                        provider(new AcceleratorCost(50.0, 20_000_000L, 0L)),
                        SUBTREE,
                        work(1),
                        MIN_SPEEDUP,
                        10L);

        assertThat(outcome.accelerated()).isFalse();
        assertThat(outcome.reason()).contains("do not repay");
    }

    @Test
    @DisplayName("an unknown row count does not refuse: that would disable the feature silently")
    void unknownRowsDoesNotRefuse() {
        AcceleratorChoice.Outcome outcome =
                AcceleratorChoice.choose(
                        provider(new AcceleratorCost(50.0, 20_000_000L, 0L)),
                        SUBTREE,
                        work(1),
                        MIN_SPEEDUP,
                        AccelWorkProfile.UNKNOWN_ROWS);

        assertThat(outcome.accelerated()).isTrue();
    }

    // ------------------------------------------------------------------

    private static AcceleratorProvider provider(AcceleratorCost cost) {
        return new StubProvider(cost);
    }

    private static class StubProvider implements AcceleratorProvider {
        private final AcceleratorCost cost;

        StubProvider(AcceleratorCost cost) {
            this.cost = cost;
        }

        @Override
        public String name() {
            return "stub";
        }

        @Override
        public int supportedIrVersion() {
            return AccelIrVersion.CURRENT;
        }

        @Override
        public Optional<AcceleratorPlan> accept(AccelNode subtree, AccelWorkProfile work) {
            if (cost == null) {
                return Optional.empty();
            }
            return Optional.of(
                    new AcceleratorPlan() {
                        private static final long serialVersionUID = 1L;

                        @Override
                        public AcceleratorCost cost() {
                            return cost;
                        }

                        @Override
                        public Serializable payload() {
                            return "compiled";
                        }
                    });
        }

        @Override
        public StreamOperatorFactory<RowData> createOperator(
                AcceleratorPlan plan, AcceleratorContext context) {
            throw new UnsupportedOperationException();
        }
    }
}
