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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The shape of the accelerator SPI: what a provider is asked, what it may answer, and what Flink
 * does when there is no provider at all.
 *
 * <p>Registration through {@code META-INF/services} is exercised by the test provider of task M0.5;
 * this covers the contract itself.
 */
class AcceleratorSpiTest {

    private static final RowType ROW = RowType.of(new DoubleType(false), new DoubleType(false));

    private static AccelNode subtree() {
        return new AccelInput(ROW);
    }

    private static AccelWorkProfile work() {
        int[] counts = new int[AccelFunction.values().length];
        counts[AccelFunction.SQRT.ordinal()] = 20;
        counts[AccelFunction.TIMES.ordinal()] = 80;
        return new AccelWorkProfile(counts, 16, 8, 2_000_000L);
    }

    @Test
    @DisplayName("no provider on the classpath is an ordinary answer, not an error")
    void absenceIsSupported() {
        assertThat(AcceleratorProviders.find(getClass().getClassLoader())).isEmpty();
    }

    @Test
    @DisplayName("a provider may decline, and declining is not a failure")
    void decliningIsSupported() {
        AcceleratorProvider declining = new StubProvider(null);

        assertThat(declining.accept(subtree(), work())).isEmpty();
    }

    @Test
    @DisplayName("a provider answers with a cost, never with a verdict")
    void answersWithCost() {
        AcceleratorCost claimed = new AcceleratorCost(11.4, 20_000_000L, 900_000L);
        AcceleratorProvider accepting = new StubProvider(claimed);

        Optional<AcceleratorPlan> plan = accepting.accept(subtree(), work());

        assertThat(plan).isPresent();
        assertThat(plan.get().cost().speedupOverCpu()).isEqualTo(11.4);
        assertThat(plan.get().cost().fixedCostNanos()).isEqualTo(20_000_000L);
        // Nothing on the plan says "run me". That judgement belongs to Flink (task M0.4).
        assertThat(AcceleratorPlan.class.getMethods())
                .noneMatch(m -> m.getName().toLowerCase().contains("worth"));
    }

    @Test
    @DisplayName("a provider claiming to be slower than the CPU is a legal answer")
    void mayClaimBelowOne() {
        AcceleratorCost slower = new AcceleratorCost(0.54, 20_000_000L, 900_000L);

        assertThat(slower.speedupOverCpu()).isLessThan(1.0);
    }

    @Test
    @DisplayName("a nonsensical cost is rejected at construction")
    void rejectsNonsense() {
        assertThatThrownBy(() -> new AcceleratorCost(0.0, 0L, 0L))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new AcceleratorCost(Double.POSITIVE_INFINITY, 0L, 0L))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new AcceleratorCost(2.0, -1L, 0L))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName(
            "the plan is serializable: it is built on the TaskManager but may be logged, "
                    + "cached and compared")
    void planIsSerializable() {
        AcceleratorPlan plan = new StubPlan(new AcceleratorCost(2.0, 1L, 1L), "payload");

        assertThat(plan).isInstanceOf(Serializable.class);
        assertThat(plan.payload()).isInstanceOf(Serializable.class);
    }

    @Test
    @DisplayName("the SPI names no accelerator framework")
    void spiIsVendorNeutral() {
        for (Class<?> type :
                new Class<?>[] {
                    AcceleratorProvider.class,
                    AcceleratorPlan.class,
                    AcceleratorCost.class,
                    AcceleratorContext.class
                }) {
            for (java.lang.reflect.Method method : type.getMethods()) {
                assertThat(method.getReturnType().getName())
                        .as(type.getSimpleName() + "." + method.getName())
                        .doesNotContain("tornado")
                        .doesNotContain("cuda")
                        .doesNotContain("manchester");
                for (Class<?> parameter : method.getParameterTypes()) {
                    assertThat(parameter.getName())
                            .as(type.getSimpleName() + "." + method.getName())
                            .doesNotContain("tornado")
                            .doesNotContain("cuda")
                            .doesNotContain("manchester");
                }
            }
        }
    }

    // ------------------------------------------------------------------

    private static final class StubPlan implements AcceleratorPlan {
        private static final long serialVersionUID = 1L;
        private final AcceleratorCost cost;
        private final Serializable payload;

        StubPlan(AcceleratorCost cost, Serializable payload) {
            this.cost = cost;
            this.payload = payload;
        }

        @Override
        public AcceleratorCost cost() {
            return cost;
        }

        @Override
        public Serializable payload() {
            return payload;
        }
    }

    /** Accepts with the given cost, or declines when it is null. */
    private static final class StubProvider implements AcceleratorProvider {
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
            return cost == null ? Optional.empty() : Optional.of(new StubPlan(cost, "compiled"));
        }

        @Override
        public StreamOperatorFactory<RowData> createOperator(
                AcceleratorPlan plan, AcceleratorContext context) {
            throw new UnsupportedOperationException("not needed for a contract test");
        }
    }
}
