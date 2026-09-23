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

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.groups.OperatorIOMetricGroup;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.accelerator.AccelFunction;
import org.apache.flink.table.accelerator.AccelInput;
import org.apache.flink.table.accelerator.AccelInputRef;
import org.apache.flink.table.accelerator.AccelProject;
import org.apache.flink.table.accelerator.AccelWorkProfile;
import org.apache.flink.table.accelerator.AcceleratorDeclineReason;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.gpu.GpuOrCpuCalcOperatorFactory;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.RowType;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The half of M3.2 that needs no provider: what a subtask publishes when nothing served it.
 *
 * <p>The accelerated half is asserted in the planner, against the M0.5 provider, where a provider
 * exists to serve anything. Here there is none on the classpath — which is not a limitation of the
 * test but the case under test, and the one a user is most likely to meet: a plan that says a node
 * is eligible, running on a machine that has nothing to run it on.
 *
 * <p>EXPLAIN cannot answer that, because it reports what the planner decided and the decision moved
 * to the TaskManager. Until this gauge existed the answer was a log line on the machine that made
 * it, which is how {@code GpuOffloadExample} came to announce that a query "ran on the GPU" while
 * the TaskManager log recorded a decline. Review finding E2.
 */
class AcceleratorDecisionMetricsTest {

    /** What was registered, by name. Static because the harness serialises the operator. */
    private static final Map<String, Object> PUBLISHED = new ConcurrentHashMap<>();

    /** Stands in for the code-generated operator, and lets the test see its metric group. */
    public static final class Probe extends AbstractStreamOperator<RowData>
            implements OneInputStreamOperator<RowData, RowData> {

        private static final long serialVersionUID = 1L;

        private transient CapturingGroup capturing;

        @Override
        public OperatorMetricGroup getMetricGroup() {
            if (capturing == null) {
                capturing = new CapturingGroup(super.getMetricGroup());
            }
            return capturing;
        }

        @Override
        public void processElement(StreamRecord<RowData> element) {}
    }

    private static final class CapturingGroup extends UnregisteredMetricsGroup
            implements OperatorMetricGroup {

        private final OperatorMetricGroup real;

        CapturingGroup(OperatorMetricGroup real) {
            this.real = real;
        }

        @Override
        public <T, G extends Gauge<T>> G gauge(String name, G gauge) {
            PUBLISHED.put(name, gauge.getValue());
            return real.gauge(name, gauge);
        }

        @Override
        public OperatorIOMetricGroup getIOMetricGroup() {
            // Delegated: AbstractStreamOperator counts records through this during setup.
            return real.getIOMetricGroup();
        }
    }

    @Test
    @DisplayName("with nothing to serve it, the gauge reads 0 and says why")
    void unacceleratedSubtaskReportsItself() throws Exception {
        PUBLISHED.clear();

        RowType row = RowType.of(new DoubleType(false));
        AcceleratedCalcSpec spec =
                new AcceleratedCalcSpec(
                        new AccelProject(
                                Collections.singletonList(
                                        new AccelInputRef(0, new DoubleType(false))),
                                new AccelInput(row),
                                row),
                        new AccelWorkProfile(new int[AccelFunction.values().length], 8, 8, 1000L),
                        row,
                        1024,
                        1.3);

        GpuOrCpuCalcOperatorFactory factory =
                new GpuOrCpuCalcOperatorFactory(spec, SimpleOperatorFactory.of(new Probe()));

        try (OneInputStreamOperatorTestHarness<RowData, RowData> harness =
                new OneInputStreamOperatorTestHarness<>(factory, 1, 1, 0)) {
            harness.setup();
            harness.open();
        }

        assertThat(PUBLISHED.get(GpuOrCpuCalcOperatorFactory.ACCELERATED))
                .as("published: %s", PUBLISHED)
                .isEqualTo(0);
        assertThat((String) PUBLISHED.get(GpuOrCpuCalcOperatorFactory.DECISION))
                .as("the reason travels with the verdict, or the gauge only raises questions")
                .contains("no accelerator provider");
        assertThat((String) PUBLISHED.get(GpuOrCpuCalcOperatorFactory.DECISION_CODE))
                .as("and the same verdict as something a dashboard can count")
                .isEqualTo(AcceleratorDeclineReason.NO_PROVIDER.name());
    }
}
