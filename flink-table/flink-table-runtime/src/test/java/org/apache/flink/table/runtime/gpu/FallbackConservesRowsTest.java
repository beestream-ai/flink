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

package org.apache.flink.table.runtime.gpu;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.accelerator.AcceleratorDeclineReason;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What happens to the rows when a device gives up: every one exactly once, or the fallback is a
 * lie.
 *
 * <p>The promise the fallback makes is that a device failing early costs the query nothing. That is
 * only true if the switch conserves rows. Two ways it could fail silently, and both would produce a
 * job that succeeds with a wrong answer rather than one that stops:
 *
 * <ul>
 *   <li><b>loss</b> — the record in flight when the device failed is dropped, and the query returns
 *       one row fewer than it should, which nothing downstream can detect;
 *   <li><b>duplication</b> — the record is handed to the accelerated operator, fails, and is then
 *       handed to the generated one as well, so it is counted twice.
 * </ul>
 *
 * <p>These drive the real {@link FallbackToCpuOperator} through the real switch, with an
 * "accelerated" operator that fails at each of the two points the contract says are recoverable.
 * Anything after those points is deliberately not covered: the contract is explicit that a failure
 * once rows have been consumed is fatal rather than recovered, and a test pretending otherwise
 * would be asserting a promise the code does not make.
 */
class FallbackConservesRowsTest {

    private static final int ROWS = 50;

    /** Collects everything the chain emits, so loss and duplication are both visible. */
    private static final class Collecting extends AbstractStreamOperator<RowData>
            implements OneInputStreamOperator<RowData, RowData> {

        private static final long serialVersionUID = 1L;

        private final List<Integer> seen = new ArrayList<>();

        /**
         * Empty, because {@code AbstractStreamOperator.open()} reads the containing task's
         * configuration and there is no task here. The switch is what is under test, not the
         * lifecycle, and the same stand-in is used by {@link FallbackReleasesStagingTest}.
         */
        @Override
        public void open() {}

        @Override
        public void processElement(StreamRecord<RowData> element) {
            seen.add(element.getValue().getInt(0));
        }
    }

    /** Fails at a chosen point, as a device that is not there would. */
    private static final class FailingAccelerated extends AbstractStreamOperator<RowData>
            implements OneInputStreamOperator<RowData, RowData> {

        private static final long serialVersionUID = 1L;

        private final boolean failOnOpen;
        private boolean failedOnce;

        FailingAccelerated(boolean failOnOpen) {
            this.failOnOpen = failOnOpen;
        }

        @Override
        public void open() throws Exception {
            if (failOnOpen) {
                throw new IllegalStateException("no device here");
            }
        }

        @Override
        public void processElement(StreamRecord<RowData> element) {
            if (!failedOnce) {
                failedOnce = true;
                // The first record, which the contract says is still recoverable because the
                // record is in hand and nothing has been emitted.
                throw new IllegalStateException("the device failed on its first record");
            }
            throw new IllegalStateException("should not be reached after the switch");
        }
    }

    @Test
    @DisplayName("a device that fails at open() loses and duplicates nothing")
    void openFailureConservesEveryRow() throws Exception {
        assertEveryRowExactlyOnce(true);
    }

    @Test
    @DisplayName("a device that fails on its first record loses and duplicates nothing")
    void firstRecordFailureConservesEveryRow() throws Exception {
        assertEveryRowExactlyOnce(false);
    }

    @Test
    @DisplayName("the operator reports which recoverable point it fell back at")
    void theFallbackPointIsReported() throws Exception {
        Collecting cpu = new Collecting();
        FallbackToCpuOperator guarded =
                new FallbackToCpuOperator(new FailingAccelerated(true), () -> cpu, null);
        guarded.open();
        assertThat(guarded.isAccelerated()).isFalse();
        assertThat(guarded.declineReason()).isEqualTo(AcceleratorDeclineReason.FELL_BACK_AT_OPEN);

        Collecting cpu2 = new Collecting();
        FallbackToCpuOperator second =
                new FallbackToCpuOperator(new FailingAccelerated(false), () -> cpu2, null);
        second.open();
        second.processElement(new StreamRecord<>(row(0)));
        assertThat(second.declineReason())
                .isEqualTo(AcceleratorDeclineReason.FELL_BACK_AT_FIRST_RECORD);
    }

    private static void assertEveryRowExactlyOnce(boolean failOnOpen) throws Exception {
        Collecting cpu = new Collecting();
        FallbackToCpuOperator guarded =
                new FallbackToCpuOperator(new FailingAccelerated(failOnOpen), () -> cpu, null);

        guarded.open();
        for (int i = 0; i < ROWS; i++) {
            guarded.processElement(new StreamRecord<>(row(i)));
        }

        List<Integer> expected = IntStream.range(0, ROWS).boxed().collect(Collectors.toList());
        assertThat(cpu.seen)
                .as("every input row must reach the generated operator exactly once, in order")
                .containsExactlyElementsOf(expected);
        assertThat(guarded.isAccelerated()).isFalse();
    }

    private static RowData row(int i) {
        return GenericRowData.of(i);
    }
}
