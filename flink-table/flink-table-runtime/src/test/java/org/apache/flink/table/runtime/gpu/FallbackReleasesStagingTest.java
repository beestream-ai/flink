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
import org.apache.flink.table.data.RowData;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The order in which a failing device hands the slot's managed memory back.
 *
 * <p>M3.4 promises that a device failing early costs a job nothing: the generated operator is in
 * the JobGraph and takes over. That promise was not kept for any generated operator that wants
 * managed memory -- a hash aggregate, a sort -- because accelerated staging reserves the slot's
 * whole operator share and held it through the switch. The fallback then died asking for memory the
 * operator it was replacing was still holding, turning a recoverable device failure into a failed
 * job.
 *
 * <p>So the ordering is the contract: the accelerated operator is closed, then staging is released,
 * and only then is the generated operator built. Releasing earlier would take buffers away from a
 * kernel still running; releasing later is the bug.
 */
class FallbackReleasesStagingTest {

    /** What happened, in the order it happened. */
    private final List<String> events = new ArrayList<>();

    @Test
    @DisplayName("staging is released after the device closes and before the CPU operator is built")
    void stagingIsReleasedBeforeTheCpuOperatorIsBuilt() throws Exception {
        FallbackToCpuOperator operator =
                new FallbackToCpuOperator(
                        new FailingOperator(),
                        () -> {
                            events.add("build-cpu");
                            return new RecordingOperator();
                        },
                        () -> events.add("release-staging"));

        operator.processElement(new StreamRecord<>(null));

        assertThat(events)
                .as("a CPU operator built while staging still holds the slot's memory is the bug")
                .containsExactly("close-device", "release-staging", "build-cpu", "cpu-processed");
    }

    @Test
    @DisplayName("a fallback with no staging to release still falls back")
    void noStagingIsNotAFailure() throws Exception {
        FallbackToCpuOperator operator =
                new FallbackToCpuOperator(new FailingOperator(), RecordingOperator::new, null);

        operator.processElement(new StreamRecord<>(null));

        assertThat(events).containsExactly("close-device", "cpu-processed");
    }

    /**
     * Releasing can fail, and it must not be the thing that fails the job.
     *
     * <p>Whatever is left may still be enough for the generated operator, and if it is not, the
     * error that says so is the useful one.
     */
    @Test
    @DisplayName("a staging release that throws does not stop the fallback")
    void aFailingReleaseDoesNotStopTheFallback() throws Exception {
        FallbackToCpuOperator operator =
                new FallbackToCpuOperator(
                        new FailingOperator(),
                        RecordingOperator::new,
                        () -> {
                            throw new IllegalStateException("already gone");
                        });

        operator.processElement(new StreamRecord<>(null));

        assertThat(events).containsExactly("close-device", "cpu-processed");
    }

    /** Stands in for an accelerated operator whose first record kills it. */
    private final class FailingOperator extends AbstractStreamOperator<RowData>
            implements OneInputStreamOperator<RowData, RowData> {

        private static final long serialVersionUID = 1L;

        @Override
        public void processElement(StreamRecord<RowData> element) {
            throw new UnsupportedOperationException("the device is not usable here");
        }

        @Override
        public void close() {
            events.add("close-device");
        }
    }

    /** Stands in for the operator Flink generated, which is what must end up running. */
    private final class RecordingOperator extends AbstractStreamOperator<RowData>
            implements OneInputStreamOperator<RowData, RowData> {

        private static final long serialVersionUID = 1L;

        @Override
        public void open() {}

        @Override
        public void processElement(StreamRecord<RowData> element) {
            events.add("cpu-processed");
        }
    }
}
