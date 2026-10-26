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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DeviceScanPlacement} — M9.8, both ends of the rule. */
class DeviceScanPlacementTest {

    @Test
    void aScanNothingOffloadedReadsIsRefused() {
        RowCost cost =
                DeviceScanPlacement.decide("parquet", DeviceScanPlacement.Consumer.NONE_OFFLOADED);
        assertThat(cost.isEligible()).isFalse();
        assertThat(cost.toString()).contains("nothing above it");
    }

    /**
     * Was a refusal until the device decode rate was measured. 3.17 + 11.3 against 17.3 is a 1.19x
     * margin — thin, and eaten by the row building rather than the read, which is the argument for
     * residency rather than against reading on the device.
     */
    @Test
    void aScanFeedingAnOffloadedOperatorWouldNowPayButHasNowhereToRun() {
        assertThat(DeviceScanPlacement.hostConsumerPaysOff())
                .as("3.17 device decode + 11.3 row building beats 17.3 CPU decode")
                .isTrue();
        RowCost cost =
                DeviceScanPlacement.decide(
                        "parquet", DeviceScanPlacement.Consumer.OFFLOADED_READS_ROWS);
        assertThat(cost.isEligible())
                .as("still declined, but on the missing implementation, not the cost")
                .isFalse();
        assertThat(cost.toString()).contains("no device source is wired into a plan yet");
        assertThat(cost.toString()).contains("3.17 ns a field");
    }

    /** The measured rate is what decides it, so it is pinned against its source. */
    @Test
    void theDeviceDecodeRateIsTheMeasuredOne() {
        assertThat(DeviceScanPlacement.DEVICE_DECODE_NANOS_PER_FIELD).isEqualTo(3.17);
        assertThat(DeviceScanPlacement.DEVICE_DECODE_NANOS_PER_FIELD)
                .as("it clears the break-even this class was written not knowing")
                .isLessThan(DeviceScanPlacement.breakEvenNanosPerField());
    }

    @Test
    void aScanFeedingAnOperatorThatTakesTheBatchIsAccepted() {
        RowCost cost =
                DeviceScanPlacement.decide(
                        "parquet", DeviceScanPlacement.Consumer.OFFLOADED_READS_DEVICE);
        assertThat(cost.isEligible()).isTrue();
    }

    /**
     * The break-even is arithmetic on two measured numbers, not an estimate of a device decode
     * rate, which nothing here has measured.
     */
    @Test
    void theBreakEvenIsTheCpuDecodeLessTheCopyBack() {
        assertThat(DeviceScanPlacement.breakEvenNanosPerField())
                .isCloseTo(
                        DeviceScanPlacement.CPU_DECODE_NANOS_PER_FIELD
                                - DeviceScanPlacement.COPY_BACK_NANOS_PER_FIELD,
                        org.assertj.core.data.Offset.offset(1e-9));
        assertThat(DeviceScanPlacement.breakEvenNanosPerField()).isLessThan(7.0);
    }

    /** Residency is worth far more than the read alone, because the staging goes too. */
    @Test
    void residencyBuysTheStagingAsWellAsTheDecode() {
        assertThat(DeviceScanPlacement.budgetWhenResidentNanosPerField())
                .isGreaterThan(5 * DeviceScanPlacement.breakEvenNanosPerField());
        assertThat(DeviceScanPlacement.residentSpeedup())
                .as("3.17 against 34.3 a field")
                .isGreaterThan(10.0);
    }
}
