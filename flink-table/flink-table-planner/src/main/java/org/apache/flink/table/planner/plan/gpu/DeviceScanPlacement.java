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

/**
 * Whether reading a source on a device is worth it, given what reads the source.
 *
 * <p>The rule spark-rapids uses, in its terms: a device operator is declined when {@code
 * operatorGpuCost + transitionCost > operatorCpuCost}. A scan is where that rule bites hardest,
 * because a scan feeding a host consumer pays for the device read <em>and</em> the copy back, and
 * gets nothing the CPU read would not have given it.
 *
 * <h2>The numbers, and where they come from</h2>
 *
 * <p>Two of the three are measured, on 8M rows of 31 columns (VERIFY.md §T25):
 *
 * <ul>
 *   <li><b>CPU decode, 17.3 ns a field.</b> Reading all 31 columns took 6,539 ms against 2,447 ms
 *       for a {@code COUNT(*)} that projection pushdown answers without reading a column, so the
 *       decode is ~4.1 s over 8M x 31 fields.
 *   <li><b>Copy back, 11.3 ns a field.</b> The drain of an offloaded top-N, 2,905 ms over 8M x 32
 *       fields.
 *   <li><b>Staging, 17.0 ns a field.</b> The arrival path, 4,594 ms over the same.
 * </ul>
 *
 * <p>The third — how fast a device decodes Parquet — is <b>not</b> measured here, because nothing
 * in this project has measured it. That is why this class does not compute a speedup. It computes
 * the <em>ceiling the device read would have to beat</em>, which is arithmetic on measured numbers
 * and does not require inventing the one figure nobody has.
 *
 * <p>For a host consumer the comparison is {@code deviceDecode + copyBack} against {@code
 * cpuDecode}: 11.3 of the 17.3 ns is spent before the device has decoded anything, so the device
 * would have to decode in under 6 ns a field to break even. For a consumer that takes the batch as
 * it is, the copy back is zero and the staging the consumer would otherwise have paid is saved too,
 * so the device read has 34.3 ns a field to work inside. The first is a bet on an unmeasured
 * number; the second does not need one.
 */
public final class DeviceScanPlacement {

    /** Measured: 4.1 s of decode over 8M x 31 fields. */
    static final double CPU_DECODE_NANOS_PER_FIELD = 17.3;

    /** Measured: the 2,905 ms drain over 8M x 32 fields. */
    static final double COPY_BACK_NANOS_PER_FIELD = 11.3;

    /** Measured: the 4,594 ms arrival path over 8M x 32 fields. */
    static final double STAGING_NANOS_PER_FIELD = 17.0;

    /**
     * Measured: 101.3 ms for a resident cuDF read of 1M rows x 32 fields, §T27.
     *
     * <p>The figure this class was first written without. Its absence was the reason a scan feeding
     * a host consumer was declined, and its value is the reason that is no longer right.
     */
    static final double DEVICE_DECODE_NANOS_PER_FIELD = 3.17;

    /** What reads the scan, which is the only thing that decides this. */
    public enum Consumer {
        /** Nothing above it was offloaded. */
        NONE_OFFLOADED,
        /** Something above it was offloaded, but it reads rows on the host. */
        OFFLOADED_READS_ROWS,
        /** Something above it was offloaded and takes a device batch as it is. */
        OFFLOADED_READS_DEVICE
    }

    private DeviceScanPlacement() {}

    /**
     * The verdict, as a cost carrying the reason when it is a refusal.
     *
     * @param format the format identifier, for the reason text
     */
    public static RowCost decide(String format, Consumer consumer) {
        switch (consumer) {
            case NONE_OFFLOADED:
                return RowCost.ineligible(
                        "a "
                                + format
                                + " file source was offered to the accelerator; nothing above it"
                                + " runs on the device, so a device read would be followed"
                                + " immediately by a copy back to the host");
            case OFFLOADED_READS_ROWS:
                // The economics now favour the read -- 3.17 + 11.3 against 17.3, a 1.19x margin --
                // but no device source is wired into a Flink plan yet, so saying "offloaded" here
                // would make EXPLAIN claim something that does not happen. Declined on the
                // implementation, and the reason says so rather than blaming the cost, because the
                // cost is no longer the obstacle.
                return RowCost.ineligible(
                        "a "
                                + format
                                + " file source was offered to the accelerator; a device read is"
                                + " measured at "
                                + DEVICE_DECODE_NANOS_PER_FIELD
                                + " ns a field against "
                                + CPU_DECODE_NANOS_PER_FIELD
                                + " on the CPU, so this would pay by about "
                                + String.format("%.2f", hostConsumerSpeedup())
                                + "x even after building rows again on the host — but no device"
                                + " source is wired into a plan yet");
            case OFFLOADED_READS_DEVICE:
            default:
                return RowCost.ZERO;
        }
    }

    /**
     * How fast a device would have to decode a field for a device read feeding a host consumer to
     * break even. Arithmetic on two measured numbers, not an estimate of anything.
     */
    public static double breakEvenNanosPerField() {
        return CPU_DECODE_NANOS_PER_FIELD - COPY_BACK_NANOS_PER_FIELD;
    }

    /** Whether a device read repays itself even when its consumer wants rows on the host. */
    public static boolean hostConsumerPaysOff() {
        return hostConsumerSpeedup() > 1.0;
    }

    /** By how much, for the same case. Thin, and eaten by the row building rather than the read. */
    public static double hostConsumerSpeedup() {
        return CPU_DECODE_NANOS_PER_FIELD
                / (DEVICE_DECODE_NANOS_PER_FIELD + COPY_BACK_NANOS_PER_FIELD);
    }

    /** The margin when the consumer takes the batch as it is: no row building, no staging. */
    public static double residentSpeedup() {
        return budgetWhenResidentNanosPerField() / DEVICE_DECODE_NANOS_PER_FIELD;
    }

    /**
     * How much a device read has to work inside when its consumer takes the batch as it is: the CPU
     * decode it replaces, plus the staging the consumer no longer pays.
     */
    public static double budgetWhenResidentNanosPerField() {
        return CPU_DECODE_NANOS_PER_FIELD + STAGING_NANOS_PER_FIELD;
    }
}
