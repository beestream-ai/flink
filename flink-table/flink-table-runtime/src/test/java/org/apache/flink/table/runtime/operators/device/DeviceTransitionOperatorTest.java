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

package org.apache.flink.table.runtime.operators.device;

import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.device.DeviceBatch;
import org.apache.flink.table.data.device.DeviceBuffer;
import org.apache.flink.table.data.device.DeviceColumn;
import org.apache.flink.table.runtime.accelerator.DeviceBatchTransfer;
import org.apache.flink.table.runtime.typeutils.DeviceBatchSerializer;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the M9.7 transition operators, in each direction. */
class DeviceTransitionOperatorTest {

    private static final RowType ROW = RowType.of(new IntType(false), new DoubleType(false));

    /** A device that is really the host, so the operators can be tested without one. */
    private static final class HostBackedTransfer implements DeviceBatchTransfer {

        private final List<int[]> keys = new ArrayList<>();
        private final List<double[]> values = new ArrayList<>();
        private int released;

        private final class Region implements DeviceBuffer {
            private final long bytes;

            private Region(long bytes) {
                this.bytes = bytes;
            }

            @Override
            public long sizeInBytes() {
                return bytes;
            }

            @Override
            public void release() {
                released++;
            }
        }

        @Override
        public void toHost(DeviceBatch batch, Consumer<RowData> out) {
            int slot = keys.size() - 1;
            int[] k = keys.get(slot);
            double[] v = values.get(slot);
            // One reused row, as a real transfer would hand out.
            GenericRowData row = new GenericRowData(2);
            for (int i = 0; i < batch.rowCount(); i++) {
                row.setField(0, k[i]);
                row.setField(1, v[i]);
                out.accept(row);
            }
        }

        @Override
        public DeviceBatch toDevice(List<RowData> rows, RowType rowType) {
            int[] k = new int[rows.size()];
            double[] v = new double[rows.size()];
            for (int i = 0; i < rows.size(); i++) {
                k[i] = rows.get(i).getInt(0);
                v[i] = rows.get(i).getDouble(1);
            }
            keys.add(k);
            values.add(v);
            return new DeviceBatch(
                    rowType,
                    Arrays.asList(
                            new DeviceColumn(new IntType(false), new Region(k.length * 4L)),
                            new DeviceColumn(new DoubleType(false), new Region(v.length * 8L))),
                    rows.size());
        }

        @Override
        public boolean supports(RowType rowType) {
            return ROW.equals(rowType);
        }
    }

    private static GenericRowData row(int k, double v) {
        GenericRowData row = new GenericRowData(2);
        row.setField(0, k);
        row.setField(1, v);
        return row;
    }

    private static RowData copy(RowData r) {
        return row(r.getInt(0), r.getDouble(1));
    }

    @Test
    void rowsGoToTheDeviceInBatchesAndTheTailIsNotDropped() throws Exception {
        HostBackedTransfer transfer = new HostBackedTransfer();
        RowToDeviceOperator operator =
                new RowToDeviceOperator(transfer, ROW, 2, DeviceTransitionOperatorTest::copy);

        List<DeviceBatch> emitted = new ArrayList<>();
        try (OneInputStreamOperatorTestHarness<RowData, DeviceBatch> harness =
                new OneInputStreamOperatorTestHarness<>(operator)) {
            harness.setup(new DeviceBatchSerializer());
            harness.open();
            harness.processElement(row(1, 1.5), 0);
            harness.processElement(row(2, 2.5), 0);
            harness.processElement(row(3, 3.5), 0);
            operator.endInput();
            harness.getOutput()
                    .forEach(
                            o ->
                                    emitted.add(
                                            ((org.apache.flink.streaming.runtime.streamrecord
                                                                            .StreamRecord<
                                                                    DeviceBatch>)
                                                            o)
                                                    .getValue()));
        }

        assertThat(emitted).as("two full rows, then the tail of one").hasSize(2);
        assertThat(emitted.get(0).rowCount()).isEqualTo(2);
        assertThat(emitted.get(1).rowCount()).isEqualTo(1);
    }

    /**
     * Flink reuses the row it hands an operator. Holding it until the batch fills would stage
     * whatever the next row overwrote it with, which shows up as a batch of identical rows.
     */
    @Test
    void arrivingRowsAreCopiedNotReferenced() throws Exception {
        HostBackedTransfer transfer = new HostBackedTransfer();
        RowToDeviceOperator operator =
                new RowToDeviceOperator(transfer, ROW, 3, DeviceTransitionOperatorTest::copy);

        GenericRowData reused = new GenericRowData(2);
        try (OneInputStreamOperatorTestHarness<RowData, DeviceBatch> harness =
                new OneInputStreamOperatorTestHarness<>(operator)) {
            harness.setup(new DeviceBatchSerializer());
            harness.open();
            for (int i = 1; i <= 3; i++) {
                reused.setField(0, i);
                reused.setField(1, i + 0.5);
                harness.processElement(reused, 0);
            }
            operator.endInput();
        }

        assertThat(transfer.keys.get(0)).containsExactly(1, 2, 3);
        assertThat(transfer.values.get(0)).containsExactly(1.5, 2.5, 3.5);
    }

    @Test
    void aBatchComesBackAsRowsInOrderAndIsThenReleased() throws Exception {
        HostBackedTransfer transfer = new HostBackedTransfer();
        DeviceBatch batch =
                transfer.toDevice(Arrays.asList(row(7, 7.5), row(8, 8.5), row(9, 9.5)), ROW);

        List<String> seen = new ArrayList<>();
        DeviceToRowOperator operator = new DeviceToRowOperator(transfer);
        try (OneInputStreamOperatorTestHarness<DeviceBatch, RowData> harness =
                new OneInputStreamOperatorTestHarness<>(operator)) {
            harness.setup();
            harness.open();
            harness.processElement(batch, 0);
            harness.getOutput()
                    .forEach(
                            o -> {
                                RowData r =
                                        ((org.apache.flink.streaming.runtime.streamrecord
                                                                        .StreamRecord<
                                                                RowData>)
                                                        o)
                                                .getValue();
                                seen.add(r.getInt(0) + ":" + r.getDouble(1));
                            });
        }

        assertThat(seen).containsExactly("7:7.5", "8:8.5", "9:9.5");
        assertThat(batch.isClosed()).as("the transition is the last reader of the batch").isTrue();
        assertThat(transfer.released).as("both columns released, once each").isEqualTo(2);
    }

    /** A round trip must not change a value, or the fallback path is a wrong answer. */
    @Test
    void aRoundTripPreservesEveryRow() throws Exception {
        HostBackedTransfer transfer = new HostBackedTransfer();
        List<RowData> original =
                Arrays.asList(row(1, -0.0), row(2, 1e-300), row(3, 1e300), row(4, 0.1));

        DeviceBatch batch = transfer.toDevice(original, ROW);
        List<String> back = new ArrayList<>();
        transfer.toHost(batch, r -> back.add(r.getInt(0) + ":" + r.getDouble(1)));
        batch.close();

        assertThat(back).containsExactly("1:-0.0", "2:1.0E-300", "3:1.0E300", "4:0.1");
    }

    /** A failure in the copy must still release the device memory. */
    @Test
    void aFailingTransferStillReleasesTheBatch() throws Exception {
        HostBackedTransfer transfer = new HostBackedTransfer();
        DeviceBatch batch = transfer.toDevice(Arrays.asList(row(1, 1.0)), ROW);

        DeviceBatchTransfer exploding =
                new DeviceBatchTransfer() {
                    @Override
                    public void toHost(DeviceBatch b, Consumer<RowData> out) {
                        throw new IllegalStateException("device gone");
                    }

                    @Override
                    public DeviceBatch toDevice(List<RowData> rows, RowType rowType) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public boolean supports(RowType rowType) {
                        return true;
                    }
                };

        DeviceToRowOperator operator = new DeviceToRowOperator(exploding);
        try (OneInputStreamOperatorTestHarness<DeviceBatch, RowData> harness =
                new OneInputStreamOperatorTestHarness<>(operator)) {
            harness.setup();
            harness.open();
            try {
                harness.processElement(batch, 0);
            } catch (Exception expected) {
                // the point is what happened to the batch, not the exception
            }
        }

        assertThat(batch.isClosed()).isTrue();
        assertThat(transfer.released).isEqualTo(2);
    }

    @Test
    void aBatchSizeThatCannotFillIsRefused() {
        HostBackedTransfer transfer = new HostBackedTransfer();
        try {
            new RowToDeviceOperator(transfer, ROW, 0, DeviceTransitionOperatorTest::copy);
            assertThat(false).as("a zero batch size must be refused").isTrue();
        } catch (IllegalArgumentException expected) {
            assertThat(expected).hasMessageContaining("batchSize");
        }
    }

    /**
     * M9.11. A masked column read as dense returns the padding wherever a value was null, and a row
     * count of the right length agrees with it. So it is refused, and the refusal names the column
     * and the fix.
     */
    @Test
    void aMaskedColumnIsRefusedByATransferThatDoesNotReadMasks() throws Exception {
        HostBackedTransfer transfer = new HostBackedTransfer();
        DeviceBuffer values = transfer.new Region(8);
        DeviceBuffer validity = transfer.new Region(1);
        DeviceBatch masked =
                new DeviceBatch(
                        RowType.of(new DoubleType(true)),
                        Arrays.asList(new DeviceColumn(new DoubleType(true), values, validity)),
                        1);

        assertThat(transfer.supportsNulls()).as("the default contract is dense").isFalse();

        DeviceToRowOperator operator = new DeviceToRowOperator(transfer);
        Throwable caught = null;
        try (OneInputStreamOperatorTestHarness<DeviceBatch, RowData> harness =
                new OneInputStreamOperatorTestHarness<>(operator)) {
            harness.setup();
            harness.open();
            try {
                harness.processElement(masked, 0);
            } catch (Exception e) {
                caught = e;
            }
        }

        assertThat(caught).isNotNull();
        assertThat(rootCause(caught))
                .hasMessageContaining("validity mask")
                .hasMessageContaining("NOT NULL");
        assertThat(masked.isClosed()).as("refusing still releases the memory").isTrue();
    }

    /** A transfer that does understand masks is handed the batch unchanged. */
    @Test
    void aTransferThatReadsMasksIsNotRefused() throws Exception {
        HostBackedTransfer backing = new HostBackedTransfer();
        DeviceBuffer values = backing.new Region(8);
        DeviceBuffer validity = backing.new Region(1);
        DeviceBatch masked =
                new DeviceBatch(
                        RowType.of(new DoubleType(true)),
                        Arrays.asList(new DeviceColumn(new DoubleType(true), values, validity)),
                        1);

        List<String> seen = new ArrayList<>();
        DeviceBatchTransfer maskAware =
                new DeviceBatchTransfer() {
                    @Override
                    public boolean supportsNulls() {
                        return true;
                    }

                    @Override
                    public void toHost(DeviceBatch b, Consumer<RowData> out) {
                        seen.add("copied " + b.rowCount());
                    }

                    @Override
                    public DeviceBatch toDevice(List<RowData> rows, RowType rowType) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public boolean supports(RowType rowType) {
                        return true;
                    }
                };

        DeviceToRowOperator operator = new DeviceToRowOperator(maskAware);
        try (OneInputStreamOperatorTestHarness<DeviceBatch, RowData> harness =
                new OneInputStreamOperatorTestHarness<>(operator)) {
            harness.setup();
            harness.open();
            harness.processElement(masked, 0);
        }

        assertThat(seen).containsExactly("copied 1");
    }

    /** An unmasked batch is unaffected, so nothing already working slows down or changes. */
    @Test
    void anUnmaskedBatchIsNotCheckedAgainstAnything() throws Exception {
        HostBackedTransfer transfer = new HostBackedTransfer();
        DeviceBatch dense = transfer.toDevice(Arrays.asList(row(1, 1.0)), ROW);
        List<String> seen = new ArrayList<>();

        DeviceToRowOperator operator = new DeviceToRowOperator(transfer);
        try (OneInputStreamOperatorTestHarness<DeviceBatch, RowData> harness =
                new OneInputStreamOperatorTestHarness<>(operator)) {
            harness.setup();
            harness.open();
            harness.processElement(dense, 0);
            harness.getOutput()
                    .forEach(
                            o ->
                                    seen.add(
                                            String.valueOf(
                                                    ((org.apache.flink.streaming.runtime
                                                                                    .streamrecord
                                                                                    .StreamRecord<
                                                                            RowData>)
                                                                    o)
                                                            .getValue()
                                                            .getInt(0))));
        }
        assertThat(seen).containsExactly("1");
    }

    private static Throwable rootCause(Throwable t) {
        Throwable c = t;
        while (c.getCause() != null) {
            c = c.getCause();
        }
        return c;
    }

    /**
     * M9.10. A batch exists on the device from the moment toDevice returns until collect hands it
     * on. A failure in that window would strand the allocation, and a device allocation that
     * outlives its job is not reclaimed by anything short of the process ending.
     */
    @Test
    void aBatchThatCannotBeHandedOnIsReleased() throws Exception {
        HostBackedTransfer transfer = new HostBackedTransfer();
        RowToDeviceOperator operator =
                new RowToDeviceOperator(transfer, ROW, 1, DeviceTransitionOperatorTest::copy);

        try (OneInputStreamOperatorTestHarness<RowData, DeviceBatch> harness =
                new OneInputStreamOperatorTestHarness<>(operator) {
                    @Override
                    public void processElement(
                            org.apache.flink.streaming.runtime.streamrecord.StreamRecord<RowData>
                                    element)
                            throws Exception {
                        super.processElement(element);
                    }
                }) {
            harness.setup(new ExplodingOnCollect());
            harness.open();
            try {
                harness.processElement(row(1, 1.0), 0);
            } catch (Exception | Error expected) {
                // the point is what happened to the allocation
            }
        }

        assertThat(transfer.released)
                .as("both columns of the stranded batch released")
                .isEqualTo(2);
    }

    /** A serializer that fails the hand-off, standing in for a downstream operator that throws. */
    private static final class ExplodingOnCollect
            extends org.apache.flink.api.common.typeutils.TypeSerializer<DeviceBatch> {
        private static final long serialVersionUID = 1L;

        @Override
        public boolean isImmutableType() {
            return false;
        }

        @Override
        public org.apache.flink.api.common.typeutils.TypeSerializer<DeviceBatch> duplicate() {
            return this;
        }

        @Override
        public DeviceBatch createInstance() {
            throw new UnsupportedOperationException();
        }

        @Override
        public DeviceBatch copy(DeviceBatch from) {
            throw new IllegalStateException("downstream refused the batch");
        }

        @Override
        public DeviceBatch copy(DeviceBatch from, DeviceBatch reuse) {
            throw new IllegalStateException("downstream refused the batch");
        }

        @Override
        public int getLength() {
            return -1;
        }

        @Override
        public void serialize(
                DeviceBatch record, org.apache.flink.core.memory.DataOutputView target) {
            throw new UnsupportedOperationException();
        }

        @Override
        public DeviceBatch deserialize(org.apache.flink.core.memory.DataInputView source) {
            throw new UnsupportedOperationException();
        }

        @Override
        public DeviceBatch deserialize(
                DeviceBatch reuse, org.apache.flink.core.memory.DataInputView source) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void copy(
                org.apache.flink.core.memory.DataInputView source,
                org.apache.flink.core.memory.DataOutputView target) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof ExplodingOnCollect;
        }

        @Override
        public int hashCode() {
            return ExplodingOnCollect.class.hashCode();
        }

        @Override
        public org.apache.flink.api.common.typeutils.TypeSerializerSnapshot<DeviceBatch>
                snapshotConfiguration() {
            throw new UnsupportedOperationException();
        }
    }

    /** Closing with nothing in flight releases nothing and throws nothing. */
    @Test
    void closingACleanOperatorIsQuiet() throws Exception {
        HostBackedTransfer transfer = new HostBackedTransfer();
        RowToDeviceOperator operator =
                new RowToDeviceOperator(transfer, ROW, 8, DeviceTransitionOperatorTest::copy);
        try (OneInputStreamOperatorTestHarness<RowData, DeviceBatch> harness =
                new OneInputStreamOperatorTestHarness<>(operator)) {
            harness.setup(new DeviceBatchSerializer());
            harness.open();
        }
        assertThat(transfer.released).isZero();
    }
}
