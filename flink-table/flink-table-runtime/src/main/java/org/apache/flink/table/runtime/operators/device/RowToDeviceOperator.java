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

import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.device.DeviceBatch;
import org.apache.flink.table.runtime.accelerator.DeviceBatchTransfer;
import org.apache.flink.table.types.logical.RowType;

import java.util.ArrayList;
import java.util.List;

/**
 * Gathers host rows into device-resident batches, for an operator that reads them on a device.
 *
 * <p>The expensive direction, and the one M9 exists to make unnecessary: staging was measured at
 * 17 ns a field, which is ~4.6 s for 8M rows of 31 columns against ~57 ms of device execution. It
 * is here because a plan that offloads an operator whose input is not already on the device still
 * has to get it there, and because a transition that exists in only one direction cannot be priced
 * against one that exists in both.
 *
 * <p>Rows are copied on arrival, not referenced. Flink reuses the {@code RowData} it hands an
 * operator, so holding one until the batch fills would hold whatever the next row overwrote it
 * with — a bug that shows up as a batch of identical rows.
 */
public class RowToDeviceOperator extends AbstractStreamOperator<DeviceBatch>
        implements OneInputStreamOperator<RowData, DeviceBatch>, BoundedOneInput {

    private static final long serialVersionUID = 1L;

    private final DeviceBatchTransfer transfer;
    private final RowType rowType;
    private final int batchSize;
    private final RowDataCopier copier;

    private transient List<RowData> pending;
    private transient StreamRecord<DeviceBatch> outRecord;
    private transient Counter rowsIn;
    private transient Counter batchesIn;
    private transient Counter nanos;

    /** How a row is detached from the buffer Flink is about to reuse. */
    @FunctionalInterface
    public interface RowDataCopier extends java.io.Serializable {
        RowData copy(RowData row);
    }

    public RowToDeviceOperator(
            DeviceBatchTransfer transfer, RowType rowType, int batchSize, RowDataCopier copier) {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be positive: " + batchSize);
        }
        this.transfer = transfer;
        this.rowType = rowType;
        this.batchSize = batchSize;
        this.copier = copier;
    }

    @Override
    public void open() throws Exception {
        super.open();
        pending = new ArrayList<>(batchSize);
        outRecord = new StreamRecord<>(null);
        rowsIn = getMetricGroup().counter("rowToDeviceRows");
        batchesIn = getMetricGroup().counter("rowToDeviceBatches");
        nanos = getMetricGroup().counter("rowToDeviceNanos");
    }

    @Override
    public void processElement(StreamRecord<RowData> element) throws Exception {
        pending.add(copier.copy(element.getValue()));
        rowsIn.inc();
        if (pending.size() >= batchSize) {
            emit();
        }
    }

    /** The tail batch. Without this the last partial batch is silently dropped. */
    @Override
    public void endInput() throws Exception {
        emit();
    }

    private void emit() {
        if (pending.isEmpty()) {
            return;
        }
        final long start = System.nanoTime();
        final DeviceBatch batch = transfer.toDevice(pending, rowType);
        nanos.inc(System.nanoTime() - start);
        batchesIn.inc();
        pending = new ArrayList<>(batchSize);
        outRecord.replace(batch);
        output.collect(outRecord);
    }
}
