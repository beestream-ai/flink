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
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.device.DeviceBatch;
import org.apache.flink.table.runtime.accelerator.DeviceBatchTransfer;

/**
 * Turns a device-resident batch into rows, for a consumer that reads them on the host.
 *
 * <p>The fallback direction, and the one that has to be right before it is quick: every plan that
 * reads from a device and then does anything Flink has not offloaded ends here, and a transition
 * that drops or reorders a row is a wrong answer no row count catches.
 *
 * <p>It closes the batch it was given. A batch arriving at a transition has no reader after it —
 * that is what makes it a transition — so this is the last place that can release the device
 * memory, and a device allocation that outlives its job is not reclaimed by anything.
 */
public class DeviceToRowOperator extends AbstractStreamOperator<RowData>
        implements OneInputStreamOperator<DeviceBatch, RowData> {

    private static final long serialVersionUID = 1L;

    private final DeviceBatchTransfer transfer;

    private transient StreamRecord<RowData> outRecord;
    private transient Counter rowsOut;
    private transient Counter batchesOut;
    private transient Counter nanos;

    public DeviceToRowOperator(DeviceBatchTransfer transfer) {
        this.transfer = transfer;
    }

    @Override
    public void open() throws Exception {
        super.open();
        outRecord = new StreamRecord<>(null);
        // Reported through Flink's own metric group rather than the provider's OffloadMetrics,
        // which lives in the provider jar and is not reachable from here. Same intent: the cost of
        // a transition is attributable on its own and does not hide inside an operator's figure.
        rowsOut = getMetricGroup().counter("deviceToRowRows");
        batchesOut = getMetricGroup().counter("deviceToRowBatches");
        nanos = getMetricGroup().counter("deviceToRowNanos");
    }

    @Override
    public void processElement(StreamRecord<DeviceBatch> element) throws Exception {
        final DeviceBatch batch = element.getValue();
        final long start = System.nanoTime();
        try {
            transfer.toHost(
                    batch,
                    row -> {
                        outRecord.replace(row);
                        output.collect(outRecord);
                        rowsOut.inc();
                    });
        } finally {
            // Released whether or not the copy succeeded: a failure here is exactly the path that
            // would otherwise leak the allocation.
            batch.close();
            nanos.inc(System.nanoTime() - start);
            batchesOut.inc();
        }
    }
}
