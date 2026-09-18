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

package org.apache.flink.table.planner.plan.accelerator;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.accelerator.AccelOverAggregate;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Host-side stand-in for a device running total, so the seam is testable with no device.
 *
 * <p>The simplest of these interpreters, and that is itself the point of having it: unlike the sort
 * and the join this operator holds nothing, emits on every record, and carries one {@code double}
 * between them. If the plumbing for a cross-row node that is nevertheless streaming is wrong, it is
 * wrong here in four lines rather than somewhere in a buffer.
 */
public class AccelOverAggregateInterpreterOperator extends AbstractStreamOperator<RowData>
        implements OneInputStreamOperator<RowData, RowData> {

    private static final long serialVersionUID = 1L;

    private final AccelOverAggregate over;
    private final RowType outputType;
    private final AtomicInteger rowsProcessed;

    private transient double runningTotal;
    private transient GenericRowData outRow;
    private transient StreamRecord<RowData> outElement;

    public AccelOverAggregateInterpreterOperator(
            AccelOverAggregate over, RowType outputType, AtomicInteger rowsProcessed) {
        this.over = over;
        this.outputType = outputType;
        this.rowsProcessed = rowsProcessed;
    }

    @Override
    public void open() throws Exception {
        super.open();
        runningTotal = 0.0;
        outRow = new GenericRowData(outputType.getFieldCount());
        outElement = new StreamRecord<>(null);
    }

    @Override
    public void processElement(StreamRecord<RowData> element) {
        RowData row = element.getValue();
        runningTotal += row.getDouble(over.valueField());
        int inputFields = outputType.getFieldCount() - 1;
        for (int i = 0; i < inputFields; i++) {
            outRow.setField(i, field(row, i, outputType.getTypeAt(i)));
        }
        // The running total is the last field, which is where Flink's own over-aggregate puts it.
        outRow.setField(inputFields, runningTotal);
        rowsProcessed.incrementAndGet();
        output.collect(outElement.replace(outRow));
    }

    private static Object field(RowData row, int i, LogicalType type) {
        if (row.isNullAt(i)) {
            return null;
        }
        switch (type.getTypeRoot()) {
            case INTEGER:
                return row.getInt(i);
            case BIGINT:
                return row.getLong(i);
            case FLOAT:
                return row.getFloat(i);
            case DOUBLE:
                return row.getDouble(i);
            case BOOLEAN:
                return row.getBoolean(i);
            default:
                return row.getString(i);
        }
    }
}
