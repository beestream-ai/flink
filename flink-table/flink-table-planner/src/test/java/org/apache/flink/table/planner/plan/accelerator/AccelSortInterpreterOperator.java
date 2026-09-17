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
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.accelerator.AccelSort;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Host-side stand-in for a device sort, so the planner seam is testable with no device.
 *
 * <p>It exists to prove the plumbing rather than the ordering: that a {@link AccelSort} reaches a
 * provider, that the provider's operator is the one that runs, and that what comes out is what the
 * CPU plan would have produced. The ordering itself is {@code List.sort}.
 *
 * <p>Whole-input, and that is the point of the class: this is the first accelerator operator here
 * that cannot emit anything until {@link #endInput()}, which is the constraint a real device sort
 * is under too.
 */
public class AccelSortInterpreterOperator extends AbstractStreamOperator<RowData>
        implements OneInputStreamOperator<RowData, RowData>, BoundedOneInput {

    private static final long serialVersionUID = 1L;

    private final AccelSort sort;
    private final RowType outputType;
    private final AtomicInteger rowsProcessed;

    private transient List<RowData> buffered;

    public AccelSortInterpreterOperator(
            AccelSort sort, RowType outputType, AtomicInteger rowsProcessed) {
        this.sort = sort;
        this.outputType = outputType;
        this.rowsProcessed = rowsProcessed;
    }

    @Override
    public void open() throws Exception {
        super.open();
        buffered = new ArrayList<>();
    }

    @Override
    public void processElement(StreamRecord<RowData> element) {
        // Copied, not retained: the planner force-enables object reuse in batch mode, so the
        // instance handed over here is very often the same one every time.
        buffered.add(copy(element.getValue()));
        rowsProcessed.incrementAndGet();
    }

    @Override
    public void endInput() {
        buffered.sort(comparator());
        StreamRecord<RowData> out = new StreamRecord<>(null);
        for (RowData row : buffered) {
            output.collect(out.replace(row));
        }
        buffered.clear();
    }

    private Comparator<RowData> comparator() {
        int field = sort.sortField();
        Comparator<RowData> byKey =
                Comparator.comparing(
                        row -> row.isNullAt(field) ? null : row.getInt(field),
                        Comparator.nullsLast(Comparator.naturalOrder()));
        if (!sort.nullsLast()) {
            byKey =
                    Comparator.comparing(
                            row -> row.isNullAt(field) ? null : row.getInt(field),
                            Comparator.nullsFirst(Comparator.naturalOrder()));
        }
        return sort.ascending() ? byKey : byKey.reversed();
    }

    private RowData copy(RowData row) {
        List<LogicalType> fields = outputType.getChildren();
        GenericRowData copy = new GenericRowData(fields.size());
        copy.setRowKind(row.getRowKind());
        for (int i = 0; i < fields.size(); i++) {
            copy.setField(i, row.isNullAt(i) ? null : field(row, i, fields.get(i)));
        }
        return copy;
    }

    private static Object field(RowData row, int i, LogicalType type) {
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
