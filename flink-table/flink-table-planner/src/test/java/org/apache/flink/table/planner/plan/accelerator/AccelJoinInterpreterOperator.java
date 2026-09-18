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
import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.api.operators.InputSelectable;
import org.apache.flink.streaming.api.operators.InputSelection;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.accelerator.AccelJoin;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Host-side stand-in for a device join, so the two-input seam is testable with no device.
 *
 * <p>It exists to prove the plumbing rather than the joining: that an {@link AccelJoin} reaches a
 * provider, that a two-input operator built by one is the operator that runs, that it is asked for
 * its inputs in build-then-probe order, and that what comes out is what the CPU plan would have
 * produced. The joining itself is a {@link HashMap}.
 *
 * <p>The first accelerator operator here with two inputs, and the two things that follow from that
 * are the two things worth testing. It must declare {@link InputSelectable}, because Flink asks the
 * running operator which input to read and the plan it was substituted into expects the build side
 * first; and it must emit the query's field order, which is not its input order — the build side is
 * whichever side the planner costed as smaller, and {@link AccelJoin#buildIsLeft()} is what says
 * where its fields go.
 */
public class AccelJoinInterpreterOperator extends AbstractStreamOperator<RowData>
        implements TwoInputStreamOperator<RowData, RowData, RowData>,
                BoundedMultiInput,
                InputSelectable {

    private static final long serialVersionUID = 1L;

    private final AccelJoin join;
    private final RowType buildType;
    private final RowType probeType;
    private final RowType outputType;
    private final AtomicInteger rowsProcessed;

    private transient Map<Integer, List<RowData>> table;
    private transient boolean buildEnded;
    private transient GenericRowData outRow;
    private transient StreamRecord<RowData> outElement;

    public AccelJoinInterpreterOperator(
            AccelJoin join, RowType outputType, AtomicInteger rowsProcessed) {
        this.join = join;
        this.buildType = join.inputs().get(0).outputType();
        this.probeType = join.inputs().get(1).outputType();
        this.outputType = outputType;
        this.rowsProcessed = rowsProcessed;
    }

    @Override
    public void open() throws Exception {
        super.open();
        table = new HashMap<>();
        buildEnded = false;
        outRow = new GenericRowData(outputType.getFieldCount());
        outElement = new StreamRecord<>(null);
    }

    @Override
    public InputSelection nextSelection() {
        return buildEnded ? InputSelection.SECOND : InputSelection.FIRST;
    }

    @Override
    public void processElement1(StreamRecord<RowData> element) {
        RowData row = element.getValue();
        // Copied, not retained: batch mode reuses the instance.
        table.computeIfAbsent(row.getInt(join.buildKeyField()), k -> new ArrayList<>())
                .add(copy(row, buildType));
    }

    @Override
    public void processElement2(StreamRecord<RowData> element) {
        RowData probe = element.getValue();
        rowsProcessed.incrementAndGet();
        List<RowData> matches = table.get(probe.getInt(join.probeKeyField()));
        if (matches == null) {
            return;
        }
        for (RowData build : matches) {
            emit(build, probe);
        }
    }

    @Override
    public void endInput(int inputId) {
        if (inputId == 1) {
            buildEnded = true;
        }
    }

    /** Build fields then probe fields, or the other way round — whichever the query wrote. */
    private void emit(RowData build, RowData probe) {
        RowData first = join.buildIsLeft() ? build : probe;
        RowData second = join.buildIsLeft() ? probe : build;
        RowType firstType = join.buildIsLeft() ? buildType : probeType;
        RowType secondType = join.buildIsLeft() ? probeType : buildType;
        int at = 0;
        for (int i = 0; i < firstType.getFieldCount(); i++) {
            outRow.setField(at++, field(first, i, firstType.getTypeAt(i)));
        }
        for (int i = 0; i < secondType.getFieldCount(); i++) {
            outRow.setField(at++, field(second, i, secondType.getTypeAt(i)));
        }
        output.collect(outElement.replace(outRow));
    }

    private static RowData copy(RowData row, RowType type) {
        GenericRowData copy = new GenericRowData(type.getFieldCount());
        copy.setRowKind(row.getRowKind());
        for (int i = 0; i < type.getFieldCount(); i++) {
            copy.setField(i, field(row, i, type.getTypeAt(i)));
        }
        return copy;
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
