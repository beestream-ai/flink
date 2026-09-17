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
import org.apache.flink.table.accelerator.AccelAggCall;
import org.apache.flink.table.accelerator.AccelAggFunction;
import org.apache.flink.table.accelerator.AccelAggregate;
import org.apache.flink.table.accelerator.AccelExpression;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Runs an {@link AccelAggregate} on the host, so the planner side of M5.3 is testable with no
 * device on the machine.
 *
 * <p>It is a <em>partial</em> aggregate, which is what Flink's local half is: the merge above the
 * shuffle combines whatever this emits, so emitting once at end of input is as correct as emitting
 * per batch. A real provider aggregates a batch at a time and emits as it goes; both are partial
 * and both are right.
 */
final class AccelAggregateInterpreterOperator extends AbstractStreamOperator<RowData>
        implements OneInputStreamOperator<RowData, RowData>, BoundedOneInput {

    private static final long serialVersionUID = 1L;

    private final AccelAggregate aggregate;
    private final RowType outputType;
    private final AtomicInteger rowsProcessed;

    private transient List<AccelExpression> projections;
    private transient AccelExpression condition;
    private transient Map<List<Object>, double[]> groups;

    AccelAggregateInterpreterOperator(
            AccelAggregate aggregate, RowType outputType, AtomicInteger rowsProcessed) {
        this.aggregate = aggregate;
        this.outputType = outputType;
        this.rowsProcessed = rowsProcessed;
    }

    @Override
    public void open() throws Exception {
        super.open();
        projections = AccelInterpreter.projectionsOf(aggregate.inputs().get(0));
        condition = AccelInterpreter.conditionOf(aggregate.inputs().get(0));
        // Insertion-ordered so a test comparing against the CPU plan sees a stable order.
        groups = new LinkedHashMap<>();
    }

    @Override
    public void processElement(StreamRecord<RowData> record) {
        RowData row = record.getValue();
        rowsProcessed.incrementAndGet();

        if (condition != null) {
            Double keep = AccelInterpreter.eval(condition, row);
            if (keep == null || keep == 0.0) {
                return;
            }
        }
        // The projection beneath produces the row the aggregate reads, so evaluate it first.
        Double[] projected = new Double[projections.size()];
        for (int i = 0; i < projections.size(); i++) {
            projected[i] = AccelInterpreter.eval(projections.get(i), row);
        }

        List<Object> key = new ArrayList<>(aggregate.grouping().length);
        for (int field : aggregate.grouping()) {
            key.add(projected[field]);
        }
        double[] accumulators =
                groups.computeIfAbsent(key, k -> new double[aggregate.calls().size()]);
        for (int i = 0; i < aggregate.calls().size(); i++) {
            AccelAggCall call = aggregate.calls().get(i);
            if (call.function() == AccelAggFunction.COUNT_STAR) {
                accumulators[i] += 1.0;
            } else {
                Double value = projected[call.inputField()];
                if (value != null) {
                    accumulators[i] += value;
                }
            }
        }
    }

    @Override
    public void endInput() {
        int[] grouping = aggregate.grouping();
        for (Map.Entry<List<Object>, double[]> group : groups.entrySet()) {
            GenericRowData out = new GenericRowData(outputType.getFieldCount());
            for (int i = 0; i < grouping.length; i++) {
                out.setField(i, narrow((Double) group.getKey().get(i), outputType.getTypeAt(i)));
            }
            for (int i = 0; i < aggregate.calls().size(); i++) {
                out.setField(
                        grouping.length + i,
                        narrow(group.getValue()[i], outputType.getTypeAt(grouping.length + i)));
            }
            output.collect(new StreamRecord<>(out));
        }
    }

    private static Object narrow(Double value, LogicalType type) {
        if (value == null) {
            return null;
        }
        LogicalTypeRoot root = type.getTypeRoot();
        if (root == LogicalTypeRoot.INTEGER) {
            return value.intValue();
        }
        if (root == LogicalTypeRoot.BIGINT) {
            return value.longValue();
        }
        if (root == LogicalTypeRoot.FLOAT) {
            return value.floatValue();
        }
        return value;
    }
}
