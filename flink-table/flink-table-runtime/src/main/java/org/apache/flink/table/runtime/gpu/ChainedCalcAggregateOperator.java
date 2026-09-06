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

import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.event.WatermarkEvent;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamTaskStateInitializer;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.RecordAttributes;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.table.data.RowData;

/**
 * Runs the code-generated {@code Calc} and the code-generated aggregate as one operator.
 *
 * <p>This is what a TaskManager without a device runs when the plan fused a {@code Calc} into the
 * aggregate above it. Fusing collapses two ExecNodes into one transformation, so the fallback can
 * no longer be "the operator Flink would have built" -- there were two of them. It is instead
 * <em>both</em> of them, wired together here.
 *
 * <p>Which is the point. The Calc path's fallback is strong because it does not merely resemble the
 * code-generated operator, it <em>is</em> the code-generated operator, and the sum it produces is
 * bit-identical. Chaining keeps that property for the pair: no aggregation is reimplemented here,
 * so {@code SUM} over no rows is still {@code NULL}, nulls are still skipped, and the accumulation
 * order is still Flink's, without this class knowing any of it.
 *
 * <p>The wiring is one substituted {@link Output}. The aggregate is built against the real output;
 * the Calc is built against an output that hands each record to the aggregate instead of
 * downstream. Everything else -- watermarks, latency markers, record attributes -- is forwarded
 * along the same path a real chain would take.
 */
@Internal
final class ChainedCalcAggregateOperator
        implements OneInputStreamOperator<RowData, RowData>, BoundedOneInput {

    private final OneInputStreamOperator<RowData, RowData> calc;
    private final OneInputStreamOperator<RowData, RowData> aggregate;

    ChainedCalcAggregateOperator(
            OneInputStreamOperator<RowData, RowData> calc,
            OneInputStreamOperator<RowData, RowData> aggregate) {
        this.calc = calc;
        this.aggregate = aggregate;
    }

    /** Feeds one operator's output into the next, in place of the task's own output. */
    static final class Forwarding implements Output<StreamRecord<RowData>> {

        private final OneInputStreamOperator<RowData, RowData> downstream;
        private final Output<StreamRecord<RowData>> real;

        Forwarding(
                OneInputStreamOperator<RowData, RowData> downstream,
                Output<StreamRecord<RowData>> real) {
            this.downstream = downstream;
            this.real = real;
        }

        @Override
        public void collect(StreamRecord<RowData> record) {
            try {
                downstream.processElement(record);
            } catch (Exception e) {
                throw new RuntimeException("chained aggregate failed on a record", e);
            }
        }

        @Override
        public <X> void collect(org.apache.flink.util.OutputTag<X> tag, StreamRecord<X> record) {
            // Side output from a Calc is not a thing; pass it on rather than swallow it.
            real.collect(tag, record);
        }

        @Override
        public void emitWatermark(Watermark mark) {
            try {
                downstream.processWatermark(mark);
            } catch (Exception e) {
                throw new RuntimeException("chained aggregate failed on a watermark", e);
            }
        }

        @Override
        public void emitWatermarkStatus(WatermarkStatus status) {
            try {
                downstream.processWatermarkStatus(status);
            } catch (Exception e) {
                throw new RuntimeException("chained aggregate failed on a watermark status", e);
            }
        }

        /** The generalised watermark channel, which a Calc passes through untouched. */
        @Override
        public void emitWatermark(WatermarkEvent watermark) {
            try {
                downstream.processWatermark(watermark);
            } catch (Exception e) {
                throw new RuntimeException("chained aggregate failed on a watermark event", e);
            }
        }

        @Override
        public void emitLatencyMarker(LatencyMarker marker) {
            try {
                downstream.processLatencyMarker(marker);
            } catch (Exception e) {
                throw new RuntimeException("chained aggregate failed on a latency marker", e);
            }
        }

        @Override
        public void emitRecordAttributes(RecordAttributes attributes) {
            try {
                downstream.processRecordAttributes(attributes);
            } catch (Exception e) {
                throw new RuntimeException("chained aggregate failed on record attributes", e);
            }
        }

        @Override
        public void close() {
            real.close();
        }
    }

    @Override
    public void processElement(StreamRecord<RowData> element) throws Exception {
        calc.processElement(element);
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        calc.processWatermark(mark);
    }

    @Override
    public void processWatermarkStatus(WatermarkStatus status) throws Exception {
        calc.processWatermarkStatus(status);
    }

    @Override
    public void processLatencyMarker(LatencyMarker marker) throws Exception {
        calc.processLatencyMarker(marker);
    }

    @Override
    public void processRecordAttributes(RecordAttributes attributes) throws Exception {
        calc.processRecordAttributes(attributes);
    }

    /**
     * End of input travels the chain in order.
     *
     * <p>The Calc drains first, because whatever it emits on the way out has to reach the aggregate
     * before the aggregate decides it has seen everything. An ungrouped aggregate emits its single
     * row from here, so getting this order wrong loses the last batch rather than failing.
     */
    @Override
    public void endInput() throws Exception {
        if (calc instanceof BoundedOneInput) {
            ((BoundedOneInput) calc).endInput();
        }
        if (aggregate instanceof BoundedOneInput) {
            ((BoundedOneInput) aggregate).endInput();
        }
    }

    @Override
    public void open() throws Exception {
        calc.open();
        aggregate.open();
    }

    @Override
    public void finish() throws Exception {
        calc.finish();
        aggregate.finish();
    }

    @Override
    public void close() throws Exception {
        Exception first = null;
        for (StreamOperator<?> operator : new StreamOperator<?>[] {calc, aggregate}) {
            try {
                operator.close();
            } catch (Exception e) {
                first = first == null ? e : first;
            }
        }
        if (first != null) {
            throw first;
        }
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        calc.prepareSnapshotPreBarrier(checkpointId);
        aggregate.prepareSnapshotPreBarrier(checkpointId);
    }

    @Override
    public OperatorID getOperatorID() {
        return aggregate.getOperatorID();
    }

    @Override
    public void setKeyContextElement(StreamRecord<RowData> record) throws Exception {
        calc.setKeyContextElement(record);
    }

    @Override
    public OperatorSnapshotFutures snapshotState(
            long checkpointId,
            long timestamp,
            org.apache.flink.runtime.checkpoint.CheckpointOptions options,
            org.apache.flink.runtime.state.CheckpointStreamFactory factory)
            throws Exception {
        // Batch only; a checkpoint here would be a bug elsewhere, but delegate rather than throw.
        return aggregate.snapshotState(checkpointId, timestamp, options, factory);
    }

    @Override
    public void initializeState(StreamTaskStateInitializer initializer) throws Exception {
        calc.initializeState(initializer);
        aggregate.initializeState(initializer);
    }

    /** The aggregate's, since that is the operator this vertex is named for. */
    @Override
    public OperatorMetricGroup getMetricGroup() {
        return aggregate.getMetricGroup();
    }

    @Override
    public void setKeyContextElement1(StreamRecord<?> record) throws Exception {
        calc.setKeyContextElement1(record);
    }

    @Override
    public void setKeyContextElement2(StreamRecord<?> record) throws Exception {
        calc.setKeyContextElement2(record);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        calc.notifyCheckpointComplete(checkpointId);
        aggregate.notifyCheckpointComplete(checkpointId);
    }

    @Override
    public void setCurrentKey(Object key) {
        aggregate.setCurrentKey(key);
    }

    @Override
    public Object getCurrentKey() {
        return aggregate.getCurrentKey();
    }
}
