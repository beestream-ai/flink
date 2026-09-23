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
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.api.operators.InputSelectable;
import org.apache.flink.streaming.api.operators.InputSelection;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.operators.StreamTaskStateInitializer;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.RecordAttributes;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.table.accelerator.AcceleratorDeclineReason;
import org.apache.flink.table.data.RowData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.util.function.Supplier;

/**
 * {@link FallbackToCpuOperator} for a two-input operator, and narrower than it by one case.
 *
 * <h2>The window is smaller here, and the reason is the build side</h2>
 *
 * <p>The one-input guard covers {@code open()} and the first record, because until the first record
 * has been handed on nothing has been consumed that cannot be handed to the generated operator
 * instead. A join reads its build side to completion before it probes anything, so that argument
 * survives exactly one record: after the second build row, the accelerated operator is holding rows
 * the generated one would need and has no way to be given.
 *
 * <p>So this covers {@code open()} and the first record of the <em>build</em> input, and nothing on
 * the probe input at all. That is not a weaker guarantee than the Calc's in the place it matters —
 * {@code open()} is still where the device context is created, the kernel compiled and the buffers
 * allocated, which is where a machine without a working device actually fails — but it is narrower,
 * and it is narrower for a structural reason rather than an unfinished one.
 *
 * <p>A device failure after that is the accelerated operator's own to survive. The join holds its
 * whole build side, so it can finish on the host with what it has; that recovery belongs to the
 * operator, which knows what it is holding, and not here, which does not.
 */
@Internal
public final class FallbackToCpuTwoInputOperator
        implements TwoInputStreamOperator<RowData, RowData, RowData>,
                BoundedMultiInput,
                InputSelectable {

    private static final Logger LOG = LoggerFactory.getLogger(FallbackToCpuTwoInputOperator.class);

    private final Supplier<TwoInputStreamOperator<RowData, RowData, RowData>> cpu;

    /** The accelerated operator's staging, given back before the generated one is built. */
    private final @Nullable Closeable staging;

    private TwoInputStreamOperator<RowData, RowData, RowData> delegate;
    private boolean accelerated = true;

    /** Which of the two safe fallback points was taken, or {@code ACCELERATED} if neither. */
    private AcceleratorDeclineReason declineReason = AcceleratorDeclineReason.ACCELERATED;

    private boolean sawFirstBuildRecord;

    FallbackToCpuTwoInputOperator(
            TwoInputStreamOperator<RowData, RowData, RowData> accelerated,
            Supplier<TwoInputStreamOperator<RowData, RowData, RowData>> cpu,
            @Nullable Closeable staging) {
        this.delegate = accelerated;
        this.cpu = cpu;
        this.staging = staging;
    }

    static FallbackToCpuTwoInputOperator around(
            TwoInputStreamOperator<RowData, RowData, RowData> accelerated,
            StreamOperatorFactory<RowData> cpuFactory,
            StreamOperatorParameters<RowData> parameters,
            @Nullable Closeable staging) {
        return new FallbackToCpuTwoInputOperator(
                accelerated,
                () -> {
                    StreamOperator<RowData> built = cpuFactory.createStreamOperator(parameters);
                    @SuppressWarnings("unchecked")
                    TwoInputStreamOperator<RowData, RowData, RowData> twoInput =
                            (TwoInputStreamOperator<RowData, RowData, RowData>) built;
                    return twoInput;
                },
                staging);
    }

    @Override
    public void open() throws Exception {
        try {
            delegate.open();
        } catch (Throwable t) {
            switchToCpu(
                    AcceleratorDeclineReason.FELL_BACK_AT_OPEN,
                    "opening the accelerated operator failed",
                    t);
            delegate.open();
        }
    }

    @Override
    public void processElement1(StreamRecord<RowData> element) throws Exception {
        if (sawFirstBuildRecord || !accelerated) {
            delegate.processElement1(element);
            return;
        }
        try {
            delegate.processElement1(element);
            sawFirstBuildRecord = true;
        } catch (Throwable t) {
            switchToCpu(
                    AcceleratorDeclineReason.FELL_BACK_AT_FIRST_RECORD,
                    "the accelerated operator failed on its first build record",
                    t);
            delegate.open();
            delegate.processElement1(element);
            sawFirstBuildRecord = true;
        }
    }

    @Override
    public void processElement2(StreamRecord<RowData> element) throws Exception {
        // No guard. By the time a probe row arrives the whole build side has been consumed, and
        // handing this one to the generated operator would give it a probe row and no table.
        delegate.processElement2(element);
    }

    /**
     * Whether a device is still serving this subtask.
     *
     * <p>Read through rather than captured, because the accelerated join may stop using the device
     * on its own — its build side outgrowing the staging is a runtime answer, not a startup one —
     * and a subtask that has stopped must stop claiming otherwise.
     */
    public boolean isAccelerated() {
        if (!accelerated) {
            return false;
        }
        return !(delegate instanceof SelfReportingAccelerator)
                || ((SelfReportingAccelerator) delegate).stillOnDevice();
    }

    /**
     * The same fact as a value: which fallback point was taken, if any.
     *
     * <p>Read the same way {@link #isAccelerated()} is, so that an operator which left the device
     * on its own is reported as that rather than as still accelerated.
     */
    public AcceleratorDeclineReason declineReason() {
        if (declineReason != AcceleratorDeclineReason.ACCELERATED) {
            return declineReason;
        }
        return isAccelerated()
                ? AcceleratorDeclineReason.ACCELERATED
                : AcceleratorDeclineReason.PROVIDER_STOPPED_USING_DEVICE;
    }

    /**
     * Implemented by an accelerated operator that can stop using its device without failing.
     *
     * <p>A one-input Calc cannot: a failure mid-batch has consumed rows it cannot reproduce, so
     * either it works or the task dies. An operator holding its whole input can, and then the
     * device gauge has to be able to tell — otherwise a job that quietly finished on the host reads
     * as an accelerated one that was merely disappointing.
     */
    public interface SelfReportingAccelerator {
        boolean stillOnDevice();
    }

    private void switchToCpu(AcceleratorDeclineReason why, String what, Throwable cause)
            throws Exception {
        declineReason = why;
        LOG.warn(
                "{}; this subtask will finish on the generated operator instead. The accelerator"
                        + " is not used again for this task.",
                what,
                cause);
        try {
            delegate.close();
        } catch (Throwable ignored) {
            // Already failing; a failure while closing the failed operator adds nothing.
        }
        releaseStaging();
        delegate = cpu.get();
        accelerated = false;
    }

    private void releaseStaging() {
        if (staging == null) {
            return;
        }
        try {
            staging.close();
        } catch (Throwable t) {
            LOG.warn("Could not release the accelerator's staging memory", t);
        }
    }

    // ------------------------------------------------------------------------------------------
    // Everything below is delegation.
    // ------------------------------------------------------------------------------------------

    @Override
    public InputSelection nextSelection() {
        // Flink reports the generated operator's class to the client, so a task built for this
        // node asks for a selection whatever is actually running. Both implementations read the
        // build side first; an accelerated operator that did not would be answering a different
        // question from the plan it was substituted into.
        return delegate instanceof InputSelectable
                ? ((InputSelectable) delegate).nextSelection()
                : InputSelection.ALL;
    }

    @Override
    public void endInput(int inputId) throws Exception {
        if (delegate instanceof BoundedMultiInput) {
            ((BoundedMultiInput) delegate).endInput(inputId);
        }
    }

    @Override
    public void processWatermark1(Watermark mark) throws Exception {
        delegate.processWatermark1(mark);
    }

    @Override
    public void processWatermark2(Watermark mark) throws Exception {
        delegate.processWatermark2(mark);
    }

    @Override
    public void processWatermarkStatus1(WatermarkStatus status) throws Exception {
        delegate.processWatermarkStatus1(status);
    }

    @Override
    public void processWatermarkStatus2(WatermarkStatus status) throws Exception {
        delegate.processWatermarkStatus2(status);
    }

    @Override
    public void processLatencyMarker1(LatencyMarker marker) throws Exception {
        delegate.processLatencyMarker1(marker);
    }

    @Override
    public void processLatencyMarker2(LatencyMarker marker) throws Exception {
        delegate.processLatencyMarker2(marker);
    }

    @Override
    public void processRecordAttributes1(RecordAttributes attributes) throws Exception {
        delegate.processRecordAttributes1(attributes);
    }

    @Override
    public void processRecordAttributes2(RecordAttributes attributes) throws Exception {
        delegate.processRecordAttributes2(attributes);
    }

    @Override
    public void finish() throws Exception {
        delegate.finish();
    }

    @Override
    public void close() throws Exception {
        delegate.close();
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        delegate.prepareSnapshotPreBarrier(checkpointId);
    }

    @Override
    public OperatorID getOperatorID() {
        return delegate.getOperatorID();
    }

    @Override
    public void setKeyContextElement1(StreamRecord<?> record) throws Exception {
        delegate.setKeyContextElement1(record);
    }

    @Override
    public void setKeyContextElement2(StreamRecord<?> record) throws Exception {
        delegate.setKeyContextElement2(record);
    }

    @Override
    public OperatorSnapshotFutures snapshotState(
            long checkpointId,
            long timestamp,
            CheckpointOptions options,
            CheckpointStreamFactory factory)
            throws Exception {
        return delegate.snapshotState(checkpointId, timestamp, options, factory);
    }

    @Override
    public void initializeState(StreamTaskStateInitializer initializer) throws Exception {
        delegate.initializeState(initializer);
    }

    @Override
    public OperatorMetricGroup getMetricGroup() {
        return delegate.getMetricGroup();
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        delegate.notifyCheckpointComplete(checkpointId);
    }

    @Override
    public void setCurrentKey(Object key) {
        delegate.setCurrentKey(key);
    }

    @Override
    public Object getCurrentKey() {
        return delegate.getCurrentKey();
    }

    /** The operator actually running, for tests and for the factory's reporting. */
    public StreamOperator<RowData> current() {
        return delegate;
    }
}
