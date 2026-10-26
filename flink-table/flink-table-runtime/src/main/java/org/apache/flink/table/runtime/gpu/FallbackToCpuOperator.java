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
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.operators.StreamTaskStateInitializer;
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
 * Runs the accelerated operator, and takes the generated one instead if the device fails early.
 *
 * <h2>What "early" means, and why it is not "any time"</h2>
 *
 * <p>A device failure arriving mid-stream cannot be recovered from here. By the time {@code
 * plan.execute()} throws, that batch's rows have been staged into columnar buffers and none of them
 * emitted; they cannot be reconstructed, because the operator holds only the columns the expression
 * referenced. Switching to the generated operator at that point would silently drop them, which is
 * worse than failing — a wrong answer instead of a stopped job.
 *
 * <p>So this covers exactly the window where nothing is at risk: {@link #open()}, and the very
 * first record. That is narrower than "any device failure" and it is deliberately not disguised as
 * more. Recovering later needs the rows kept somewhere they can be replayed, which costs a batch of
 * copies on top of the staging of the same batch, and a way for the provider to say when a batch
 * has been safely emitted. That is recorded as a possible later task rather than assumed to be
 * worth it.
 *
 * <p>The window is worth covering on its own, because it is where the expensive and failure-prone
 * work happens. {@code open()} is what compiles the kernel, allocates device buffers and builds the
 * execution plan — and it is called by the task <em>after</em> the factory has returned, so until
 * now a device that was unavailable by then took the job down, with a code-generated operator
 * sitting in the JobGraph that would have run the query perfectly well.
 */
@Internal
public final class FallbackToCpuOperator
        implements OneInputStreamOperator<RowData, RowData>, BoundedOneInput {

    private static final Logger LOG = LoggerFactory.getLogger(FallbackToCpuOperator.class);

    private final Supplier<OneInputStreamOperator<RowData, RowData>> cpu;

    /**
     * The accelerated operator's staging memory, given back before the generated one is built.
     *
     * <p>Not politeness. Staging reserves the slot's whole operator share of managed memory, and
     * the operator this falls back to is the one Flink generated -- which for anything above a
     * projection wants managed memory of its own: a hash table, a sorter, a spilling buffer. Both
     * cannot hold it. A fallback that does not release first dies asking for memory the operator it
     * is replacing is still holding, and takes the job down, which is the opposite of what this
     * class exists for.
     */
    private final @Nullable Closeable staging;

    private OneInputStreamOperator<RowData, RowData> delegate;

    /**
     * The initializer the task gave this wrapper, kept for an operator substituted later.
     *
     * <p>{@code StreamTask} calls {@code initializeState} once, before {@code open()}, and it
     * reaches whichever operator is the delegate at that moment -- the accelerated one. The
     * generated operator that replaces it on fallback is built afterwards, so without this it would
     * run having never been initialised: {@code stateHandler} and {@code timeServiceManager} null,
     * {@code snapshotState} dereferencing the first of them, and any delegate that touches state or
     * timers in {@code open()} failing there. That turns the guard meant to rescue the task into a
     * second way to lose it.
     */
    private @Nullable StreamTaskStateInitializer stateInitializer;

    private boolean accelerated = true;

    /** Which of the two safe fallback points was taken, or {@code ACCELERATED} if neither. */
    private AcceleratorDeclineReason declineReason = AcceleratorDeclineReason.ACCELERATED;

    private boolean sawFirstRecord;

    FallbackToCpuOperator(
            OneInputStreamOperator<RowData, RowData> accelerated,
            Supplier<OneInputStreamOperator<RowData, RowData>> cpu,
            @Nullable Closeable staging) {
        this.delegate = accelerated;
        this.cpu = cpu;
        this.staging = staging;
    }

    /**
     * Builds the accelerated operator, or the generated one if that fails.
     *
     * <p>Only reached with the device operator in hand, so a failure here means the device was
     * reachable when the task was deployed and is not now — which is exactly the case worth
     * surviving, and the case that used to be fatal.
     */
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
    public void processElement(StreamRecord<RowData> element) throws Exception {
        if (sawFirstRecord || !accelerated) {
            delegate.processElement(element);
            return;
        }
        // The first record is the last moment nothing is at risk: it is still in hand, so it can
        // be handed to the generated operator instead. After it, a failure may have consumed rows
        // this operator cannot give back.
        try {
            delegate.processElement(element);
            sawFirstRecord = true;
        } catch (Throwable t) {
            switchToCpu(
                    AcceleratorDeclineReason.FELL_BACK_AT_FIRST_RECORD,
                    "the accelerated operator failed on its first record",
                    t);
            delegate.open();
            delegate.processElement(element);
            sawFirstRecord = true;
        }
    }

    /** Logged once, at WARN: a job quietly running on the CPU it was told would use a device. */
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
        if (stateInitializer != null) {
            // The replacement is built after the task's one call to initializeState, so
            // it is initialised here or not at all.
            delegate.initializeState(stateInitializer);
        }
        accelerated = false;
    }

    /** Whether a device is still serving this subtask. Reported as a metric by the factory. */
    public boolean isAccelerated() {
        return accelerated;
    }

    /** The same fact as a value: which fallback point was taken, if any. */
    public AcceleratorDeclineReason declineReason() {
        return declineReason;
    }

    @Override
    public void endInput() throws Exception {
        if (delegate instanceof BoundedOneInput) {
            ((BoundedOneInput) delegate).endInput();
        }
    }

    // ------------------------------------------------------------------------------------------
    // Everything below is delegation.
    // ------------------------------------------------------------------------------------------

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        delegate.processWatermark(mark);
    }

    @Override
    public void processWatermarkStatus(WatermarkStatus status) throws Exception {
        delegate.processWatermarkStatus(status);
    }

    @Override
    public void processLatencyMarker(LatencyMarker marker) throws Exception {
        delegate.processLatencyMarker(marker);
    }

    @Override
    public void processRecordAttributes(RecordAttributes attributes) throws Exception {
        delegate.processRecordAttributes(attributes);
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
    public void setKeyContextElement(StreamRecord<RowData> record) throws Exception {
        delegate.setKeyContextElement(record);
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
        this.stateInitializer = initializer;
        delegate.initializeState(initializer);
    }

    @Override
    public OperatorMetricGroup getMetricGroup() {
        return delegate.getMetricGroup();
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

    /** Builds one, given the accelerated operator and a way to make the generated one. */
    /**
     * Gives the staging memory back, after the accelerated operator is done with it and before the
     * generated one asks for any.
     *
     * <p>Ordered, not incidental: the buffers staging handed out are freed by this, and the
     * accelerated operator is closed just above. Releasing earlier would pull memory out from under
     * a kernel that is still running.
     */
    private void releaseStaging() {
        if (staging == null) {
            return;
        }
        try {
            staging.close();
        } catch (Throwable t) {
            // The generated operator may still fit in what is left; let it try and say so.
            LOG.warn("could not release accelerated staging memory before falling back", t);
        }
    }

    static FallbackToCpuOperator around(
            OneInputStreamOperator<RowData, RowData> accelerated,
            StreamOperatorFactory<RowData> cpuFactory,
            StreamOperatorParameters<RowData> parameters,
            @Nullable Closeable staging) {
        return new FallbackToCpuOperator(
                accelerated, () -> cpuFactory.createStreamOperator(parameters), staging);
    }
}
