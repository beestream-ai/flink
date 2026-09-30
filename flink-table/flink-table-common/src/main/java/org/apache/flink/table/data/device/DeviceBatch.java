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

package org.apache.flink.table.data.device;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.types.logical.RowType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A batch of rows that live on an accelerator, described to Flink but not readable by it.
 *
 * <p>What travels between an offloaded source and the operator above it. Today that operator
 * receives {@code RowData}, so the source decodes Parquet on the CPU and the operator stages every
 * field back into a device array: measured at ~4.3 s of decode and ~4.6 s of staging against
 * ~57 ms of device execution, for 8M rows of 31 columns. This type is how those two stages stop
 * happening — the rows are produced where they are used and never take the round trip.
 *
 * <h2>What it does not offer</h2>
 *
 * <p>No accessor returns a value, a row, or anything derived from the data. Flink could only
 * implement one by copying the column back, which would reintroduce exactly the cost this exists
 * to remove, and it would do so invisibly at whatever call site happened to ask. A consumer that
 * needs values on the host asks for the transition operator instead, which states the cost.
 *
 * <p>It is deliberately <b>not</b> {@code Serializable}. A device pointer is meaningless in
 * another process and usually in another thread, so a batch cannot travel in a JobGraph or through
 * a shuffle. Implementing the interface would let a caller try, and fail somewhere far away from
 * the mistake.
 *
 * <h2>Ownership</h2>
 *
 * <p>A batch is owned by whoever holds it. Receiving one transfers ownership, and the receiver
 * closes it. {@link #close()} releases every buffer of every column, and may be called more than
 * once — the failure path that closes a batch cannot know whether the normal path already did.
 * After {@code close()} the batch reports no columns and its handles must not be used.
 */
@PublicEvolving
public final class DeviceBatch implements AutoCloseable {

    private final RowType rowType;

    private final List<DeviceColumn> columns;

    private final int rowCount;

    private boolean closed;

    public DeviceBatch(RowType rowType, List<DeviceColumn> columns, int rowCount) {
        this.rowType = Objects.requireNonNull(rowType, "rowType");
        this.columns =
                Collections.unmodifiableList(
                        new ArrayList<>(Objects.requireNonNull(columns, "columns")));
        if (rowCount < 0) {
            throw new IllegalArgumentException("rowCount must not be negative: " + rowCount);
        }
        if (this.columns.size() != rowType.getFieldCount()) {
            throw new IllegalArgumentException(
                    "row type has "
                            + rowType.getFieldCount()
                            + " fields but "
                            + this.columns.size()
                            + " columns were given");
        }
        for (int i = 0; i < this.columns.size(); i++) {
            LogicalTypeCheck.sameType(rowType, this.columns.get(i), i);
        }
        this.rowCount = rowCount;
    }

    /** How many rows the batch holds. Known without reading any of them. */
    public int rowCount() {
        return rowCount;
    }

    /** The row this batch is a batch of. */
    public RowType rowType() {
        return rowType;
    }

    /** The columns, in the row type's field order. Empty once closed. */
    public List<DeviceColumn> columns() {
        return closed ? Collections.emptyList() : columns;
    }

    public DeviceColumn column(int index) {
        if (closed) {
            throw new IllegalStateException("batch is closed");
        }
        return columns.get(index);
    }

    /** Whether {@link #close()} has run. */
    public boolean isClosed() {
        return closed;
    }

    /** Total device memory the batch occupies, across every column and mask. */
    public long sizeInBytes() {
        long total = 0;
        for (DeviceColumn column : columns) {
            total += column.values().sizeInBytes();
            total += column.validity().map(DeviceBuffer::sizeInBytes).orElse(0L);
        }
        return total;
    }

    /**
     * Releases every buffer this batch owns.
     *
     * <p>Idempotent: a second call does nothing rather than freeing twice. Every buffer is
     * released even if one of them throws, and the first failure is what the caller sees.
     */
    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        RuntimeException failure = null;
        for (DeviceColumn column : columns) {
            failure = releaseQuietly(column.values(), failure);
            if (column.validity().isPresent()) {
                failure = releaseQuietly(column.validity().get(), failure);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private static RuntimeException releaseQuietly(DeviceBuffer buffer, RuntimeException first) {
        try {
            buffer.release();
            return first;
        } catch (RuntimeException e) {
            if (first == null) {
                return e;
            }
            first.addSuppressed(e);
            return first;
        }
    }

    @Override
    public String toString() {
        return "DeviceBatch("
                + rowCount
                + " rows, "
                + columns.size()
                + " columns"
                + (closed ? ", closed" : "")
                + ")";
    }

    /** Kept separate so the constructor reads as a list of checks rather than a block of them. */
    private static final class LogicalTypeCheck {
        static void sameType(RowType rowType, DeviceColumn column, int index) {
            if (!rowType.getTypeAt(index).equals(column.type())) {
                throw new IllegalArgumentException(
                        "column "
                                + index
                                + " is "
                                + column.type()
                                + " but the row type declares "
                                + rowType.getTypeAt(index));
            }
        }

        private LogicalTypeCheck() {}
    }
}
