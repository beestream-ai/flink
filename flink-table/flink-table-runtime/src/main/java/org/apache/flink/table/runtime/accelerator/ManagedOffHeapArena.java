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

package org.apache.flink.table.runtime.accelerator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.MemoryReservationException;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Staging memory for one accelerated operator, taken from the slot's managed budget.
 *
 * <h2>Why Flink allocates it rather than the provider</h2>
 *
 * <p>A provider allocating for itself is invisible to Flink. The slot's budget does not know about
 * it, two accelerated operators in one slot each behave as though they had the machine to
 * themselves, and the only way to make a cluster survive was to hand-configure {@code
 * taskmanager.memory.task.off-heap.size} per deployment and hope. Taking it from here makes it a
 * declared cost: the transformation says how much it wants, the {@link MemoryManager} says how much
 * of the slot that is, and the reservation fails loudly rather than the JVM failing obscurely.
 *
 * <h2>Unsafe, not direct</h2>
 *
 * <p>{@link MemorySegmentFactory#allocateOffHeapUnsafeMemory} rather than the direct-buffer
 * variant, and the difference is the whole of the third thing this fixes. A direct {@code
 * ByteBuffer} counts against {@code -XX:MaxDirectMemorySize}, which Flink derives from the task
 * off-heap configuration — so staging through one meant every deployment had to be told, by hand,
 * how much off-heap its accelerator would want, and a job that guessed low died with {@code
 * OutOfMemoryError: Direct buffer memory} in a place that said nothing about accelerators. Unsafe
 * memory sits outside that limit, which is exactly why Flink's own managed memory uses it.
 *
 * <h2>Lifetime</h2>
 *
 * <p>Registered with the task's {@code CloseableRegistry}, so it is released when the task
 * finishes, fails <em>or</em> is cancelled. Tying it to the operator's {@code close} would leak
 * everything on a cancellation, which is the case that matters: a slot whose accelerated task was
 * cancelled must be usable by the next one.
 */
@Internal
public final class ManagedOffHeapArena implements Closeable {

    /**
     * How much is zeroed per bulk write.
     *
     * <p>A scratch array rather than one the size of the arena: the point is to leave the per-byte
     * loop, not to allocate a second buffer as large as the first. At this size the copy runs at
     * memory bandwidth and the array is small enough to stay in cache.
     */
    private static final int ZEROING_CHUNK_BYTES = 64 * 1024;

    private final MemoryManager memoryManager;
    private final Object owner;
    private final long reserved;
    private final List<MemorySegment> segments = new ArrayList<>();

    private long handedOut;
    private boolean closed;

    private ManagedOffHeapArena(MemoryManager memoryManager, Object owner, long reserved) {
        this.memoryManager = memoryManager;
        this.owner = owner;
        this.reserved = reserved;
    }

    /**
     * Reserves {@code bytes} of the slot's managed memory, or returns empty if it cannot.
     *
     * <p>Empty is an ordinary answer. A budget of zero means the transformation declared no managed
     * memory — which is true of any plan built before this existed, and of a test harness that
     * builds an operator without a slot — and a reservation that fails means the slot is genuinely
     * short. Neither should take a query down: the caller falls back to the configured batch size
     * and the provider allocates privately, which is exactly what happened before.
     */
    public static java.util.Optional<ManagedOffHeapArena> reserve(
            MemoryManager memoryManager, Object owner, long bytes) {
        if (memoryManager == null || bytes <= 0) {
            return java.util.Optional.empty();
        }
        try {
            memoryManager.reserveMemory(owner, bytes);
        } catch (MemoryReservationException e) {
            return java.util.Optional.empty();
        }
        return java.util.Optional.of(new ManagedOffHeapArena(memoryManager, owner, bytes));
    }

    /** How much was reserved, which is what a batch size should be derived from. */
    public long reservedBytes() {
        return reserved;
    }

    /**
     * Hands out a zeroed off-heap buffer.
     *
     * <p>Throws rather than over-allocating when the request would exceed what was reserved. The
     * reservation is the promise made to the slot, and quietly breaking it would put the accounting
     * back where it started.
     */
    public ByteBuffer allocate(int bytes) {
        if (closed) {
            throw new IllegalStateException("arena is closed");
        }
        if (bytes <= 0) {
            throw new IllegalArgumentException("asked for " + bytes + " bytes");
        }
        if (handedOut + bytes > reserved) {
            throw new IllegalStateException(
                    "accelerator asked for "
                            + bytes
                            + " bytes on top of "
                            + handedOut
                            + " already taken, which exceeds the "
                            + reserved
                            + " reserved for this operator");
        }
        MemorySegment segment = MemorySegmentFactory.allocateOffHeapUnsafeMemory(bytes);
        segments.add(segment);
        handedOut += bytes;
        ByteBuffer buffer = segment.getOffHeapBuffer();
        // Unsafe memory is not zeroed on allocation, and a provider is entitled to assume the
        // buffer it is handed holds nothing. The tail of a partial batch is the case that cares.
        //
        // In chunks rather than a byte at a time: this runs over the slot's whole operator
        // share of managed memory, which is routinely hundreds of megabytes, and it sits on
        // the path that builds the operator. A per-byte loop costs roughly a nanosecond a
        // byte -- half a second for a 512 MiB arena -- charged against a feature whose point
        // is latency.
        final byte[] zeros = new byte[Math.min(bytes, ZEROING_CHUNK_BYTES)];
        for (int offset = 0; offset < bytes; offset += zeros.length) {
            segment.put(offset, zeros, 0, Math.min(zeros.length, bytes - offset));
        }
        return buffer;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        for (MemorySegment segment : segments) {
            segment.free();
        }
        segments.clear();
        memoryManager.releaseMemory(owner, reserved);
    }
}
