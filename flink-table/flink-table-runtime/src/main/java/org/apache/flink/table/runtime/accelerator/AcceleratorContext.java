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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.types.logical.RowType;

import java.nio.ByteBuffer;

/**
 * What a provider is told about the task it is being asked to run in.
 *
 * <p>Supplied on the TaskManager, where these facts exist. None of them are knowable to the
 * planner: the row type is, but the memory budget and the classloader are properties of the slot
 * the scheduler happened to choose.
 */
@PublicEvolving
public interface AcceleratorContext {

    /** The row the operator must emit. */
    RowType outputType();

    /**
     * The largest batch the provider may stage, in rows.
     *
     * <p>A ceiling rather than an instruction. It is derived from the slot's memory budget and the
     * row width, so a provider that wants smaller batches may use them; one that wants larger may
     * not.
     */
    int maxBatchSize();

    /** The loader that supplied the provider, for anything it must load reflectively. */
    ClassLoader userCodeClassLoader();

    /**
     * Whether {@link #allocateOffHeap} will work here.
     *
     * <p>False is ordinary: a plan compiled before Flink declared managed memory for this operator,
     * a slot too short to reserve any, a test harness with no slot behind it at all. A provider
     * that sees false should allocate for itself, exactly as it did before this existed.
     *
     * <p>Asking is the point. The alternative is a speculative allocation to see whether one works,
     * which spends budget to learn something the caller could simply have been told.
     */
    boolean providesOffHeap();

    /**
     * Off-heap memory for staging, from the slot's managed budget.
     *
     * <p>A provider should stage through this rather than allocating for itself, and the reason is
     * accounting rather than convenience. Memory a provider allocates privately is invisible to
     * Flink: the slot's budget does not know about it, two accelerated operators sharing a slot
     * each believe they have the whole machine, and the only way to make a cluster survive was to
     * hand-configure an off-heap figure per deployment. Memory taken from here is reserved against
     * the managed budget the transformation declared, counted against the slot, and released when
     * the task finishes whether it succeeds, fails or is cancelled.
     *
     * <p>Returned as a {@link ByteBuffer} because that is the widest currency available. It is
     * direct and its address is stable, so a provider can hand it to whatever its own runtime wants
     * — a {@code java.lang.foreign.MemorySegment}, a JNI pointer, a device mapping. Flink
     * deliberately names none of those: it is compiled for a language level where some of them are
     * preview features, and an SPI that named one would exclude the providers that use another.
     *
     * <p>The buffer is zeroed. Do not free it; do not use it after the operator closes.
     *
     * @param bytes how many bytes, which must not exceed what {@link #maxBatchSize} implies
     * @throws IllegalStateException if the budget cannot cover the request
     */
    ByteBuffer allocateOffHeap(int bytes);

    /**
     * Total bytes {@link #allocateOffHeap} can hand out, or 0 where nothing was reserved.
     *
     * <p>{@link #maxBatchSize} answers the question a per-row operator asks — how many rows may be
     * in flight at once — and it is capped by the configured batch size, because a Calc that stages
     * more than that gains nothing. An operator that must hold its <em>whole</em> input is asking a
     * different question: not how large a batch may be, but how large an input it can undertake to
     * keep. A sort is the first of those, and the configured batch size is the wrong bound for it —
     * sorting a batch at a time gives sorted runs that still have to be merged.
     *
     * <p>So this reports the reservation itself and lets the provider divide by whatever a row
     * costs in its own layout, which Flink does not know. Zero is ordinary and means the same thing
     * {@link #providesOffHeap} returning false means: allocate privately, or decline.
     */
    default long stagingBytes() {
        return 0L;
    }
}
