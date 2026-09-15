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

import java.io.Serializable;

/**
 * What a provider claims serving a subtree will cost on its own hardware.
 *
 * <h2>Why a ratio and not nanoseconds</h2>
 *
 * <p>An absolute figure would oblige Flink to hold a CPU rate to compare it against -- another
 * calibrated hardware constant, merely for the other processor. A ratio needs none: the provider
 * states how much faster it expects to be than a scalar CPU executing the same {@code
 * AccelWorkProfile}, and Flink's policy is a threshold on that claim.
 *
 * <p>That threshold is a <em>policy</em> constant -- how much margin Flink wants before preferring
 * an unfamiliar execution path -- and it means the same thing on a GPU, an FPGA and whatever comes
 * next. It is not a device constant, which is why it can live in Flink when {@code
 * MIN_ROW_COST = 96} could not.
 *
 * <p>The two absolute figures that remain are ones only the provider can know and that no ratio
 * can express: what it costs before a single row is processed, and what it costs per batch.
 */
@PublicEvolving
public final class AcceleratorCost implements Serializable {

    private static final long serialVersionUID = 1L;

    private final double speedupOverCpu;
    private final long fixedCostNanos;
    private final long perBatchNanos;

    public AcceleratorCost(double speedupOverCpu, long fixedCostNanos, long perBatchNanos) {
        if (!(speedupOverCpu > 0.0) || Double.isInfinite(speedupOverCpu)) {
            throw new IllegalArgumentException(
                    "speedupOverCpu must be finite and positive, was " + speedupOverCpu);
        }
        if (fixedCostNanos < 0 || perBatchNanos < 0) {
            throw new IllegalArgumentException("costs must be non-negative");
        }
        this.speedupOverCpu = speedupOverCpu;
        this.fixedCostNanos = fixedCostNanos;
        this.perBatchNanos = perBatchNanos;
    }

    /**
     * Claimed throughput relative to a scalar CPU doing the same work.
     *
     * <p>Below 1.0 means the provider expects to be slower and is saying so, which is a useful
     * answer: it lets a provider accept a subtree it can express while declining to win on it.
     */
    public double speedupOverCpu() {
        return speedupOverCpu;
    }

    /** Paid once per subtask before any row: compiling a kernel, allocating buffers. */
    public long fixedCostNanos() {
        return fixedCostNanos;
    }

    /** Paid per batch whatever the batch contains: transfer, dispatch, synchronisation. */
    public long perBatchNanos() {
        return perBatchNanos;
    }

    @Override
    public String toString() {
        return "AcceleratorCost[speedup="
                + speedupOverCpu
                + "x, fixed="
                + fixedCostNanos / 1_000_000
                + "ms, perBatch="
                + perBatchNanos / 1_000
                + "us]";
    }
}
