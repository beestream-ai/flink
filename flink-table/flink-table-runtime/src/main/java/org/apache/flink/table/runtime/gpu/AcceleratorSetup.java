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

import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.accelerator.AcceleratorContext;
import org.apache.flink.table.runtime.accelerator.ManagedOffHeapArena;
import org.apache.flink.table.types.logical.RowType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.util.Optional;

/**
 * What every accelerated operator needs from the machine it landed on, whatever its input count.
 *
 * <p>Extracted when the join arrived and not before, which is the right moment: with one factory
 * this was private detail, and with two it is either shared or it drifts. The reservation and the
 * context are exactly the parts that have nothing to do with how many inputs an operator has — the
 * slot's memory and the row it must emit are the same question either way.
 */
final class AcceleratorSetup {

    private static final Logger LOG = LoggerFactory.getLogger(AcceleratorSetup.class);

    private AcceleratorSetup() {}

    /**
     * Reserves the slot's share of managed memory for this operator, or null if there is none.
     *
     * <p>Registered with the task's cancelables rather than closed by the operator: a cancelled
     * task must give its memory back, and an operator cancelled before {@code close} would
     * otherwise hold it for the life of the slot.
     */
    static @Nullable ManagedOffHeapArena reserveStaging(
            Object owner, StreamOperatorParameters<RowData> parameters) {
        try {
            Environment environment = parameters.getContainingTask().getEnvironment();
            MemoryManager memoryManager = environment.getMemoryManager();
            double fraction =
                    parameters
                            .getStreamConfig()
                            .getManagedMemoryFractionOperatorUseCaseOfSlot(
                                    ManagedMemoryUseCase.OPERATOR,
                                    environment.getJobConfiguration(),
                                    environment.getTaskManagerInfo().getConfiguration(),
                                    environment.getUserCodeClassLoader().asClassLoader());
            long budget = memoryManager.computeMemorySize(fraction);

            Optional<ManagedOffHeapArena> arena =
                    ManagedOffHeapArena.reserve(memoryManager, owner, budget);
            if (!arena.isPresent()) {
                LOG.info(
                        "No managed memory for accelerated staging (budget {} bytes); the provider"
                                + " allocates its own and the configured batch size stands",
                        budget);
                return null;
            }
            parameters.getContainingTask().getCancelables().registerCloseable(arena.get());
            return arena.get();
        } catch (Throwable t) {
            // Reserving improves the accounting; it is never a precondition for running.
            LOG.info("Could not reserve managed memory for accelerated staging ({})", t.toString());
            return null;
        }
    }

    /** What this machine tells the provider about the task it is being built into. */
    static AcceleratorContext contextFor(
            RowType outputType,
            int configuredBatchSize,
            int bytesPerRow,
            ClassLoader loader,
            @Nullable ManagedOffHeapArena arena) {

        final int batchSize = batchSizeFor(configuredBatchSize, bytesPerRow, arena);

        return new AcceleratorContext() {
            @Override
            public RowType outputType() {
                return outputType;
            }

            @Override
            public int maxBatchSize() {
                return batchSize;
            }

            @Override
            public ClassLoader userCodeClassLoader() {
                return loader;
            }

            @Override
            public boolean providesOffHeap() {
                return arena != null;
            }

            @Override
            public long stagingBytes() {
                return arena == null ? 0L : arena.reservedBytes();
            }

            @Override
            public ByteBuffer allocateOffHeap(int bytes) {
                if (arena == null) {
                    throw new IllegalStateException(
                            "no managed memory was reserved for this operator, so the provider"
                                    + " must allocate its own staging");
                }
                return arena.allocate(bytes);
            }
        };
    }

    /** The configured ceiling, or what the reserved budget actually fits, whichever is smaller. */
    static int batchSizeFor(
            int configuredBatchSize, int bytesPerRow, @Nullable ManagedOffHeapArena arena) {
        if (arena == null) {
            return configuredBatchSize;
        }
        long fits = arena.reservedBytes() / Math.max(1, bytesPerRow);
        int batchSize = (int) Math.max(1, Math.min(configuredBatchSize, fits));
        LOG.info(
                "Accelerated staging: {} bytes reserved, {} a row, batch size {} (ceiling {})",
                arena.reservedBytes(),
                bytesPerRow,
                batchSize,
                configuredBatchSize);
        return batchSize;
    }
}
