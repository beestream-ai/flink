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

import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.MemoryManagerBuilder;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Staging memory is taken from the slot's budget, bounded by it, and given back. */
class ManagedOffHeapArenaTest {

    private static final long SLOT = 4 * 1024 * 1024;

    private MemoryManager slot() {
        return MemoryManagerBuilder.newBuilder().setMemorySize(SLOT).build();
    }

    @Test
    @DisplayName("what is reserved is taken from the slot and given back on close")
    void reservesAndReleases() {
        MemoryManager memory = slot();
        Object owner = new Object();

        Optional<ManagedOffHeapArena> arena =
                ManagedOffHeapArena.reserve(memory, owner, 1024 * 1024);

        assertThat(arena).isPresent();
        assertThat(memory.availableMemory()).isEqualTo(SLOT - 1024 * 1024);

        arena.get().close();
        assertThat(memory.availableMemory())
                .as("a slot must be as usable after an accelerated task as before it")
                .isEqualTo(SLOT);
    }

    @Test
    @DisplayName("closing twice is harmless, because cancellation and close both reach it")
    void closeIsIdempotent() {
        MemoryManager memory = slot();
        ManagedOffHeapArena arena =
                ManagedOffHeapArena.reserve(memory, new Object(), 1024 * 1024).orElseThrow();

        arena.close();
        arena.close();

        assertThat(memory.availableMemory()).isEqualTo(SLOT);
    }

    @Test
    @DisplayName("buffers come back zeroed, because unsafe memory does not")
    void buffersAreZeroed() {
        ManagedOffHeapArena arena =
                ManagedOffHeapArena.reserve(slot(), new Object(), 1024 * 1024).orElseThrow();

        ByteBuffer buffer = arena.allocate(4096);

        assertThat(buffer.isDirect()).isTrue();
        assertThat(buffer.capacity()).isEqualTo(4096);
        for (int i = 0; i < 4096; i++) {
            assertThat(buffer.get(i)).as("byte %d", i).isZero();
        }
        arena.close();
    }

    @Test
    @DisplayName("the reservation is a promise, so over-allocating fails rather than drifting")
    void refusesMoreThanReserved() {
        ManagedOffHeapArena arena =
                ManagedOffHeapArena.reserve(slot(), new Object(), 8192).orElseThrow();

        arena.allocate(4096);
        arena.allocate(4096);

        assertThatThrownBy(() -> arena.allocate(1))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("exceeds");
        arena.close();
    }

    @Test
    @DisplayName("a slot with nothing to give is an ordinary answer, not a failure")
    void tooLargeAReservationIsEmptyRatherThanThrowing() {
        assertThat(ManagedOffHeapArena.reserve(slot(), new Object(), SLOT * 4)).isEmpty();
    }

    @Test
    @DisplayName("and so is no budget at all, which is every plan compiled before this existed")
    void zeroBudgetIsEmpty() {
        assertThat(ManagedOffHeapArena.reserve(slot(), new Object(), 0)).isEmpty();
        assertThat(ManagedOffHeapArena.reserve(null, new Object(), 1024)).isEmpty();
    }
}
