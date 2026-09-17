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

package org.apache.flink.table.runtime.strategy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.apache.flink.table.runtime.strategy.AcceleratorCardinalityStrategy.rowsPerSubtask;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The arithmetic the cardinality gate turns on, isolated from the graph walk.
 *
 * <p>Whether the strategy finds the right node is covered end to end by the planner's {@code
 * AcceleratorCardinalityIT}, against a real AdaptiveBatch job. What is worth pinning separately is
 * that the divisor is the parallelism handed in — the number the scheduler settled on — because the
 * whole reason M4.3 exists is that the configured one is a request rather than a fact.
 */
class AcceleratorCardinalityStrategyTest {

    @Test
    @DisplayName("the parallelism handed in is the divisor, not a constant")
    void parallelismDividesTheWork() {
        assertThat(rowsPerSubtask(8_000, 1, 8)).isEqualTo(1_000);
        assertThat(rowsPerSubtask(8_000, 4, 8)).isEqualTo(250);
        assertThat(rowsPerSubtask(8_000, 8, 8)).isEqualTo(125);
    }

    /**
     * An undecided parallelism is filtered out by the caller, not absorbed here.
     *
     * <p>AdaptiveBatch reports -1 for a vertex whose parallelism it has not chosen yet, and the
     * strategy declines to decide rather than standing in a 1. Treating -1 as serial overstates
     * what each subtask sees, which lets through the very case the gate exists to refuse -- and a
     * deployment saw it, logging "over -1 subtasks the scheduler chose". The floor below stays as a
     * guard against a caller that forgets, and this test pins that it is only that.
     */
    @Test
    @DisplayName("an unset parallelism cannot silently become a divisor")
    void unsetParallelismIsNotUsable() {
        assertThat(rowsPerSubtask(8_000, -1, 8)).isEqualTo(1_000);
        assertThat(rowsPerSubtask(8_000, 0, 8)).isEqualTo(1_000);
    }

    @Test
    @DisplayName("rounding goes toward the CPU, which is the side that is always correct")
    void roundingFavoursRefusing() {
        // 7 rows' worth of bytes over 2 subtasks is 3.5; claiming 4 would overstate what a subtask
        // sees, and the gate refuses on being under a threshold.
        assertThat(rowsPerSubtask(7 * 8, 2, 8)).isEqualTo(3);
        assertThat(rowsPerSubtask(7, 1, 8)).isZero();
    }

    @Test
    @DisplayName("a row width of zero does not divide by zero")
    void zeroWidthIsSurvivable() {
        assertThat(rowsPerSubtask(8_000, 2, 0)).isEqualTo(4_000);
    }
}
