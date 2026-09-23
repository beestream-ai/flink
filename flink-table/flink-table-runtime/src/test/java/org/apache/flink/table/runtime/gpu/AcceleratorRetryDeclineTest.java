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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Whether a retried task is offered to an accelerator again.
 *
 * <p>It is not, and the reason is that the interesting device failures are deterministic. A kernel
 * that will not compile on this machine, a driver a restart does not clear, a partition larger than
 * the provider undertook to hold: each of them kills every attempt in exactly the same way, so
 * retrying on the same path spends the job's whole restart budget rediscovering one fact and then
 * fails the job — with a code-generated operator that would have finished sitting unused in the
 * JobGraph the entire time.
 *
 * <p>The rule is tested apart from where the attempt number comes from. Reading it is one call
 * through the task's {@code Environment}, and there is no test harness that produces an environment
 * claiming to be a retry; wrapping the rule so it can be stated as a rule is the trade.
 */
class AcceleratorRetryDeclineTest {

    @Test
    @DisplayName("the first attempt has no history to be suspicious of")
    void firstAttemptIsOffered() {
        assertThat(GpuOrCpuCalcOperatorFactory.retryAttempt(0)).isNull();
    }

    @Test
    @DisplayName("every later attempt runs on the CPU, and says which attempt it was")
    void laterAttemptsDecline() {
        assertThat(GpuOrCpuCalcOperatorFactory.retryAttempt(1)).isNotNull().contains("attempt 1");
        assertThat(GpuOrCpuCalcOperatorFactory.retryAttempt(7)).isNotNull().contains("attempt 7");
    }

    @Test
    @DisplayName("parallelism above the supported ceiling declines before the device is touched")
    void parallelismAboveCeilingDeclines() {
        // Containment for an unexplained intermittent CUDA launch failure at parallelism above 1.
        // The point is that the refusal happens before execution, not that the number is right.
        assertThat(GpuOrCpuCalcOperatorFactory.aboveSupportedParallelism(1, 1)).isNull();
        assertThat(GpuOrCpuCalcOperatorFactory.aboveSupportedParallelism(2, 1))
                .isNotNull()
                .contains("parallelism 2")
                .contains("table.exec.accelerator.max-parallelism");
        assertThat(GpuOrCpuCalcOperatorFactory.aboveSupportedParallelism(8, 4)).isNotNull();
        assertThat(GpuOrCpuCalcOperatorFactory.aboveSupportedParallelism(4, 4)).isNull();
    }

    @Test
    @DisplayName("a ceiling of zero or less disables the containment rather than refusing all")
    void nonPositiveCeilingDisablesTheRule() {
        // So that an operator who has established their own limit can switch it off entirely
        // without the rule silently refusing every subtask.
        assertThat(GpuOrCpuCalcOperatorFactory.aboveSupportedParallelism(64, 0)).isNull();
        assertThat(GpuOrCpuCalcOperatorFactory.aboveSupportedParallelism(64, -1)).isNull();
    }

    @Test
    @DisplayName("a negative attempt number is treated as a first attempt, not as a retry")
    void nonsenseIsNotARetry() {
        // Nothing produces one, but refusing to accelerate on a number nobody set would disable
        // the feature silently rather than loudly.
        assertThat(GpuOrCpuCalcOperatorFactory.retryAttempt(-1)).isNull();
    }
}
