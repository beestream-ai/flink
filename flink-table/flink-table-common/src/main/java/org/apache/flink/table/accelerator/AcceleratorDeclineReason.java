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

package org.apache.flink.table.accelerator;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Why a subtask did or did not run on an accelerator, as a value rather than a sentence.
 *
 * <p>The sentence is still produced and still published — it carries the numbers, the provider's
 * name and the threshold it missed, and none of that fits in an enum. What the sentence cannot do
 * is be counted. "How often does this cluster decline for want of a device, as against declining
 * because the provider thought the work too small?" is a question about proportions across many
 * subtasks, and answering it by matching substrings against a log line is how a reason ends up
 * being load-bearing prose that nobody may reword.
 *
 * <p>So every outcome has both: a constant here for the metric and the histogram, and free text
 * beside it for the person reading one line about one subtask.
 *
 * <p>This is the <em>runtime</em> vocabulary — what the TaskManager decided, per subtask, against
 * the hardware it turned out to have. The planner has its own reasons for not selecting a node at
 * all, and those are properties of the query rather than of a machine; they are reported through
 * {@code EXPLAIN} and are deliberately not mixed in here.
 */
@PublicEvolving
public enum AcceleratorDeclineReason {

    /** A provider served this subtask, and was still serving it when the task finished. */
    ACCELERATED,

    /**
     * Nothing implementing the SPI was on this TaskManager's classpath.
     *
     * <p>The ordinary outcome on most machines in most clusters, and not an error. It is also the
     * outcome that most often surprises someone who read an {@code EXPLAIN} saying otherwise.
     */
    NO_PROVIDER,

    /**
     * A provider was asked and answered no.
     *
     * <p>Covers every reason a provider keeps to itself: no device present, a driver that did not
     * load, an operation it cannot express, a shape it does not want. Flink does not distinguish
     * them because the SPI deliberately does not ask.
     */
    PROVIDER_DECLINED,

    /** A provider threw from {@code accept}, which the SPI forbids. The task ran on the CPU. */
    PROVIDER_THREW,

    /** The provider's claimed speedup was below {@code table.exec.accelerator.min-speedup}. */
    BELOW_MIN_SPEEDUP,

    /** The rows this subtask will see do not repay what the provider charges before the first. */
    FIXED_COST_NOT_REPAID,

    /** A measurement of the real input found fewer rows than one batch. */
    TOO_FEW_ROWS,

    /** Building the provider's operator failed, before the task had started. */
    OPERATOR_CREATION_FAILED,

    /**
     * This is a retried attempt of a task, so the accelerator was not offered the work.
     *
     * <p>A device failure that has already killed one attempt is usually deterministic — a kernel
     * that will not compile here, a driver that is wedged, a partition larger than the provider
     * undertook to hold. Retrying it on the same path spends the job's whole restart budget
     * rediscovering the same fact, and then fails the job. Declining on retry turns that into one
     * slow attempt that succeeds.
     */
    RETRY_ATTEMPT,

    /**
     * The provider's operator failed in {@code open()} and the generated one took over.
     *
     * <p>No rows had been consumed, so nothing was at risk. This is where kernel compilation and
     * device allocation happen, which is where failure is likeliest.
     */
    FELL_BACK_AT_OPEN,

    /**
     * The provider's operator failed on its first record and the generated one took over.
     *
     * <p>The last moment at which falling back is safe: the record is still in hand and can be
     * handed on. After it, a failure may have consumed rows this operator cannot give back.
     */
    FELL_BACK_AT_FIRST_RECORD,

    /**
     * The provider's operator went on serving the subtask but stopped using its device.
     *
     * <p>Distinct from the two fallback points above, because Flink did not do it and no operator
     * was replaced: the provider decided, mid-task, that it would finish the work another way. A
     * join whose build side outgrows the staging it was given is the case this exists for. The
     * answer is still correct and the device is still not being used, which is precisely the
     * combination that is invisible unless it is reported.
     */
    PROVIDER_STOPPED_USING_DEVICE,

    /**
     * The subtask's parallelism is above what the accelerated path is known to survive.
     *
     * <p>A containment rather than a preference. Repeated jobs at parallelism above 1 on a single
     * device have been observed to fail intermittently with a CUDA launch error, and the first
     * failing operation has not been identified. Until it is, the accelerated path declines above a
     * configured ceiling instead of exposing a query to a failure nobody can yet explain — which is
     * the contract's own rule: decline predictably before unsafe execution rather than discover the
     * limit at run time.
     *
     * <p>The ceiling is {@code table.exec.accelerator.max-parallelism}, and it is deliberately not
     * a claim that the limit is exactly right. It is the largest value that has been tested clean.
     */
    PARALLELISM_ABOVE_SUPPORTED
}
