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
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.table.accelerator.AccelIrVersion;
import org.apache.flink.table.accelerator.AccelNode;
import org.apache.flink.table.accelerator.AccelWorkProfile;
import org.apache.flink.table.data.RowData;

import java.util.Optional;

/**
 * An execution backend Flink can delegate a plan subtree to.
 *
 * <p>Discovered with {@code ServiceLoader}. Flink references nothing an implementation depends on:
 * no accelerator framework, no device API, no vendor package. An implementation is free to be a GPU
 * runtime, an FPGA bitstream loader, or a hand-written SIMD library -- Flink learns one throughput
 * ratio and two absolute costs, and nothing else.
 *
 * <h2>Everything here happens on the TaskManager</h2>
 *
 * <p>Not in the client. The planner cannot know which machine the scheduler will choose, so it
 * cannot know whether a device exists, which one, or what it would charge. It therefore ships a
 * description of the work and leaves the decision to the machine that has to do it. A consequence
 * worth stating plainly: two subtasks of the same job may decide differently, and in a
 * heterogeneous cluster that is the correct outcome rather than an inconsistency.
 *
 * <h2>The unit is a subtree</h2>
 *
 * <p>{@link #accept} takes an {@link AccelNode}, never a single operator, even though Flink
 * currently only ever offers it a projection. That is deliberate: it lets a provider recognise a
 * shape spanning several operators -- a projection feeding a sum, say -- and serve it with one
 * library call. Narrowing this to a single operator would make that a redesign later instead of a
 * rule the provider adds on its own.
 */
@PublicEvolving
public interface AcceleratorProvider {

    /** Stable identity for EXPLAIN, logs and metrics. For example {@code "acme-gpu"}. */
    String name();

    /**
     * The highest {@link AccelIrVersion} this provider understands.
     *
     * <p>Java 11 has no sealed types, so the IR hierarchy is closed by package-private
     * constructors: nothing outside Flink can add a node kind. What that cannot catch is a provider
     * compiled against an older IR meeting a newer Flink, which would otherwise surface as an
     * unexplained refusal or, worse, a node silently ignored. Declaring the version turns that into
     * a clean rejection at lookup.
     */
    int supportedIrVersion();

    /**
     * Can this provider serve the subtree on this machine, and at what cost?
     *
     * <p>Returning empty is an ordinary answer, not a failure. No device present, a driver that did
     * not load, an operation this backend cannot express, a shape whose arithmetic per byte makes
     * it not worth moving -- all of them are empty, and all of them mean Flink runs the
     * code-generated operator it carried alongside and records why.
     *
     * <p>Implementations must not throw. This is called while a task is starting, and an exception
     * here would take down a job that had a perfectly good CPU implementation available.
     *
     * @param subtree what to execute, in Flink's device-neutral IR
     * @param work how much work it performs per row, counted rather than weighted
     */
    Optional<AcceleratorPlan> accept(AccelNode subtree, AccelWorkProfile work);

    /**
     * Builds the operator, once Flink has decided the plan is worth running.
     *
     * <p>Called only with a plan this provider returned from {@link #accept}, so the payload can be
     * cast without checking.
     */
    StreamOperatorFactory<RowData> createOperator(AcceleratorPlan plan, AcceleratorContext context);
}
