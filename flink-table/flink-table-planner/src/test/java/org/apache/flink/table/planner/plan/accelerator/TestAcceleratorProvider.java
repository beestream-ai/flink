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

package org.apache.flink.table.planner.plan.accelerator;

import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.table.accelerator.AccelIrVersion;
import org.apache.flink.table.accelerator.AccelNode;
import org.apache.flink.table.accelerator.AccelWorkProfile;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.accelerator.AcceleratorContext;
import org.apache.flink.table.runtime.accelerator.AcceleratorCost;
import org.apache.flink.table.runtime.accelerator.AcceleratorPlan;
import org.apache.flink.table.runtime.accelerator.AcceleratorProvider;

import java.io.Serializable;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An accelerator that is not an accelerator: it executes the IR on the host, in Java.
 *
 * <p>This exists so the <em>accelerator path</em> can be tested without an accelerator. Before it,
 * every integration test in this repository took the CPU branch — the planner module has no
 * dependency on any provider, so {@code AcceleratorProviders.find} always returned empty — which
 * meant the suite verified that offload <em>degrades</em> and never that it <em>runs</em>. That is
 * review finding E2, and it blocked several correctness tasks that had nothing to do with devices.
 *
 * <p>Registered through {@code META-INF/services}, so it is visible to every test in this module.
 * It therefore <b>declines by default</b>: a test that wants the accelerator path must say so with
 * {@link #enable}, and must {@link #reset} afterwards. Without that, adding this class would
 * silently reroute every existing test.
 */
public class TestAcceleratorProvider implements AcceleratorProvider {

    // Static, not ThreadLocal: accept() is called on the MiniCluster's task thread, not the
    // thread the test runs on, so a ThreadLocal set by the test is invisible where it matters.
    // That makes these fields shared across tests in a JVM -- acceptable because the flag defaults
    // to off and every test that sets it resets it, but it does mean these tests must not run
    // concurrently with each other.
    private static volatile boolean enabled = false;
    private static volatile double speedup = 10.0;

    /** Rows this provider has actually processed, across all instances. */
    public static final AtomicInteger ROWS_PROCESSED = new AtomicInteger();

    /** Times {@link #accept} returned a plan. */
    public static final AtomicInteger PLANS_OFFERED = new AtomicInteger();

    /** Accept subtrees on this thread, claiming {@code speedup} over the CPU. */
    public static void enable(double claimedSpeedup) {
        enabled = true;
        speedup = claimedSpeedup;
        ROWS_PROCESSED.set(0);
        PLANS_OFFERED.set(0);
    }

    public static void reset() {
        enabled = false;
        speedup = 10.0;
        AccelInterpreter.corrupt = false;
    }

    /** Makes the interpreter compute TIMES as addition, to prove the harness can detect it. */
    public static void corruptArithmetic() {
        AccelInterpreter.corrupt = true;
    }

    @Override
    public String name() {
        return "test-host-interpreter";
    }

    @Override
    public int supportedIrVersion() {
        return AccelIrVersion.CURRENT;
    }

    @Override
    public Optional<AcceleratorPlan> accept(AccelNode subtree, AccelWorkProfile work) {
        if (!enabled) {
            return Optional.empty();
        }
        if (!AccelInterpreter.canInterpret(subtree)) {
            return Optional.empty();
        }
        PLANS_OFFERED.incrementAndGet();
        return Optional.of(new Plan(subtree, new AcceleratorCost(speedup, 0L, 0L)));
    }

    @Override
    public StreamOperatorFactory<RowData> createOperator(
            AcceleratorPlan plan, AcceleratorContext context) {
        return SimpleOperatorFactory.of(
                new AccelInterpreterOperator(
                        (AccelNode) plan.payload(), context.outputType(), ROWS_PROCESSED));
    }

    private static final class Plan implements AcceleratorPlan {
        private static final long serialVersionUID = 1L;

        private final AccelNode subtree;
        private final AcceleratorCost cost;

        Plan(AccelNode subtree, AcceleratorCost cost) {
            this.subtree = subtree;
            this.cost = cost;
        }

        @Override
        public AcceleratorCost cost() {
            return cost;
        }

        @Override
        public Serializable payload() {
            return subtree;
        }
    }
}
