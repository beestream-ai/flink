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
import org.apache.flink.table.accelerator.AccelAggregate;
import org.apache.flink.table.accelerator.AccelIrVersion;
import org.apache.flink.table.accelerator.AccelJoin;
import org.apache.flink.table.accelerator.AccelNode;
import org.apache.flink.table.accelerator.AccelOverAggregate;
import org.apache.flink.table.accelerator.AccelSort;
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

    /**
     * Whether the last plan accepted was a grouped aggregate rather than a projection.
     *
     * <p>Without it a test can only see that <em>something</em> was offered, and "the Calc was
     * offered" and "the fused aggregate was offered" look identical from outside.
     */
    public static final java.util.concurrent.atomic.AtomicBoolean LAST_WAS_AGGREGATE =
            new java.util.concurrent.atomic.AtomicBoolean();

    /** Whether the last subtree offered was a sort, so a test can tell which seam fired. */
    public static final java.util.concurrent.atomic.AtomicBoolean LAST_WAS_SORT =
            new java.util.concurrent.atomic.AtomicBoolean(false);

    /** Whether the last subtree offered was a join — the two-input seam. */
    public static final java.util.concurrent.atomic.AtomicBoolean LAST_WAS_JOIN =
            new java.util.concurrent.atomic.AtomicBoolean(false);

    /**
     * How many OVER windows have been accepted since {@link #enable}.
     *
     * <p>A count rather than a {@code LAST_WAS_} flag, because a query with an {@code ORDER BY}
     * inside its {@code OVER} offers <em>two</em> offloadable nodes — the window and the sort
     * beneath it — and whichever is offered second overwrites a flag. That is not hypothetical:
     * it is every query this node serves.
     */
    public static final AtomicInteger OVER_PLANS_OFFERED = new AtomicInteger();

    /** Times {@link #accept} returned a plan. */
    public static final AtomicInteger PLANS_OFFERED = new AtomicInteger();

    /**
     * The batch size the last operator was told it could use, and whether the memory behind it was
     * Flink's.
     *
     * <p>Both are what a provider is handed rather than what it asks for, which is the point: since
     * M3.1 the configured batch size is a ceiling and the slot's managed budget decides the rest.
     * Recording them here is the only way a test can see that from the outside.
     */
    public static final AtomicInteger LAST_MAX_BATCH_SIZE = new AtomicInteger(-1);

    public static final java.util.concurrent.atomic.AtomicBoolean LAST_HAD_MANAGED_MEMORY =
            new java.util.concurrent.atomic.AtomicBoolean();

    /** Accept subtrees on this thread, claiming {@code speedup} over the CPU. */
    public static void enable(double claimedSpeedup) {
        enabled = true;
        speedup = claimedSpeedup;
        ROWS_PROCESSED.set(0);
        PLANS_OFFERED.set(0);
        OVER_PLANS_OFFERED.set(0);
    }

    /** Makes the accelerated operator fail in {@code open()}, as a lost device would. */
    public static void failOnOpen() {
        AccelInterpreterOperator.failOnOpen = true;
    }

    public static void reset() {
        AccelInterpreterOperator.failOnOpen = false;
        enabled = false;
        speedup = 10.0;
        AccelInterpreter.corrupt = false;
        LAST_WAS_AGGREGATE.set(false);
        LAST_WAS_SORT.set(false);
        LAST_WAS_JOIN.set(false);
        OVER_PLANS_OFFERED.set(0);
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
        boolean interpretable;
        if (subtree instanceof AccelSort
                || subtree instanceof AccelJoin
                || subtree instanceof AccelOverAggregate) {
            // A sort names a key and a join names an equality; neither says anything about
            // expressions, so there is nothing for the interpreter to be able or unable to
            // evaluate.
            interpretable = true;
        } else if (subtree instanceof AccelAggregate) {
            interpretable = AccelInterpreter.canInterpret(subtree.inputs().get(0));
        } else {
            interpretable = AccelInterpreter.canInterpret(subtree);
        }
        if (!interpretable) {
            return Optional.empty();
        }
        PLANS_OFFERED.incrementAndGet();
        LAST_WAS_AGGREGATE.set(subtree instanceof AccelAggregate);
        LAST_WAS_SORT.set(subtree instanceof AccelSort);
        LAST_WAS_JOIN.set(subtree instanceof AccelJoin);
        if (subtree instanceof AccelOverAggregate) {
            OVER_PLANS_OFFERED.incrementAndGet();
        }
        return Optional.of(new Plan(subtree, new AcceleratorCost(speedup, 0L, 0L)));
    }

    @Override
    public StreamOperatorFactory<RowData> createOperator(
            AcceleratorPlan plan, AcceleratorContext context) {
        LAST_MAX_BATCH_SIZE.set(context.maxBatchSize());
        LAST_HAD_MANAGED_MEMORY.set(context.providesOffHeap());
        AccelNode subtree = (AccelNode) plan.payload();
        if (subtree instanceof AccelSort) {
            return SimpleOperatorFactory.of(
                    new AccelSortInterpreterOperator(
                            (AccelSort) subtree, context.outputType(), ROWS_PROCESSED));
        }
        if (subtree instanceof AccelOverAggregate) {
            return SimpleOperatorFactory.of(
                    new AccelOverAggregateInterpreterOperator(
                            (AccelOverAggregate) subtree, context.outputType(), ROWS_PROCESSED));
        }
        if (subtree instanceof AccelJoin) {
            return SimpleOperatorFactory.of(
                    new AccelJoinInterpreterOperator(
                            (AccelJoin) subtree, context.outputType(), ROWS_PROCESSED));
        }
        if (subtree instanceof AccelAggregate) {
            return SimpleOperatorFactory.of(
                    new AccelAggregateInterpreterOperator(
                            (AccelAggregate) subtree, context.outputType(), ROWS_PROCESSED));
        }
        return SimpleOperatorFactory.of(
                new AccelInterpreterOperator(subtree, context.outputType(), ROWS_PROCESSED));
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
