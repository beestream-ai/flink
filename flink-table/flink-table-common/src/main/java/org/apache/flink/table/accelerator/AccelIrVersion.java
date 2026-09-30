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
 * Version of the accelerator IR a provider was compiled against.
 *
 * <p>Java 11 has no {@code sealed}, so the node hierarchy is closed with package-private
 * constructors instead: nothing outside this package can add a node kind. That stops an outside
 * implementor inventing a node, but it does not stop a provider built against an older IR meeting a
 * newer one at run time. This constant is how that mismatch is detected rather than discovered.
 */
@PublicEvolving
public final class AccelIrVersion {

    /** Input, Project, Filter, and an ungrouped Aggregate. */
    public static final int V1 = 1;

    /**
     * Adds {@link AccelFunction#IS_NULL} and {@link AccelFunction#IS_NOT_NULL}, and stops refusing
     * a nullable value before it reaches a provider.
     *
     * <p>A provider built against {@link #V1} has never been asked to serve a value that might be
     * absent — Flink refused those itself — so handing it one would be handing it a case it has no
     * reason to have considered. Hence the bump: this is exactly the mismatch this constant exists
     * to catch.
     *
     * <p>It works because Java inlines a compile-time constant. A provider that writes {@code
     * AccelIrVersion.CURRENT} compiles the value it was built against into its own class file, so a
     * jar built against V1 still answers 1 when it meets a Flink that emits 2, and is rejected.
     */
    public static final int V2 = 2;

    /**
     * Adds {@link AccelFunction#MOD} and {@link AccelFunction#INT_DIVIDE}.
     *
     * <p>A provider that reports V2 never sees either: integer arithmetic was previously refused by
     * the planner rather than mis-computed, and a V2 provider has no rendering for them.
     */
    public static final int V3 = 3;

    /**
     * Admits a grouping set on {@link AccelAggregate}.
     *
     * <p>A provider reporting V3 never sees one: before this the planner refused a grouped
     * aggregate outright, so there is no older provider with an opinion about what it means.
     */
    public static final int V4 = 4;

    /** The version this Flink emits. */
    /**
     * Adds {@link AccelSort}.
     *
     * <p>A provider reporting V4 never sees one, which matters more here than for the nodes before
     * it: every earlier node is per-row or partial, and a provider written against them may
     * reasonably assume it can serve a batch and emit. A sort cannot be served that way, so meeting
     * one unprepared is not a missing rendering but a wrong answer.
     */
    public static final int V5 = 5;

    /**
     * Adds {@link AccelJoin}, and with it the first node that has two inputs.
     *
     * <p>The bump matters for a reason the node kind alone does not say. Every version before this
     * one describes a subtree a provider serves with a one-input operator, and the SPI's {@code
     * createOperator} returns a factory that Flink has until now only ever built a {@code
     * OneInputStreamOperator} from. A V5 provider handed this would return the wrong <em>kind</em>
     * of operator, and the failure would be a class cast in the middle of building a JobGraph
     * rather than anything about accelerators.
     */
    public static final int V6 = 6;

    /**
     * Adds {@link AccelOverAggregate}.
     *
     * <p>A V6 provider never sees one, and here the bump is the mildest of the set: the node is
     * one-input, batchable and per-row in everything but the number it carries between batches, so
     * a provider meeting it unprepared would merely lack a rendering. It is bumped anyway because
     * the alternative is deciding, per node kind, how much surprise is tolerable.
     */
    public static final int V7 = 7;

    /**
     * Adds a limit to {@link AccelSort}, so a top-N is one node rather than a sort under a limit.
     *
     * <p>Bumped rather than added quietly, and this one is not mild. A V7 provider reads
     * {@code AccelSort} as "order every row and give them all back"; a V8 planner may hand it a
     * node meaning "order every row and give back the first thousand". The two differ in the row
     * count of the answer, not in whether a rendering exists — so a provider that met the new node
     * unprepared would return a correct ordering of the wrong number of rows, and nothing
     * downstream would notice.
     */
    public static final int V8 = 8;

    /**
     * Adds {@link AccelScan}, a leaf that reads from storage instead of receiving rows.
     *
     * <p>The bump is for a leaf a V8 provider has never been shown. Every subtree it has been
     * offered is rooted at {@link AccelInput}, whose meaning is "rows arrive, already on the
     * host"; a V9 planner may root one at a scan, whose meaning is "read this file yourself".
     * A provider that walked to the leaf and assumed it was an input would read a format string
     * and a list of paths as though rows were waiting behind them, and there is no shape check
     * that would catch it — both are leaves producing a row type.
     *
     * <p>Mild in one respect and not in another. A provider that declines the subtree is correct
     * and loses nothing, because declining a scan plans the CPU source Flink would have planned
     * anyway. A provider that accepts one it did not understand returns rows from nowhere.
     */
    public static final int V9 = 9;

    public static final int CURRENT = V9;

    private AccelIrVersion() {}
}
