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
import org.apache.flink.table.types.logical.RowType;

import java.util.Collections;
import java.util.List;

/**
 * Orders its input by one key. V5.
 *
 * <p>The first node in this IR that is neither per-row nor partial, and both facts shape it.
 *
 * <h2>Why it carries a key and not a comparator</h2>
 *
 * <p>A device orders a column, not a row. So this node names <em>one</em> field of its input and
 * the direction to order it in, and says nothing about how the rows themselves move: a provider is
 * expected to obtain a permutation and apply it to whatever it is holding. That keeps the payload
 * out of the contract entirely — a row may carry columns of types no kernel could express, and this
 * node is still serviceable, because no kernel has to touch them.
 *
 * <p>Multiple sort keys are deliberately not expressible. Composing them means a comparator, a
 * comparator means running query-defined code per comparison, and that is the thing a library
 * primitive cannot take. A provider that met a two-key sort would have to decline it anyway, so the
 * planner does not build one.
 *
 * <h2>Why a provider may refuse on size alone</h2>
 *
 * <p>Unlike {@link AccelAggregate}, this cannot be served a batch at a time. A sort of one batch is
 * a sorted run, and runs have to be merged; the operator therefore has to hold its whole input
 * before it can emit anything. Flink's own sort spills past its memory budget, and a provider is
 * expected to <em>decline</em> past its own rather than spill — falling back to the operator Flink
 * generated, which knows how.
 *
 * <h2>Why the limit is here and not a node above</h2>
 *
 * <p>A top-N is one device operation, not two: order the key column, then read the first {@code N}
 * of the permutation. Expressing it as a sort with a {@code Limit} node above would oblige a
 * provider to hand back the whole ordered input so that something else could discard nearly all of
 * it — which is the one thing a sort must not do, because its output is as large as its input and
 * the copy back is what makes a device sort unprofitable.
 *
 * <p>The limit also changes what a provider may accept. An unbounded sort returns as many rows as
 * it read; a bounded one returns {@code N} however much it read, so the size a provider refuses on
 * above is the <em>input</em> size, while the cost of returning the answer is fixed. Those are
 * different enough that a provider will reasonably take one and decline the other.
 */
@PublicEvolving
public final class AccelSort extends AccelNode {

    private static final long serialVersionUID = 1L;

    /** What {@link #limit()} reports when every ordered row is wanted. */
    public static final long NO_LIMIT = Long.MAX_VALUE;

    private final int sortField;
    private final boolean ascending;
    private final boolean nullsLast;
    private final long limit;
    private final AccelNode input;
    private final RowType outputType;

    /** An unbounded sort: every row the input holds, ordered. */
    public AccelSort(
            int sortField,
            boolean ascending,
            boolean nullsLast,
            AccelNode input,
            RowType outputType) {
        this(sortField, ascending, nullsLast, NO_LIMIT, input, outputType);
    }

    /**
     * A sort that keeps only the first {@code limit} rows of the ordering.
     *
     * @param limit how many ordered rows to emit, or {@link #NO_LIMIT} for all of them
     */
    public AccelSort(
            int sortField,
            boolean ascending,
            boolean nullsLast,
            long limit,
            AccelNode input,
            RowType outputType) {
        if (limit <= 0) {
            throw new IllegalArgumentException(
                    "a sort limit is a row count and must be positive, not " + limit);
        }
        this.sortField = sortField;
        this.ascending = ascending;
        this.nullsLast = nullsLast;
        this.limit = limit;
        this.input = input;
        this.outputType = outputType;
    }

    /** The field of the input row to order by. */
    public int sortField() {
        return sortField;
    }

    public boolean ascending() {
        return ascending;
    }

    /**
     * Where absent keys go.
     *
     * <p>Part of the contract rather than a detail, because SQL lets a query state it and the two
     * answers are both orderings: a provider that guessed would be wrong half the time, silently.
     */
    public boolean nullsLast() {
        return nullsLast;
    }

    /**
     * How many ordered rows to emit, or {@link #NO_LIMIT}.
     *
     * <p>Always positive. A provider that ignores it produces a correct ordering and the wrong
     * number of rows, so it is not safe to leave unread.
     */
    public long limit() {
        return limit;
    }

    /** Whether this sort emits fewer rows than it read. */
    public boolean isBounded() {
        return limit != NO_LIMIT;
    }

    @Override
    public List<AccelNode> inputs() {
        return Collections.singletonList(input);
    }

    /** Same row as the input: a sort reorders rows, it does not change them. */
    @Override
    public RowType outputType() {
        return outputType;
    }

    @Override
    public String toString() {
        return "Sort(f"
                + sortField
                + " "
                + (ascending ? "ASC" : "DESC")
                + " NULLS "
                + (nullsLast ? "LAST" : "FIRST")
                + (isBounded() ? " LIMIT " + limit : "")
                + ")";
    }
}
