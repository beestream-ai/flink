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
 */
@PublicEvolving
public final class AccelSort extends AccelNode {

    private static final long serialVersionUID = 1L;

    private final int sortField;
    private final boolean ascending;
    private final boolean nullsLast;
    private final AccelNode input;
    private final RowType outputType;

    public AccelSort(
            int sortField,
            boolean ascending,
            boolean nullsLast,
            AccelNode input,
            RowType outputType) {
        this.sortField = sortField;
        this.ascending = ascending;
        this.nullsLast = nullsLast;
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
                + ")";
    }
}
