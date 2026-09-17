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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Contracts its input to one row per group.
 *
 * <p><b>V1 to V3 admitted only the empty grouping set</b>, because the only device route then
 * imagined for a grouped aggregate was a sparse matrix-vector product and nothing built the
 * grouping matrix. V4 admits a grouping set, because a device group-by turned out not to need one:
 * a library has the primitive, and what reaches it is a key column and a value column.
 *
 * <p>The node is a <em>partial</em> aggregate and that is what makes it workable. Flink splits
 * aggregation into a local half and a merge half, and the local half may emit several rows per key
 * for the merge to combine. So a provider may aggregate a batch at a time and emit as it goes, with
 * no state carried between batches and nothing to spill.
 */
@PublicEvolving
public final class AccelAggregate extends AccelNode {

    private static final long serialVersionUID = 1L;

    private final int[] grouping;
    private final List<AccelAggCall> calls;
    private final AccelNode input;
    private final RowType outputType;

    public AccelAggregate(
            int[] grouping, List<AccelAggCall> calls, AccelNode input, RowType outputType) {
        this.grouping = grouping.clone();
        this.calls = Collections.unmodifiableList(new ArrayList<>(calls));
        this.input = input;
        this.outputType = outputType;
    }

    /** Grouping key field indexes into the input row; empty for an ungrouped aggregate. */
    public int[] grouping() {
        return grouping.clone();
    }

    public List<AccelAggCall> calls() {
        return calls;
    }

    @Override
    public List<AccelNode> inputs() {
        return Collections.singletonList(input);
    }

    @Override
    public RowType outputType() {
        return outputType;
    }

    @Override
    public String toString() {
        return "Aggregate" + calls + "(" + input + ")";
    }
}
