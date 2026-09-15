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
 * <p><b>V1 admits only the empty grouping set</b> — an ungrouped aggregate. A grouped aggregate is
 * a sparse matrix-vector product and needs a grouping matrix nothing builds yet; admitting the
 * field now means a provider never has to guess whether {@code grouping} can be non-empty, and a
 * later IR version can widen it without changing the shape.
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
        if (grouping.length != 0) {
            throw new IllegalArgumentException(
                    "accelerator IR v"
                            + AccelIrVersion.CURRENT
                            + " admits only an ungrouped aggregate; GROUP BY needs a grouping "
                            + "matrix that is not built yet");
        }
        this.grouping = grouping.clone();
        this.calls = Collections.unmodifiableList(new ArrayList<>(calls));
        this.input = input;
        this.outputType = outputType;
    }

    /** Grouping key field indexes. Empty in V1. */
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
