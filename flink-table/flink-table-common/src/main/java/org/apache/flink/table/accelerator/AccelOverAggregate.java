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
 * A running total over an ordered input: {@code SUM(x) OVER (ORDER BY ... ROWS BETWEEN UNBOUNDED
 * PRECEDING AND CURRENT ROW)}. V7.
 *
 * <h2>Why the frame is not a field</h2>
 *
 * <p>Because there is only one. Every other frame is a different primitive rather than a parameter
 * of this one — a bounded {@code ROWS} frame is a sliding window, a {@code RANGE} frame has to
 * resolve peer groups, and {@code NTILE} or {@code LEAD} are not accumulations at all. A node that
 * carried bounds would be describing shapes no provider could serve, and the planner would have to
 * refuse them anyway. So this node means exactly one thing and the planner builds it only for that.
 *
 * <h2>Why there is no partition key</h2>
 *
 * <p>Same reasoning, one level up. A partitioned running total is a <em>segmented</em> scan, which
 * is a different primitive from a scan and not one this project has bound or measured. Rather than
 * let the node express a partition that every provider would decline, the planner declines it.
 *
 * <h2>What makes this the easy one</h2>
 *
 * <p>Alone among the cross-row nodes here, this does not have to hold its input. {@link AccelSort}
 * must see every row before it can emit any, and {@link AccelJoin} must hold a whole build side; a
 * running total needs only the total so far, so a provider may serve it a batch at a time and carry
 * one number between batches. That is the Calc's shape, and it is why this node says nothing about
 * cardinality: there is no size at which it stops fitting.
 */
@PublicEvolving
public final class AccelOverAggregate extends AccelNode {

    private static final long serialVersionUID = 1L;

    private final int valueField;
    private final AccelNode input;
    private final RowType outputType;

    public AccelOverAggregate(int valueField, AccelNode input, RowType outputType) {
        this.valueField = valueField;
        this.outputType = outputType;
        this.input = input;
    }

    /** The input field being accumulated. */
    public int valueField() {
        return valueField;
    }

    @Override
    public List<AccelNode> inputs() {
        return Collections.singletonList(input);
    }

    /**
     * The input row with the running total appended as its last field.
     *
     * <p>Appended rather than placed, because that is where Flink's own over-aggregate puts it: the
     * operator emits the input row followed by one value per window group, and with one group that
     * is one column at the end.
     */
    @Override
    public RowType outputType() {
        return outputType;
    }

    @Override
    public String toString() {
        return "OverAggregate(SUM(f" + valueField + ") ROWS UNBOUNDED PRECEDING)";
    }
}
