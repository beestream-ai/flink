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

package org.apache.flink.table.runtime.gpu;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.table.data.RowData;

import javax.annotation.Nullable;

/**
 * Runs a projection and the aggregate above it inside one operator, on the CPU.
 *
 * <p>The fallback half of the fused node the planner builds for an offloadable {@code GROUP BY}: no
 * device path is offered here, because this is what runs when there is no device or a provider
 * declines. Running the members as one chained operator is what the pair did before the fused node
 * existed.
 *
 * <h2>Why this is in the runtime and not beside the node that builds it</h2>
 *
 * <p>It is serialized into the JobGraph, so the TaskManager has to be able to load it -- and the
 * TaskManager cannot see planner classes. {@code flink-table-planner-loader} keeps the planner in a
 * classloader of its own and hides it from user code, which is deliberate: it is what lets a job
 * carry its own Calcite. A factory declared inside an {@code ExecNode} therefore deserializes on
 * the client, where the planner is present, and fails on the TaskManager with {@code
 * ClassNotFoundException} naming a class the jar demonstrably contains.
 *
 * <p>The rule the placement follows is Flink's own and predates this project: anything that travels
 * in the JobGraph lives in {@code flink-table-runtime}, which ships in {@code lib/}.
 */
@Internal
public final class ChainedCalcAggregateFactory extends AbstractStreamOperatorFactory<RowData> {

    private static final long serialVersionUID = 1L;

    private final StreamOperatorFactory<RowData> calcFactory;
    private final StreamOperatorFactory<RowData> aggregateFactory;

    /**
     * What the {@code Calc} emits, or null to keep the original wiring. Needed only where the
     * downstream operator takes its input serializer from the StreamConfig rather than reading
     * fields by index — {@code SortOperator} does, a generated aggregate does not.
     */
    private final @Nullable TypeSerializer<RowData> downstreamInputSerializer;

    public ChainedCalcAggregateFactory(
            StreamOperatorFactory<RowData> calcFactory,
            StreamOperatorFactory<RowData> aggregateFactory) {
        this(calcFactory, aggregateFactory, null);
    }

    public ChainedCalcAggregateFactory(
            StreamOperatorFactory<RowData> calcFactory,
            StreamOperatorFactory<RowData> aggregateFactory,
            @Nullable TypeSerializer<RowData> downstreamInputSerializer) {
        this.calcFactory = calcFactory;
        this.aggregateFactory = aggregateFactory;
        this.downstreamInputSerializer = downstreamInputSerializer;
    }

    @Override
    public <T extends StreamOperator<RowData>> T createStreamOperator(
            StreamOperatorParameters<RowData> parameters) {
        return ChainedCalcAggregateOperator.chain(
                parameters,
                calcFactory,
                aggregateFactory,
                processingTimeService,
                downstreamInputSerializer);
    }

    @Override
    @SuppressWarnings("rawtypes")
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return aggregateFactory.getStreamOperatorClass(classLoader);
    }
}
