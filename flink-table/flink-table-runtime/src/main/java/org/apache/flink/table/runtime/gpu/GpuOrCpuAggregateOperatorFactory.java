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
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeServiceAware;
import org.apache.flink.table.data.RowData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * Picks, on the TaskManager, between running a fused Calc-and-aggregate on a device and running
 * both generated operators chained on the CPU.
 *
 * <p>The same shape as {@link GpuOrCpuCalcOperatorFactory} and for the same reason: the client
 * cannot know which machine will get the task, so both implementations travel in the JobGraph and
 * the choice is made where the answer is knowable. What differs is the CPU side. A fused node has
 * collapsed two ExecNodes into one transformation, so falling back means running <em>both</em>
 * generated operators, which {@link ChainedCalcAggregateOperator} does.
 *
 * <p>That keeps the property the Calc path relies on: nothing about the aggregate is reimplemented,
 * so the fallback's answer is Flink's answer rather than a lookalike.
 */
@Internal
public class GpuOrCpuAggregateOperatorFactory extends AbstractStreamOperatorFactory<RowData> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG =
            LoggerFactory.getLogger(GpuOrCpuAggregateOperatorFactory.class);

    private final GpuAggregateSpec spec;
    private final StreamOperatorFactory<RowData> calcFactory;
    private final StreamOperatorFactory<RowData> aggregateFactory;

    public GpuOrCpuAggregateOperatorFactory(
            GpuAggregateSpec spec,
            StreamOperatorFactory<RowData> calcFactory,
            StreamOperatorFactory<RowData> aggregateFactory) {
        this.spec = spec;
        this.calcFactory = calcFactory;
        this.aggregateFactory = aggregateFactory;
    }

    public GpuAggregateSpec spec() {
        return spec;
    }

    /**
     * Every way this can fail lands on the chained CPU operator.
     *
     * <p>{@code Throwable} rather than {@code Exception} for the reasons the Calc factory gives:
     * the module may be absent, the JVM may lack {@code --enable-preview}, the service file may
     * resolve to something unloadable. None of those should take the query down.
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T extends StreamOperator<RowData>> T createStreamOperator(
            StreamOperatorParameters<RowData> parameters) {
        try {
            Optional<GpuOperatorFactoryProvider> provider =
                    GpuOperatorFactoryProvider.find(
                            parameters.getContainingTask().getUserCodeClassLoader());
            if (!provider.isPresent()) {
                return chained(
                        parameters,
                        "no GpuOperatorFactoryProvider on this TaskManager's classpath");
            }
            Optional<StreamOperatorFactory<RowData>> factory =
                    provider.get().createAggregateOperatorFactory(spec);
            if (!factory.isPresent()) {
                return chained(parameters, provider.get().describe() + " declined the spec");
            }
            StreamOperatorFactory<RowData> gpu = factory.get();
            gpu.setChainingStrategy(getChainingStrategy());
            if (gpu instanceof ProcessingTimeServiceAware) {
                ((ProcessingTimeServiceAware) gpu).setProcessingTimeService(processingTimeService);
            }
            T operator = gpu.createStreamOperator(parameters);
            LOG.info(
                    "GPU offload active for {} fused with its aggregate on this TaskManager via {}",
                    spec.kernel().methodName(),
                    provider.get().describe());
            return operator;
        } catch (Throwable t) {
            return chained(parameters, "GPU operator could not be created (" + t + ")");
        }
    }

    /**
     * Builds the two generated operators and wires one into the other.
     *
     * <p>The aggregate is created against the task's real output; the Calc against an output that
     * hands each record to the aggregate. Both factories need the processing-time service the task
     * set on this one, since the task only knows about the outer factory.
     */
    @SuppressWarnings("unchecked")
    private <T extends StreamOperator<RowData>> T chained(
            StreamOperatorParameters<RowData> parameters, String reason) {
        LOG.info(
                "GPU offload declined on this TaskManager, chaining the generated operators for {}: {}",
                spec.kernel().methodName(),
                reason);

        return chain(parameters, calcFactory, aggregateFactory, processingTimeService);
    }

    /**
     * Builds the two generated operators and wires one into the other.
     *
     * <p>The aggregate is created against the task's real output; the Calc against an output that
     * hands each record to the aggregate. Both factories need the processing-time service the task
     * set on the outer one, since the task only knows about that.
     *
     * <p>Static and shared, because the planner reaches it too: when the client already knows there
     * is no kernel to offer, it emits this chain directly rather than a factory that would ask the
     * TaskManager a question with one possible answer.
     */
    @SuppressWarnings("unchecked")
    public static <T extends StreamOperator<RowData>> T chain(
            StreamOperatorParameters<RowData> parameters,
            StreamOperatorFactory<RowData> calcFactory,
            StreamOperatorFactory<RowData> aggregateFactory,
            ProcessingTimeService processingTimeService) {

        propagate(aggregateFactory, processingTimeService);
        propagate(calcFactory, processingTimeService);

        OneInputStreamOperator<RowData, RowData> aggregate =
                aggregateFactory.createStreamOperator(parameters);

        Output<StreamRecord<RowData>> intoAggregate =
                new ChainedCalcAggregateOperator.Forwarding(aggregate, parameters.getOutput());
        StreamOperatorParameters<RowData> calcParameters =
                new StreamOperatorParameters<>(
                        parameters.getContainingTask(),
                        parameters.getStreamConfig(),
                        intoAggregate,
                        parameters::getProcessingTimeService,
                        parameters.getOperatorEventDispatcher(),
                        parameters.getMailboxExecutor());
        OneInputStreamOperator<RowData, RowData> calc =
                calcFactory.createStreamOperator(calcParameters);

        return (T) new ChainedCalcAggregateOperator(calc, aggregate);
    }

    private static void propagate(
            StreamOperatorFactory<RowData> factory, ProcessingTimeService service) {
        if (factory instanceof ProcessingTimeServiceAware) {
            ((ProcessingTimeServiceAware) factory).setProcessingTimeService(service);
        }
    }

    /**
     * Reports the generated aggregate's class, never the device one.
     *
     * <p>Flink asks this on the client to test for {@code InputSelectable}; answering with the
     * device class would need the GPU module wherever the JobGraph is built.
     */
    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return aggregateFactory.getStreamOperatorClass(classLoader);
    }

    @Override
    public String toString() {
        return "GpuOrCpuAggregateOperatorFactory[" + spec + "]";
    }
}
