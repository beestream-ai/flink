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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeServiceAware;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.accelerator.AcceleratedCalcSpec;
import org.apache.flink.table.runtime.accelerator.AcceleratorChoice;
import org.apache.flink.table.runtime.accelerator.AcceleratorContext;
import org.apache.flink.table.runtime.accelerator.AcceleratorProvider;
import org.apache.flink.table.runtime.accelerator.AcceleratorProviders;
import org.apache.flink.table.types.logical.RowType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * Carries both implementations of one offloaded Calc and picks between them on the TaskManager.
 *
 * <h2>Why the choice cannot be made where it used to be</h2>
 *
 * <p>The planner runs in the client JVM. It knows whether an expression is worth offloading and
 * whether a kernel can be generated for it, and both of those are properties of the query. It does
 * not know, and cannot know, which TaskManager will be given the task or what hardware that machine
 * has: placement happens later, in the scheduler, and nothing feeds back.
 *
 * <p>Deciding there was therefore a decision made on the wrong evidence. A client with TornadoVM on
 * its classpath substituted a device operator into the JobGraph, and if the task landed on a node
 * without a GPU the job did not degrade — it failed, either deserializing an operator whose class
 * was absent or throwing out of {@code open()} with no CPU implementation left to fall back to,
 * because the code-generated one had been discarded at plan time.
 *
 * <p>So the planner now generates both and defers. This class holds the {@link GpuCalcSpec} and the
 * ordinary code-generated factory, and {@link #createStreamOperator} — which runs on the machine
 * that will execute the task — asks that machine what it can do. A node without the GPU module,
 * without a device, or without a JVM configured for TornadoVM runs the code-generated operator and
 * logs why. Nothing fails.
 *
 * <h2>What this costs</h2>
 *
 * <p>Both implementations are generated at plan time and both travel in the JobGraph. The extra
 * weight is one Janino source string per offloaded node, which is what an ordinary Calc ships
 * anyway; the extra work is one code generation the previous design skipped.
 *
 * <p>It also changes what {@code EXPLAIN} means. A plan that says {@code GPU} now states an
 * intention rather than a fact — everything knowable in the client is still decided there and
 * reported, but the last word belongs to the TaskManager. Which path actually ran is logged at
 * {@code INFO} by this class, once per subtask.
 *
 * <p>This is deliberately not a substitute for placement. Falling back is correct, not free: a node
 * that lands on a GPU-less machine runs at CPU speed while {@code EXPLAIN} said otherwise. Making
 * the scheduler put the task where the device is remains open work.
 */
public class GpuOrCpuCalcOperatorFactory extends AbstractStreamOperatorFactory<RowData> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(GpuOrCpuCalcOperatorFactory.class);

    private final AcceleratedCalcSpec spec;

    /** What Flink would have built for this Calc, and what runs if the device is unavailable. */
    private final StreamOperatorFactory<RowData> cpuFactory;

    public GpuOrCpuCalcOperatorFactory(
            AcceleratedCalcSpec spec, StreamOperatorFactory<RowData> cpuFactory) {
        this.spec = spec;
        this.cpuFactory = cpuFactory;
    }

    /**
     * Builds the device operator if this machine can run it, and the code-generated one otherwise.
     *
     * <p>Every failure below is an expected outcome on some machine in a heterogeneous cluster, so
     * all of them are caught. {@code Throwable} rather than {@code Exception} because the ways this
     * fails are mostly errors: {@code NoClassDefFoundError} where the module is absent, {@code
     * UnsupportedClassVersionError} where the JVM lacks {@code --enable-preview}, {@code
     * ServiceConfigurationError} where the service file resolves to something unloadable. A device
     * that cannot be reached must not take the query down with it.
     */
    @Override
    public <T extends StreamOperator<RowData>> T createStreamOperator(
            StreamOperatorParameters<RowData> parameters) {
        try {
            ClassLoader loader = parameters.getContainingTask().getUserCodeClassLoader();

            // The accelerator SPI first. It is the one that asks a provider what its device would
            // charge, rather than merely whether it exists.
            Optional<AcceleratorProvider> accelerator = AcceleratorProviders.find(loader);
            if (accelerator.isPresent()) {
                AcceleratorChoice.Outcome outcome =
                        AcceleratorChoice.choose(
                                accelerator.get(),
                                spec.subtree(),
                                spec.work(),
                                spec.minSpeedup(),
                                spec.work().estimatedRows());
                if (outcome.accelerated()) {
                    StreamOperatorFactory<RowData> built =
                            accelerator
                                    .get()
                                    .createOperator(outcome.plan().get(), contextFor(spec, loader));
                    built.setChainingStrategy(getChainingStrategy());
                    if (built instanceof ProcessingTimeServiceAware) {
                        ((ProcessingTimeServiceAware) built)
                                .setProcessingTimeService(processingTimeService);
                    }
                    T operator = built.createStreamOperator(parameters);
                    LOG.info("Accelerated on this TaskManager: {}", outcome.reason());
                    return operator;
                }
                return codeGenerated(parameters, outcome.reason());
            }

            return codeGenerated(
                    parameters, "no accelerator provider on this TaskManager's classpath");
        } catch (Throwable t) {
            return codeGenerated(
                    parameters, "accelerator operator could not be created (" + t + ")");
        }
    }

    private static AcceleratorContext contextFor(AcceleratedCalcSpec spec, ClassLoader loader) {
        return new AcceleratorContext() {
            @Override
            public RowType outputType() {
                return spec.outputType();
            }

            @Override
            public int maxBatchSize() {
                return spec.maxBatchSize();
            }

            @Override
            public ClassLoader userCodeClassLoader() {
                return loader;
            }
        };
    }

    private <T extends StreamOperator<RowData>> T codeGenerated(
            StreamOperatorParameters<RowData> parameters, String reason) {
        LOG.info(
                "Accelerator declined on this TaskManager, running the generated CPU operator: {}",
                reason);
        return cpuFactory.createStreamOperator(parameters);
    }

    /**
     * Reports the code-generated operator's class, never the device one.
     *
     * <p>Flink calls this on the client to test for {@code InputSelectable}, which neither
     * implementation is. Answering with the device class would require the GPU module wherever the
     * JobGraph is built, which is exactly the coupling this class exists to remove.
     */
    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return cpuFactory.getStreamOperatorClass(classLoader);
    }

    @Override
    public void setChainingStrategy(ChainingStrategy strategy) {
        super.setChainingStrategy(strategy);
        cpuFactory.setChainingStrategy(strategy);
    }

    /**
     * Forwards the service to the delegate that will use it.
     *
     * <p>Flink sets this on the outermost factory only, and {@code CodeGenOperatorFactory} reads
     * its own field rather than the parameters, so without this the CPU path would be handed null.
     */
    @Override
    public void setProcessingTimeService(ProcessingTimeService processingTimeService) {
        super.setProcessingTimeService(processingTimeService);
        if (cpuFactory instanceof ProcessingTimeServiceAware) {
            ((ProcessingTimeServiceAware) cpuFactory)
                    .setProcessingTimeService(processingTimeService);
        }
    }

    @Override
    public boolean isOutputTypeConfigurable() {
        return cpuFactory.isOutputTypeConfigurable();
    }

    @Override
    public void setOutputType(TypeInformation<RowData> type, ExecutionConfig executionConfig) {
        cpuFactory.setOutputType(type, executionConfig);
    }

    @Override
    public boolean isInputTypeConfigurable() {
        return cpuFactory.isInputTypeConfigurable();
    }

    @Override
    public void setInputType(TypeInformation<?> type, ExecutionConfig executionConfig) {
        cpuFactory.setInputType(type, executionConfig);
    }

    /** The spec that would run on a device, for tests and for the operator's own reporting. */
    public AcceleratedCalcSpec spec() {
        return spec;
    }

    public StreamOperatorFactory<RowData> cpuFactory() {
        return cpuFactory;
    }
}
