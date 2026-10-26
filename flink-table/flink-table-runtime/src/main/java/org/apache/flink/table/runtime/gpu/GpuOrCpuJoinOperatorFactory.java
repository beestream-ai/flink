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

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeServiceAware;
import org.apache.flink.table.accelerator.AcceleratorDeclineReason;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.accelerator.AcceleratedJoinSpec;
import org.apache.flink.table.runtime.accelerator.AcceleratorChoice;
import org.apache.flink.table.runtime.accelerator.AcceleratorProvider;
import org.apache.flink.table.runtime.accelerator.AcceleratorProviders;
import org.apache.flink.table.runtime.accelerator.ManagedOffHeapArena;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Optional;

/**
 * Carries both implementations of one offloaded join and picks between them on the TaskManager.
 *
 * <p>{@link GpuOrCpuCalcOperatorFactory} with a second input, and the parts that differ are the
 * parts worth reading. Everything about <em>why</em> the choice is made here rather than in the
 * planner is unchanged and documented there.
 *
 * <h2>What a second input actually changed</h2>
 *
 * <p>Less than it looks, and saying which is the point of building the seam before the join logic:
 *
 * <ul>
 *   <li><b>The SPI did not change.</b> {@code accept} takes a subtree and {@code createOperator}
 *       returns a factory; a two-input subtree produces a factory that builds a {@code
 *       TwoInputStreamOperator}, and neither signature had anything to say about input count. The
 *       IR carries both row types, so the context did not need to grow a second one either.
 *   <li><b>The guard did.</b> {@link FallbackToCpuTwoInputOperator} covers a strictly narrower
 *       window, because a join has consumed its whole build side before the probe starts.
 *   <li><b>{@code InputSelectable} had to be honoured.</b> Flink reports the <em>generated</em>
 *       operator's class to the client, and {@code HashJoinOperator} is {@code InputSelectable}; so
 *       whatever actually runs is asked for a selection, and an accelerated join that did not
 *       answer "build side first" would be answering a different question from the plan it was
 *       substituted into.
 *   <li><b>The cardinality gate does not reach it.</b> {@code AcceleratorCardinalityStrategy} stops
 *       its walk at any node with more than one input, deliberately and from before this existed.
 *       So a join is decided on the planner's estimate; refining it would mean saying which of two
 *       inputs a byte count bounds, which the refinement's signature cannot express. Left as it is
 *       rather than half-changed.
 * </ul>
 */
public class GpuOrCpuJoinOperatorFactory extends AbstractStreamOperatorFactory<RowData> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(GpuOrCpuJoinOperatorFactory.class);

    private final AcceleratedJoinSpec spec;

    /** What Flink would have built for this join, and what runs if the device is unavailable. */
    private final StreamOperatorFactory<RowData> cpuFactory;

    public GpuOrCpuJoinOperatorFactory(
            AcceleratedJoinSpec spec, StreamOperatorFactory<RowData> cpuFactory) {
        this.spec = spec;
        this.cpuFactory = cpuFactory;
    }

    @Override
    public <T extends StreamOperator<RowData>> T createStreamOperator(
            StreamOperatorParameters<RowData> parameters) {
        ManagedOffHeapArena staging = null;
        try {
            String retry = GpuOrCpuCalcOperatorFactory.retryAttempt(parameters);
            if (retry != null) {
                return codeGenerated(parameters, AcceleratorDeclineReason.RETRY_ATTEMPT, retry);
            }
            ClassLoader loader = parameters.getContainingTask().getUserCodeClassLoader();
            Optional<AcceleratorProvider> accelerator = AcceleratorProviders.find(loader);
            if (!accelerator.isPresent()) {
                return codeGenerated(
                        parameters,
                        AcceleratorDeclineReason.NO_PROVIDER,
                        "no accelerator provider on this TaskManager's classpath");
            }
            AcceleratorChoice.Outcome outcome =
                    AcceleratorChoice.choose(
                            accelerator.get(),
                            spec.subtree(),
                            spec.work(),
                            spec.minSpeedup(),
                            spec.work().estimatedRows());
            if (!outcome.accelerated()) {
                return codeGenerated(parameters, outcome.code(), outcome.reason());
            }
            staging = AcceleratorSetup.reserveStaging(spec, parameters);
            StreamOperatorFactory<RowData> built =
                    accelerator
                            .get()
                            .createOperator(
                                    outcome.plan().get(),
                                    AcceleratorSetup.contextFor(
                                            spec.outputType(),
                                            spec.maxBatchSize(),
                                            bytesPerRow(),
                                            loader,
                                            staging));
            built.setChainingStrategy(getChainingStrategy());
            if (built instanceof ProcessingTimeServiceAware) {
                ((ProcessingTimeServiceAware) built)
                        .setProcessingTimeService(processingTimeService);
            }
            StreamOperator<RowData> device = built.createStreamOperator(parameters);
            if (!(device instanceof TwoInputStreamOperator)) {
                // A provider that answered a two-input subtree with a one-input operator. Caught
                // rather than allowed to become a class cast somewhere further in, because the
                // message there would say nothing about which provider was wrong.
                throw new IllegalStateException(
                        "provider "
                                + accelerator.get().name()
                                + " returned "
                                + device.getClass().getName()
                                + " for a two-input subtree");
            }
            LOG.info("Accelerated on this TaskManager: {}", outcome.reason());
            @SuppressWarnings("unchecked")
            TwoInputStreamOperator<RowData, RowData, RowData> twoInput =
                    (TwoInputStreamOperator<RowData, RowData, RowData>) device;
            FallbackToCpuTwoInputOperator guarded =
                    FallbackToCpuTwoInputOperator.around(twoInput, cpuFactory, parameters, staging);
            @SuppressWarnings("unchecked")
            T operator = (T) guarded;
            return report(operator, guarded, outcome.code(), outcome.reason());
        } catch (Throwable t) {
            // The staging arena holds the slot's whole operator share of managed memory, and
            // the generated operator this falls back to wants managed memory of its own. On
            // this path nothing has taken ownership of it -- the guard that would give it back
            // is never returned -- so without this the fallback asks the MemoryManager for a
            // share that is still booked and fails the task instead of running on the CPU.
            releaseStaging(staging);
            return codeGenerated(
                    parameters,
                    AcceleratorDeclineReason.OPERATOR_CREATION_FAILED,
                    "accelerator operator could not be created (" + t + ")");
        }
    }

    /**
     * Gives a reserved staging arena back, where one was reserved and nothing else owns it.
     *
     * <p>Safe to call with an arena a guard has already taken: {@code close()} is idempotent, and
     * on a failure path the guard holding it was never returned to the task. Failures here are
     * logged rather than thrown, because this runs while already handling a failure and the
     * accelerator decline is the more useful thing for the caller to see.
     */
    private static void releaseStaging(@Nullable ManagedOffHeapArena staging) {
        if (staging == null) {
            return;
        }
        try {
            staging.close();
        } catch (Throwable t) {
            LOG.warn("could not release accelerator staging memory after a failed build", t);
        }
    }

    /**
     * The staging width one probe row costs, for sizing a batch.
     *
     * <p>The probe side and the output, which is what is in flight at once. The build side is not
     * in here and must not be: it is resident for the whole join rather than per batch, and a
     * provider sizes it from {@code AcceleratorContext.stagingBytes()} as the sort does.
     */
    public int bytesPerRow() {
        return Math.max(1, spec.work().inputBytesPerRow() + spec.work().outputBytesPerRow());
    }

    private <T extends StreamOperator<RowData>> T codeGenerated(
            StreamOperatorParameters<RowData> parameters,
            AcceleratorDeclineReason code,
            String reason) {
        LOG.info(
                "Accelerator declined on this TaskManager, running the generated CPU operator: {}",
                reason);
        return report(cpuFactory.createStreamOperator(parameters), null, code, reason);
    }

    private static <T extends StreamOperator<RowData>> T report(
            T operator,
            @Nullable FallbackToCpuTwoInputOperator guarded,
            AcceleratorDeclineReason code,
            String reason) {
        try {
            OperatorMetricGroup metrics = operator.getMetricGroup();
            metrics.gauge(
                    GpuOrCpuCalcOperatorFactory.ACCELERATED,
                    (Gauge<Integer>) () -> guarded != null && guarded.isAccelerated() ? 1 : 0);
            metrics.gauge(GpuOrCpuCalcOperatorFactory.DECISION, (Gauge<String>) () -> reason);
            metrics.gauge(
                    GpuOrCpuCalcOperatorFactory.DECISION_CODE,
                    (Gauge<String>)
                            () -> (guarded != null ? guarded.declineReason() : code).name());
        } catch (Throwable t) {
            LOG.debug("Could not publish the accelerator decision ({})", t.toString());
        }
        return operator;
    }

    /**
     * Reports the code-generated operator's class, never the device one.
     *
     * <p>Matters more here than it does for a Calc. Flink reads this on the client to decide
     * whether the task has to drive input selection at all, and the answer for a hash join is yes —
     * so reporting the generated class is not merely avoiding a classpath dependency, it is what
     * makes the substituted operator run under the same input ordering the plan assumed.
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

    @Override
    public void setProcessingTimeService(ProcessingTimeService processingTimeService) {
        super.setProcessingTimeService(processingTimeService);
        if (cpuFactory instanceof ProcessingTimeServiceAware) {
            ((ProcessingTimeServiceAware) cpuFactory)
                    .setProcessingTimeService(processingTimeService);
        }
    }
}
