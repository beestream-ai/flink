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
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeServiceAware;
import org.apache.flink.table.accelerator.AcceleratorDeclineReason;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.accelerator.AccelInput;
import org.apache.flink.table.accelerator.AccelNode;
import org.apache.flink.table.runtime.accelerator.AcceleratedCalcSpec;
import org.apache.flink.table.runtime.accelerator.AcceleratorChoice;
import org.apache.flink.table.runtime.accelerator.AcceleratorProvider;
import org.apache.flink.table.runtime.accelerator.AcceleratorProviders;
import org.apache.flink.table.runtime.accelerator.ManagedOffHeapArena;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

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

    /**
     * Rows this subtask will actually see, once something has measured them. Null until then.
     *
     * <p>Set on the JobManager by {@code AcceleratorCardinalityStrategy} after the input has been
     * produced, and read here on the TaskManager. Not final and not immutable on purpose: this is
     * the same shape adaptive join uses, where the strategy reaches the factory instance still
     * sitting in the StreamGraph and mutates it before the JobGraph is built. M4.3.
     */
    private @Nullable RuntimeCardinality cardinality;

    /**
     * The parallelism above which this declines rather than offloading.
     *
     * <p>Carried on the factory because the planner knows the configuration and the TaskManager
     * knows the parallelism, and the decision needs both.
     */
    private final int maxParallelism;

    public GpuOrCpuCalcOperatorFactory(
            AcceleratedCalcSpec spec, StreamOperatorFactory<RowData> cpuFactory) {
        this(spec, cpuFactory, 1);
    }

    public GpuOrCpuCalcOperatorFactory(
            AcceleratedCalcSpec spec,
            StreamOperatorFactory<RowData> cpuFactory,
            int maxParallelism) {
        this.spec = spec;
        this.cpuFactory = cpuFactory;
        this.maxParallelism = maxParallelism;
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
    /**
     * Tells this factory how many rows a subtask will really see, replacing the planner's guess.
     *
     * <p>Called on the JobManager, once, before the vertex is scheduled. The planner's estimate is
     * made from catalog statistics in the client and can be wrong by orders of magnitude; by the
     * time the input has been produced the exact byte count and the parallelism the scheduler
     * settled on are both known, and the two together are what decide whether there is enough work
     * per subtask to repay setting a device up at all.
     *
     * @param rowsPerSubtask rows one subtask of this node will process
     * @param provenance how that figure was arrived at, for the decision metric and the log
     */
    public void refineCardinality(long rowsPerSubtask, String provenance) {
        this.cardinality = new RuntimeCardinality(rowsPerSubtask, provenance);
    }

    /** What a measurement of the real input told us, and how. */
    private static final class RuntimeCardinality implements java.io.Serializable {
        private static final long serialVersionUID = 1L;

        private final long rowsPerSubtask;
        private final String provenance;

        RuntimeCardinality(long rowsPerSubtask, String provenance) {
            this.rowsPerSubtask = rowsPerSubtask;
            this.provenance = provenance;
        }
    }

    @Override
    public <T extends StreamOperator<RowData>> T createStreamOperator(
            StreamOperatorParameters<RowData> parameters) {
        try {
            String retry = retryAttempt(parameters);
            if (retry != null) {
                return codeGenerated(parameters, AcceleratorDeclineReason.RETRY_ATTEMPT, retry);
            }
            String tooParallel = aboveSupportedParallelism(parameters, maxParallelism);
            if (tooParallel != null) {
                return codeGenerated(
                        parameters,
                        AcceleratorDeclineReason.PARALLELISM_ABOVE_SUPPORTED,
                        tooParallel);
            }
            ClassLoader loader = parameters.getContainingTask().getUserCodeClassLoader();

            // The accelerator SPI first. It is the one that asks a provider what its device would
            // charge, rather than merely whether it exists.
            Optional<AcceleratorProvider> accelerator = AcceleratorProviders.find(loader);
            if (accelerator.isPresent()) {
                String tooFew = tooFewRowsToBother();
                if (tooFew != null) {
                    return codeGenerated(parameters, AcceleratorDeclineReason.TOO_FEW_ROWS, tooFew);
                }
                AcceleratorChoice.Outcome outcome =
                        AcceleratorChoice.choose(
                                accelerator.get(),
                                spec.subtree(),
                                spec.work(),
                                spec.minSpeedup(),
                                rowsToDecideOn());
                if (outcome.accelerated()) {
                    // Reserved here rather than inside contextFor so that the guard below can
                    // give it back. Staging holds the slot's whole operator share of managed
                    // memory, and the generated operator this falls back to wants managed memory
                    // of its own -- a hash table, a sorter -- so the two cannot both hold it.
                    ManagedOffHeapArena staging = AcceleratorSetup.reserveStaging(spec, parameters);
                    StreamOperatorFactory<RowData> built =
                            accelerator
                                    .get()
                                    .createOperator(
                                            outcome.plan().get(),
                                            AcceleratorSetup.contextFor(
                                                    spec.outputType(),
                                                    spec.maxBatchSize(),
                                                    bytesPerRowOf(spec),
                                                    loader,
                                                    staging));
                    built.setChainingStrategy(getChainingStrategy());
                    if (built instanceof ProcessingTimeServiceAware) {
                        ((ProcessingTimeServiceAware) built)
                                .setProcessingTimeService(processingTimeService);
                    }
                    StreamOperator<RowData> device = built.createStreamOperator(parameters);
                    LOG.info("Accelerated on this TaskManager: {}", outcome.reason());
                    // Wrapped so that a device which fails while the task is starting -- open() is
                    // what compiles the kernel and allocates its buffers, and the task calls it
                    // after this method has returned -- finishes on the operator already in the
                    // JobGraph rather than taking the job down. M3.4.
                    FallbackToCpuOperator guarded =
                            FallbackToCpuOperator.around(
                                    (OneInputStreamOperator<RowData, RowData>) device,
                                    cpuFactory,
                                    parameters,
                                    staging);
                    @SuppressWarnings("unchecked")
                    T operator = (T) guarded;
                    return report(operator, guarded, outcome.code(), outcome.reason());
                }
                return codeGenerated(parameters, outcome.code(), outcome.reason());
            }

            return codeGenerated(
                    parameters,
                    AcceleratorDeclineReason.NO_PROVIDER,
                    "no accelerator provider on this TaskManager's classpath");
        } catch (Throwable t) {
            return codeGenerated(
                    parameters,
                    AcceleratorDeclineReason.OPERATOR_CREATION_FAILED,
                    "accelerator operator could not be created (" + t + ")");
        }
    }

    /**
     * The staging width one row costs, for whoever is turning bytes into rows.
     *
     * <p>Exposed for {@code AcceleratorCardinalityStrategy}, which has an exact byte count and
     * needs a row count. Input plus output, the same figure {@link AcceleratorSetup#batchSizeFor}
     * sizes the arena with.
     */
    public int bytesPerRow() {
        return arrivingBytesPerRow(spec);
    }

    /**
     * The width of the row this operator <em>receives</em>, which is what a byte count produced
     * upstream should be divided by.
     *
     * <p>M8.3. This used to report {@link #bytesPerRowOf}, input plus output, which is the figure
     * the staging arena is sized with and the wrong one here. For a fused Calc and aggregate the
     * profile's "input" is the aggregate's input -- the projection's output, one value per
     * expression -- not the row the upstream produced. On a Gram matrix that is 496 doubles a row
     * against an arriving row of 31, so the gate divided by thirty times too much and refused a
     * million rows as thirty thousand.
     *
     * <p>The arriving row is the subtree's leaf, which is the one node whose type is by definition
     * what the operator is handed. Falls back to the old figure if no leaf is found, which cannot
     * happen for a well-formed subtree and is not worth throwing over.
     */
    private static int arrivingBytesPerRow(AcceleratedCalcSpec spec) {
        AccelNode node = spec.subtree();
        while (node != null && !(node instanceof AccelInput)) {
            node = node.inputs().isEmpty() ? null : node.inputs().get(0);
        }
        if (node == null) {
            return bytesPerRowOf(spec);
        }
        // Eight bytes a field, which is what a fixed-width row costs in Flink's binary layout and
        // what the numeric types this operator stages are worth. Checked against the measurement
        // that found this bug: 273,000,026 bytes over 1,000,000 rows of 32 fields is 273 a row,
        // against 256 here -- the difference is the row header, and the gate is an order-of-
        // magnitude test rather than an accounting.
        return Math.max(1, 8 * node.outputType().getFieldCount());
    }

    private static int bytesPerRowOf(AcceleratedCalcSpec spec) {
        return Math.max(1, spec.work().inputBytesPerRow() + spec.work().outputBytesPerRow());
    }

    /**
     * Refuses a subtask that cannot fill one batch, or null when there is enough work. M4.3.
     *
     * <p>One batch is the floor because it is the unit the fixed cost is paid in. A subtask sets a
     * device up once -- compiling the kernel, reserving and zeroing the staging arena, and handing
     * the provider its first dispatch -- and that cost lands whether the batch it fills is full or
     * holds eleven rows. Below one batch there is by construction no second batch to spread it
     * over, so the arithmetic cannot come out in the device's favour however fast the device is.
     *
     * <p>Deliberately not a new configuration key. The batch size is already the number the
     * operator is sized against, and inventing a second threshold would mean two knobs that have to
     * agree. The ceiling is used rather than the memory-derived batch size of {@link
     * AcceleratorSetup#batchSizeFor}: the real batch can only be smaller, so this errs toward the
     * CPU, which is the side that is always correct.
     *
     * <p>Silent when nothing measured the input. No measurement means no blocking exchange feeding
     * this node -- a chained source, most often -- and a planner estimate is not evidence enough to
     * refuse on.
     */
    private @Nullable String tooFewRowsToBother() {
        RuntimeCardinality measured = cardinality;
        if (measured == null || measured.rowsPerSubtask >= spec.maxBatchSize()) {
            return null;
        }
        return "too few rows to repay setting a device up: "
                + measured.rowsPerSubtask
                + " a subtask against a batch of "
                + spec.maxBatchSize()
                + " ("
                + measured.provenance
                + ")";
    }

    /**
     * Rows to charge the provider's rate against: what was measured, or the planner's estimate.
     *
     * <p>The M0.4 half of the decision is unchanged -- the provider still prices its own device and
     * the comparison still happens here. All that moves is which row count goes into it.
     */
    private long rowsToDecideOn() {
        return cardinality == null ? spec.work().estimatedRows() : cardinality.rowsPerSubtask;
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

    /**
     * Refuses a retried attempt, or null on the first one.
     *
     * <p>A device failure that has already killed an attempt is usually deterministic: a kernel
     * that will not compile on this machine, a driver in a state a restart does not clear, a
     * partition larger than the provider undertook to hold. Batch failover re-runs the task on the
     * same path, rediscovers the same fact, and spends the job's whole restart budget doing it
     * before failing the job -- when a CPU implementation that would have finished was sitting in
     * the JobGraph the whole time.
     *
     * <p>Deliberately not conditioned on <em>why</em> the previous attempt died. A task is retried
     * for reasons that have nothing to do with the accelerator -- a lost TaskManager, an upstream
     * failure -- and declining in those cases costs one slow attempt. Distinguishing them needs a
     * failure cause to survive into the next attempt, which is a scheduler-side change; until then
     * the conservative reading is the one that cannot lose a job.
     */
    static @Nullable String retryAttempt(StreamOperatorParameters<RowData> parameters) {
        try {
            return retryAttempt(
                    parameters
                            .getContainingTask()
                            .getEnvironment()
                            .getTaskInfo()
                            .getAttemptNumber());
        } catch (Throwable noTaskInfo) {
            // A harness with no task behind it. Not knowing the attempt is not a reason to refuse.
            return null;
        }
    }

    /**
     * Refuses a subtask whose parallelism is above what the accelerated path survives.
     *
     * <p>Containment for a failure whose cause is not established: repeated jobs at parallelism
     * above 1 sharing one device fail intermittently with {@code CUDA_ERROR_LAUNCH_FAILED}, and
     * device memory has been ruled out by measurement. Declining here is the contract's own answer
     * to a configuration that cannot be admitted safely -- refuse before execution rather than fail
     * during it -- and it is reversible by configuration once the cause is found.
     */
    static @Nullable String aboveSupportedParallelism(
            StreamOperatorParameters<RowData> parameters, int limit) {
        try {
            int parallelism =
                    parameters
                            .getContainingTask()
                            .getEnvironment()
                            .getTaskInfo()
                            .getNumberOfParallelSubtasks();
            return aboveSupportedParallelism(parallelism, limit);
        } catch (Throwable noTaskInfo) {
            return null;
        }
    }

    /** The rule itself, separated from where the number comes from so that it can be tested. */
    static @Nullable String aboveSupportedParallelism(int parallelism, int limit) {
        if (limit <= 0 || parallelism <= limit) {
            return null;
        }
        return "subtask parallelism "
                + parallelism
                + " is above the "
                + limit
                + " the accelerated path is known to survive "
                // Named rather than referenced: the option is declared in flink-table-api-java,
                // which this module does not depend on and should not start depending on for a
                // string.
                + "(table.exec.accelerator.max-parallelism); running the generated operator"
                + " instead";
    }

    /** The rule itself, separated from where the number comes from so that it can be tested. */
    static @Nullable String retryAttempt(int attemptNumber) {
        if (attemptNumber <= 0) {
            return null;
        }
        return "attempt "
                + attemptNumber
                + " of this task: a previous attempt failed, and the accelerator is not offered "
                + "the work again in case it was the cause";
    }

    /**
     * Publishes which path this subtask actually took, as metrics.
     *
     * <p>This is the only way to answer "did my query use the accelerator?", and it has to exist
     * because nothing else can answer it any more. EXPLAIN reports what the <em>planner</em>
     * decided, and since costing moved to the TaskManager that is a different question: the plan
     * says a node is eligible, and whether a device served it is settled here, per subtask, against
     * whatever hardware this machine turned out to have. Two subtasks of one job may answer
     * differently and both be right.
     *
     * <p>Registered by Flink rather than by the provider, on the operator the provider returned. A
     * provider that publishes nothing still gets this, which is the point — the question belongs to
     * the person who ran the query, not to the backend that did or did not serve it.
     *
     * <p>{@code accelerated} is 1 or 0 rather than a boolean so that a metrics reporter can graph
     * it; {@code acceleratorDecision} carries the reason, which until now existed only in a log
     * line on the machine that made it.
     */
    private static <T extends StreamOperator<RowData>> T report(
            T operator,
            @Nullable FallbackToCpuOperator guarded,
            AcceleratorDeclineReason code,
            String reason) {
        try {
            OperatorMetricGroup metrics = operator.getMetricGroup();
            // Read through the guard rather than captured: a subtask that started on a device and
            // fell back mid-task must stop claiming to be accelerated, and a gauge is read when it
            // is reported rather than when it is registered.
            metrics.gauge(
                    ACCELERATED,
                    (Gauge<Integer>) () -> guarded != null && guarded.isAccelerated() ? 1 : 0);
            metrics.gauge(DECISION, (Gauge<String>) () -> reason);
            // Read through the guard for the same reason the flag above is: a subtask that fell
            // back mid-task must report the fallback point, not the verdict it started with.
            metrics.gauge(
                    DECISION_CODE,
                    (Gauge<String>)
                            () -> (guarded != null ? guarded.declineReason() : code).name());
        } catch (Throwable t) {
            // A metric is never worth failing a task for, and a harness may hand back an operator
            // with no metric group at all.
            LOG.debug("Could not publish the accelerator decision ({})", t.toString());
        }
        return operator;
    }

    /** 1 when a device served this subtask, 0 when the generated operator did. */
    public static final String ACCELERATED = "accelerated";

    /** Why, in the words the decision was made in. */
    public static final String DECISION = "acceleratorDecision";

    /**
     * The same why, as one of a closed set of names, so that it can be counted.
     *
     * <p>{@link #DECISION} is for the person reading one subtask's line; this is for the dashboard
     * asking what proportion of a cluster's subtasks decline for want of a device. See {@code
     * AcceleratorDeclineReason}.
     */
    public static final String DECISION_CODE = "acceleratorDecisionCode";

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
