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
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
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
import org.apache.flink.table.runtime.accelerator.ManagedOffHeapArena;
import org.apache.flink.table.types.logical.RowType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
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
            ClassLoader loader = parameters.getContainingTask().getUserCodeClassLoader();

            // The accelerator SPI first. It is the one that asks a provider what its device would
            // charge, rather than merely whether it exists.
            Optional<AcceleratorProvider> accelerator = AcceleratorProviders.find(loader);
            if (accelerator.isPresent()) {
                String tooFew = tooFewRowsToBother();
                if (tooFew != null) {
                    return codeGenerated(parameters, tooFew);
                }
                AcceleratorChoice.Outcome outcome =
                        AcceleratorChoice.choose(
                                accelerator.get(),
                                spec.subtree(),
                                spec.work(),
                                spec.minSpeedup(),
                                rowsToDecideOn());
                if (outcome.accelerated()) {
                    StreamOperatorFactory<RowData> built =
                            accelerator
                                    .get()
                                    .createOperator(
                                            outcome.plan().get(),
                                            contextFor(spec, loader, parameters));
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
                                    parameters);
                    @SuppressWarnings("unchecked")
                    T operator = (T) guarded;
                    return report(operator, guarded, outcome.reason());
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

    /**
     * What this machine tells the provider about the task it is being built into.
     *
     * <p>The batch size is the part worth reading. Until M3.1 it was whatever {@code
     * table.exec.accelerator.batch-size} said, which is a number chosen once, in a config file, by
     * someone who could not know how much memory the slot running this task would have or how wide
     * its rows would be. It is now a <em>ceiling</em>: the operator asks the slot what it has,
     * works out how many rows of this width fit, and takes the smaller of the two.
     *
     * <p>Where there is no budget — a plan built before the transformation declared managed memory,
     * or a test harness with no slot behind it — the configured value stands and the provider
     * allocates privately. That is what happened before, and it is a safe degradation rather than a
     * failure.
     */
    private static AcceleratorContext contextFor(
            AcceleratedCalcSpec spec,
            ClassLoader loader,
            StreamOperatorParameters<RowData> parameters) {

        final ManagedOffHeapArena arena = reserveStagingMemory(spec, parameters);
        final int batchSize = batchSizeFor(spec, arena);

        return new AcceleratorContext() {
            @Override
            public RowType outputType() {
                return spec.outputType();
            }

            @Override
            public int maxBatchSize() {
                return batchSize;
            }

            @Override
            public ClassLoader userCodeClassLoader() {
                return loader;
            }

            @Override
            public boolean providesOffHeap() {
                return arena != null;
            }

            @Override
            public ByteBuffer allocateOffHeap(int bytes) {
                if (arena == null) {
                    throw new IllegalStateException(
                            "no managed memory was reserved for this operator, so the provider"
                                    + " must allocate its own staging");
                }
                return arena.allocate(bytes);
            }
        };
    }

    /**
     * Reserves the slot's share of managed memory for this operator, or null if there is none.
     *
     * <p>Registered with the task's cancelables rather than closed by the operator: a cancelled
     * task must give its memory back, and an operator cancelled before {@code close} would
     * otherwise hold it for the life of the slot.
     */
    private static @Nullable ManagedOffHeapArena reserveStagingMemory(
            AcceleratedCalcSpec spec, StreamOperatorParameters<RowData> parameters) {
        try {
            Environment environment = parameters.getContainingTask().getEnvironment();
            MemoryManager memoryManager = environment.getMemoryManager();
            double fraction =
                    parameters
                            .getStreamConfig()
                            .getManagedMemoryFractionOperatorUseCaseOfSlot(
                                    ManagedMemoryUseCase.OPERATOR,
                                    environment.getJobConfiguration(),
                                    environment.getTaskManagerInfo().getConfiguration(),
                                    environment.getUserCodeClassLoader().asClassLoader());
            long budget = memoryManager.computeMemorySize(fraction);

            Optional<ManagedOffHeapArena> arena =
                    ManagedOffHeapArena.reserve(memoryManager, spec, budget);
            if (!arena.isPresent()) {
                LOG.info(
                        "No managed memory for accelerated staging (budget {} bytes); the provider"
                                + " allocates its own and the configured batch size stands",
                        budget);
                return null;
            }
            parameters.getContainingTask().getCancelables().registerCloseable(arena.get());
            return arena.get();
        } catch (Throwable t) {
            // Reserving improves the accounting; it is never a precondition for running.
            LOG.info("Could not reserve managed memory for accelerated staging ({})", t.toString());
            return null;
        }
    }

    /**
     * The staging width one row costs, for whoever is turning bytes into rows.
     *
     * <p>Exposed for {@code AcceleratorCardinalityStrategy}, which has an exact byte count and
     * needs a row count. Input plus output, the same figure {@link #batchSizeFor} sizes the arena
     * with.
     */
    public int bytesPerRow() {
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
     * #batchSizeFor}: the real batch can only be smaller, so this errs toward the CPU, which is the
     * side that is always correct.
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

    /** The configured ceiling, or what the reserved budget actually fits, whichever is smaller. */
    private static int batchSizeFor(AcceleratedCalcSpec spec, @Nullable ManagedOffHeapArena arena) {
        if (arena == null) {
            return spec.maxBatchSize();
        }
        int bytesPerRow =
                Math.max(1, spec.work().inputBytesPerRow() + spec.work().outputBytesPerRow());
        long fits = arena.reservedBytes() / bytesPerRow;
        int batchSize = (int) Math.max(1, Math.min(spec.maxBatchSize(), fits));
        LOG.info(
                "Accelerated staging: {} bytes reserved, {} a row, batch size {} (ceiling {})",
                arena.reservedBytes(),
                bytesPerRow,
                batchSize,
                spec.maxBatchSize());
        return batchSize;
    }

    private <T extends StreamOperator<RowData>> T codeGenerated(
            StreamOperatorParameters<RowData> parameters, String reason) {
        LOG.info(
                "Accelerator declined on this TaskManager, running the generated CPU operator: {}",
                reason);
        return report(cpuFactory.createStreamOperator(parameters), null, reason);
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
            T operator, @Nullable FallbackToCpuOperator guarded, String reason) {
        try {
            OperatorMetricGroup metrics = operator.getMetricGroup();
            // Read through the guard rather than captured: a subtask that started on a device and
            // fell back mid-task must stop claiming to be accelerated, and a gauge is read when it
            // is reported rather than when it is registered.
            metrics.gauge(
                    ACCELERATED,
                    (Gauge<Integer>) () -> guarded != null && guarded.isAccelerated() ? 1 : 0);
            metrics.gauge(DECISION, (Gauge<String>) () -> reason);
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
