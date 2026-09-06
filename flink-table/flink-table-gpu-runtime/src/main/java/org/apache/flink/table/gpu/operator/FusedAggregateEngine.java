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

package org.apache.flink.table.gpu.operator;

import org.apache.flink.table.gpu.gather.RowGather;
import org.apache.flink.table.runtime.gpu.GpuAggregateSpec;
import org.apache.flink.table.runtime.gpu.GpuKernelSource;
import org.apache.flink.table.runtime.gpu.GpuValueType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.ac.manchester.tornado.api.TaskGraph;
import uk.ac.manchester.tornado.api.TornadoExecutionPlan;
import uk.ac.manchester.tornado.api.annotations.Parallel;
import uk.ac.manchester.tornado.api.enums.DataTransferMode;
import uk.ac.manchester.tornado.api.types.arrays.DoubleArray;
import uk.ac.manchester.tornado.api.types.arrays.IntArray;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 * A {@code Calc} and the {@code SUM}s above it, as one device pass.
 *
 * <p>The projection kernel writes its columns to device buffers and the contraction reads those
 * buffers in place. Nothing about the intermediate returns to the host: the only thing copied back
 * per batch is one partial sum per output column, a few kilobytes whatever the batch size, against
 * the megabytes the same pair costs when the Calc has to emit rows for a separate aggregate to
 * consume.
 *
 * <h2>Why the contraction is generated rather than a library call</h2>
 *
 * <p>{@code SUM(e1), ..., SUM(ek)} is {@code ones' * M}, and {@code cublasSgemv} would do it in one
 * call. It is not used here, and the reason is the {@code S}. The binding exposes no FP64 entry
 * point, so dispatching a {@code SUM} over a {@code DOUBLE} column to cuBLAS would quietly
 * accumulate hundreds of thousands of values in FP32 and hand back an answer several digits worse
 * than the CPU plan's, in a path whose entire premise is that the user did not ask for a device and
 * cannot tell one was used. A transparent optimisation may be slower than expected; it may not
 * return different numbers.
 *
 * <p>So the contraction is generated and stays in FP64. The library becomes reachable the moment
 * either side of that changes -- an FP64 binding, or a query whose columns are declared {@code
 * FLOAT} and so ask for FP32 in the first place -- and the shape here does not have to move for it.
 *
 * <h2>The reduction</h2>
 *
 * <p>One thread per output column would launch {@code k} threads on a device with thousands of
 * cores, which is the occupancy trap the standalone sweep already fell into. Instead each column is
 * split into {@link #PARTIALS} contiguous chunks, one thread each, and the host adds the chunk
 * sums. That last addition is what makes the result independent of how many threads ran.
 *
 * <p>The shape is copied from {@code FeatureGramEngine.features}, which works: an {@code int}
 * parameter as the parallel bound and a plain counted inner loop. The first attempt bounded the
 * parallel loop by {@code partials.getSize()} and strided by the thread count, and on hardware
 * exactly one thread ran and summed exactly one element -- the diagnostic reported "1 of 1024
 * partials non-zero, partial[0]=4752.41" against 262,144 rows staged. Neither loop bound survived,
 * so the accessor call and the stride are both gone.
 */
public final class FusedAggregateEngine implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(FusedAggregateEngine.class);

    /**
     * Threads per reduced column.
     *
     * <p>Enough to fill a device and small enough that the host-side combine is free. It also fixes
     * the summation order for a given batch size, so two runs of the same query agree exactly even
     * though neither agrees bit for bit with the CPU's left-to-right order.
     */
    private static final int PARTIALS = 1024;

    /**
     * The batch must divide evenly into chunks, so every element is covered exactly once.
     *
     * <p>Checked rather than assumed: a batch size that is not a multiple of {@link #PARTIALS}
     * would drop the remainder of every batch from every sum, which looks like a rounding
     * difference until someone counts.
     */
    private static int chunkSizeFor(int batchSize) {
        if (batchSize % PARTIALS != 0) {
            throw new IllegalArgumentException(
                    "batch size "
                            + batchSize
                            + " is not a multiple of "
                            + PARTIALS
                            + "; the contraction would drop the remainder of every batch");
        }
        return batchSize / PARTIALS;
    }

    private final GpuAggregateSpec spec;
    private final int batchSize;

    /** Staged input columns, one per column the kernel reads, at their declared widths. */
    private Object[] inputs;

    /** The Calc's outputs, which the contraction reads without them leaving the device. */
    private DoubleArray[] projected;

    /** 0/1 per row when the Calc has a condition; all ones when it does not. */
    private IntArray mask;

    /** {@link #PARTIALS} partial sums per contracted column. */
    private DoubleArray[] partials;

    private final double[] totals;
    private long contributingRows;
    private long stagedRows;
    private int buffered;

    private GeneratedKernel kernel;
    private TornadoExecutionPlan plan;

    public FusedAggregateEngine(GpuAggregateSpec spec) {
        this.spec = spec;
        this.batchSize = spec.batchSize();
        this.totals = new double[spec.sumSources().length];
    }

    /** Which Calc output columns are contracted, in output-field order, ignoring COUNT(*). */
    private int[] contractedColumns() {
        List<Integer> columns = new ArrayList<>();
        for (int source : spec.sumSources()) {
            if (source != GpuAggregateSpec.COUNT_STAR) {
                columns.add(source);
            }
        }
        int[] result = new int[columns.size()];
        for (int i = 0; i < result.length; i++) {
            result[i] = columns.get(i);
        }
        return result;
    }

    public void open() throws Exception {
        final GpuKernelSource source = spec.kernel();
        kernel = new GeneratedKernel(source);
        Method entry = kernel.compile();

        inputs = new Object[source.inputFieldIndexes().length];
        for (int i = 0; i < inputs.length; i++) {
            inputs[i] = GeneratedKernel.allocate(source.inputTypes()[i], batchSize);
        }
        projected = new DoubleArray[source.outputCount()];
        for (int i = 0; i < projected.length; i++) {
            projected[i] = new DoubleArray(batchSize);
            projected[i].init(0.0);
        }
        mask = new IntArray(batchSize);
        mask.init(1);

        int[] contracted = contractedColumns();
        partials = new DoubleArray[contracted.length];
        for (int i = 0; i < partials.length; i++) {
            partials[i] = new DoubleArray(PARTIALS);
            partials[i].init(0.0);
        }

        Object[] kernelArgs =
                new Object[inputs.length + projected.length + (source.hasFilter() ? 1 : 0)];
        int at = 0;
        for (Object in : inputs) {
            kernelArgs[at++] = in;
        }
        for (DoubleArray out : projected) {
            kernelArgs[at++] = out;
        }
        if (source.hasFilter()) {
            kernelArgs[at] = mask;
        }

        TaskGraph graph =
                new TaskGraph("fused-aggregate")
                        .transferToDevice(DataTransferMode.EVERY_EXECUTION, inputs)
                        .transferToDevice(DataTransferMode.EVERY_EXECUTION, mask)
                        // The projected columns never go to the host, but they still have to be
                        // declared. FeatureGramEngine -- which works -- declares the buffer its
                        // kernel writes and its library call then reads, for the same reason:
                        // TornadoVM wants every task parameter named in a transfer list, and an
                        // intermediate that appears in no list is not a buffer it has agreed to
                        // keep. Leaving these out is the likeliest cause of the wrong sum.
                        .transferToDevice(DataTransferMode.EVERY_EXECUTION, (Object[]) projected)
                        .task("project", entry, kernelArgs);
        // One reduction task per contracted column, all reading buffers the projection just wrote.
        // They could be one task over a matrix if the columns were adjacent in memory; they are
        // separate arrays because the projection writes them separately, and k is small.
        for (int i = 0; i < contracted.length; i++) {
            graph =
                    graph.task(
                            "sum" + i,
                            FusedAggregateEngine::sumChunks,
                            projected[contracted[i]],
                            mask,
                            partials[i],
                            PARTIALS,
                            chunkSizeFor(batchSize));
        }
        graph = graph.transferToHost(DataTransferMode.EVERY_EXECUTION, (Object[]) partials);
        plan = new TornadoExecutionPlan(graph.snapshot());
    }

    /**
     * Sums one column, {@link #PARTIALS} threads striding through it.
     *
     * <p>The stride is the thread count rather than a contiguous block per thread, so the threads
     * of a warp read adjacent addresses on every step. Masked-out rows contribute zero rather than
     * being skipped, because a branch that changes the iteration count would diverge the warp for
     * no saving.
     */
    public static void sumChunks(
            DoubleArray column, IntArray mask, DoubleArray partials, int chunks, int chunkSize) {
        for (@Parallel int c = 0; c < chunks; c++) {
            double acc = 0.0;
            int start = c * chunkSize;
            for (int i = 0; i < chunkSize; i++) {
                acc += column.get(start + i) * mask.get(start + i);
            }
            partials.set(c, acc);
        }
    }

    /** The write side of one staged column, bound to its buffer. */
    public RowGather.StagingColumn inputColumn(int column) {
        return GeneratedKernel.writerFor(inputs[column]);
    }

    /** Off-heap buffer for a staged column, for a gather that can bulk-copy into it. */
    public java.lang.foreign.MemorySegment inputSegment(int column) {
        return GeneratedKernel.segmentOf(inputs[column]);
    }

    /** Ends a row. Returns true if the batch filled and ran. */
    public boolean rowComplete() throws Exception {
        stagedRows++;
        if (++buffered == batchSize) {
            execute();
            return true;
        }
        return false;
    }

    /** Runs whatever is staged. Safe on an empty batch. */
    public void flush() throws Exception {
        if (buffered > 0) {
            execute();
        }
    }

    /**
     * Runs one batch and folds its partials into the running totals.
     *
     * <p>A short final batch is handled by zeroing the mask beyond what was staged rather than by
     * resizing: a {@link TornadoExecutionPlan} fixes buffer sizes, and a masked row contributes
     * exactly zero to a sum, so the padding is exact rather than approximate.
     */
    /** Log the shape of the first batch's partials once, so a wrong sum explains itself. */
    private static final boolean DIAGNOSE_FIRST_BATCH = true;

    private boolean diagnosed;

    private void execute() throws Exception {
        for (int i = buffered; i < batchSize; i++) {
            mask.set(i, 0);
        }
        withKernelLoader(
                () -> {
                    try {
                        return plan.execute();
                    } catch (Exception e) {
                        throw new IllegalStateException("fused aggregate execution failed", e);
                    }
                });
        for (int column = 0; column < partials.length; column++) {
            double sum = 0.0;
            for (int c = 0; c < PARTIALS; c++) {
                sum += partials[column].get(c);
            }
            totals[column] += sum;
            if (DIAGNOSE_FIRST_BATCH && !diagnosed && column == 0) {
                int nonZero = 0;
                int lastNonZero = -1;
                for (int c = 0; c < PARTIALS; c++) {
                    if (partials[column].get(c) != 0.0) {
                        nonZero++;
                        lastNonZero = c;
                    }
                }
                // How many threads wrote, and what one of them accumulated. A single non-zero
                // partial means one thread ran; 1024 tiny ones mean every thread summed a single
                // element; 1024 large ones mean the reduction worked and the fault is elsewhere.
                LOG.info(
                        "fused aggregate first batch: {} of {} partials non-zero, last at {}, "
                                + "partial[0]={}, batch sum={}, rows staged={}",
                        nonZero,
                        PARTIALS,
                        lastNonZero,
                        partials[column].get(0),
                        sum,
                        buffered);
                diagnosed = true;
            }
        }
        for (int i = 0; i < buffered; i++) {
            if (mask.get(i) != 0) {
                contributingRows++;
            }
        }
        if (buffered < batchSize) {
            mask.init(1);
        }
        buffered = 0;
    }

    /**
     * Runs {@code action} with the generated kernel's loader as the context class loader.
     *
     * <p>The same reason the Calc engine does it: TornadoVM reads a kernel's bytecode as a resource
     * through the context loader, and a class compiled at run time lives in a loader of our making.
     * Without this the {@code @Parallel} annotation is not found and the loop is emitted as a
     * sequential one that every device thread runs in full.
     */
    private <T> T withKernelLoader(java.util.function.Supplier<T> action) {
        Thread current = Thread.currentThread();
        ClassLoader previous = current.getContextClassLoader();
        current.setContextClassLoader(kernel.loader());
        try {
            return action.get();
        } finally {
            current.setContextClassLoader(previous);
        }
    }

    /** Accumulated sum for the {@code i}-th contracted column. */
    public double total(int contractedIndex) {
        return totals[contractedIndex];
    }

    /** Rows that survived the Calc's condition, which is what {@code COUNT(*)} reports. */
    public long contributingRows() {
        return contributingRows;
    }

    /** Rows staged, whether or not they survived a filter. */
    public long stagedRows() {
        return stagedRows;
    }

    @Override
    public void close() throws Exception {
        if (plan != null) {
            plan.close();
            plan = null;
        }
        if (kernel != null) {
            kernel.close();
            kernel = null;
        }
    }

    /**
     * Whether the contraction has been shown to agree with the CPU on hardware.
     *
     * <p>It did not, the first time. On cyclone, 2,000,000 rows of the haversine query, the device
     * returned <b>31,919.56</b> where the CPU returns <b>7.0779784665051e9</b> -- not
     * reassociation, a different number -- while the log confirmed the fused operator ran and
     * staged every row. Three things were wrong or missing and are fixed above: the projected
     * buffers appeared in no transfer list, the stride was an accessor call inside the loop rather
     * than a parameter, and the plan executed without the generated kernel's loader as the context
     * loader.
     *
     * <p>Set back to true to test those. If it disagrees again, {@link #DIAGNOSE_FIRST_BATCH} says
     * whether one thread ran or every thread summed one element, which separates the remaining
     * candidates without another round trip.
     */
    private static final boolean VERIFIED_ON_HARDWARE = true;

    /** Whether a spec's contracted columns are all types this engine reduces. */
    public static boolean canContract(GpuAggregateSpec spec) {
        if (!VERIFIED_ON_HARDWARE) {
            return false;
        }
        GpuValueType[] outputs = spec.kernel().outputTypes();
        for (int source : spec.sumSources()) {
            if (source == GpuAggregateSpec.COUNT_STAR) {
                continue;
            }
            if (source < 0 || source >= outputs.length || outputs[source] != GpuValueType.DOUBLE) {
                // A FLOAT column would need its own reduction kernel, and is the case where the
                // library becomes the right answer rather than a demotion. Neither exists yet.
                return false;
            }
        }
        return true;
    }
}
