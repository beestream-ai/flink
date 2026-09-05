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

import uk.ac.manchester.tornado.api.TaskGraph;
import uk.ac.manchester.tornado.api.TornadoExecutionPlan;
import uk.ac.manchester.tornado.api.TornadoExecutionResult;
import uk.ac.manchester.tornado.api.annotations.Parallel;
import uk.ac.manchester.tornado.api.enums.DataTransferMode;
import uk.ac.manchester.tornado.api.math.TornadoMath;
import uk.ac.manchester.tornado.api.types.arrays.FloatArray;
import uk.ac.manchester.tornado.cublas.CuBlas;
import uk.ac.manchester.tornado.cublas.enums.CuBlasOperation;

/**
 * Logistic regression by gradient descent, iterating entirely on the device.
 *
 * <p>Every benchmark before this one measured a single pass over the data, and every one of them
 * ended up bound by reading its input rather than by arithmetic. That is not a fact about devices,
 * it is a fact about single-pass queries: the work is linear in the bytes, so the ratio between a
 * device and a host is capped by how much of the job is arithmetic in the first place.
 *
 * <p>Iteration breaks that. The data is read once and stays resident; each pass adds arithmetic and
 * no I/O. The iteration count is therefore the knob the feature-map benchmark lacked, and it plays
 * the part {@code --depots} plays for the haversine query.
 *
 * <h2>Why it needs both a kernel and a library</h2>
 *
 * <p>One pass is four steps, and no single tool does all four:
 *
 * <pre>{@code
 * z = X w           cublasSgemv    contraction over features
 * r = sigmoid(z)-y  generated      elementwise transcendental
 * g = X' r          cublasSgemv    contraction over rows
 * w = w - lr*g      generated      elementwise, on features
 * }</pre>
 *
 * <p>cuBLAS has no sigmoid, and a generated kernel has no tiled GEMV. What makes it a hybrid rather
 * than two pipelines is the middle step: it reads the buffer the first library call wrote and
 * writes the buffer the second one reads, on the device, without the intermediate ever returning to
 * the host. On a host implementation that intermediate is an array of {@code rows} floats
 * materialised and re-read twice per iteration.
 *
 * <h2>Residency</h2>
 *
 * <p>{@code X} and {@code y} transfer under {@link DataTransferMode#FIRST_EXECUTION} and the
 * weights stay on the device across every iteration, so executing the plan {@code k} times moves no
 * data at all after the first. The weights come back once, at the end, under {@link
 * DataTransferMode#UNDER_DEMAND}. Nothing else in this project has exercised residency: the Calc
 * operator re-uploads every batch because every batch is different data.
 *
 * <h2>Float, and what it costs</h2>
 *
 * <p>FP32 throughout, because the binding has no FP64 GEMM. Gradient descent is more forgiving of
 * that than a covariance matrix is -- it is a fixed point iteration, so a small error per step is
 * corrected by the next one rather than accumulated -- but the caller should still compare the
 * weights against a double-precision reference rather than assume.
 */
public final class LogisticRegressionEngine implements AutoCloseable {

    private final int rows;
    private final int features;
    private final float learningRate;

    /** Column-major {@code rows x features}, so a feature's values are contiguous for cuBLAS. */
    private final FloatArray x;

    private final FloatArray y;
    private final FloatArray weights;

    /** {@code X w}, then overwritten with the residual. Never leaves the device. */
    private final FloatArray z;

    private final FloatArray gradient;

    private TornadoExecutionPlan plan;
    private int staged;

    public LogisticRegressionEngine(int rows, int features, float learningRate) {
        this.rows = rows;
        this.features = features;
        this.learningRate = learningRate;
        this.x = new FloatArray(rows * features);
        this.y = new FloatArray(rows);
        this.weights = new FloatArray(features);
        this.z = new FloatArray(rows);
        this.gradient = new FloatArray(features);
    }

    /** Stages one value of the row being built. */
    public void set(int feature, double value) {
        x.set(feature * rows + staged, (float) value);
    }

    /** Ends a row, with its label. */
    public void rowComplete(double label) {
        y.set(staged, (float) label);
        staged++;
    }

    public int staged() {
        return staged;
    }

    /**
     * Builds the plan. Call once, after every row is staged.
     *
     * <p>The graph is one iteration. Running {@code k} of them is {@code k} executions of the same
     * plan, which is what keeps the data resident: only the first moves {@code X} and {@code y}.
     */
    public void open() {
        TaskGraph graph =
                new TaskGraph("logistic")
                        .transferToDevice(DataTransferMode.FIRST_EXECUTION, x, y, weights)
                        .libraryTask(
                                "forward",
                                CuBlas::cublasSgemv,
                                CuBlasOperation.CUBLAS_OP_N.operation(),
                                rows,
                                features,
                                1.0f,
                                x,
                                rows,
                                weights,
                                1,
                                0.0f,
                                z,
                                1)
                        .task("residual", LogisticRegressionEngine::residual, z, y, rows)
                        .libraryTask(
                                "backward",
                                CuBlas::cublasSgemv,
                                CuBlasOperation.CUBLAS_OP_T.operation(),
                                rows,
                                features,
                                1.0f,
                                x,
                                rows,
                                z,
                                1,
                                0.0f,
                                gradient,
                                1)
                        .task(
                                "update",
                                LogisticRegressionEngine::update,
                                weights,
                                gradient,
                                learningRate / rows,
                                features)
                        .transferToHost(DataTransferMode.UNDER_DEMAND, weights);
        plan = new TornadoExecutionPlan(graph.snapshot());
    }

    /** Runs {@code iterations} passes. No data crosses the bus after the first. */
    public void train(int iterations) {
        try {
            TornadoExecutionResult result = null;
            for (int i = 0; i < iterations; i++) {
                result = plan.execute();
            }
            // UNDER_DEMAND, so the weights come back once here rather than after every pass.
            if (result != null) {
                result.transferToHost(weights);
            }
        } catch (Exception e) {
            throw new IllegalStateException("logistic regression execution failed", e);
        }
    }

    /**
     * The elementwise step, in place: {@code z := sigmoid(z) - y}.
     *
     * <p>In place because the value is consumed by the next library call from the same buffer.
     * Writing to a second array would cost another {@code rows}-sized allocation and another pass,
     * and buy nothing -- the previous GEMV has finished before this task starts.
     */
    public static void residual(FloatArray z, FloatArray y, int rows) {
        for (@Parallel int r = 0; r < rows; r++) {
            float sigmoid = 1.0f / (1.0f + TornadoMath.exp(-z.get(r)));
            z.set(r, sigmoid - y.get(r));
        }
    }

    /** The weight update, elementwise over features. Tiny, but it keeps the loop on the device. */
    public static void update(FloatArray weights, FloatArray gradient, float step, int features) {
        for (@Parallel int f = 0; f < features; f++) {
            weights.set(f, weights.get(f) - step * gradient.get(f));
        }
    }

    public double[] weights() {
        double[] out = new double[features];
        for (int f = 0; f < features; f++) {
            out[f] = weights.get(f);
        }
        return out;
    }

    @Override
    public void close() throws Exception {
        if (plan != null) {
            plan.close();
            plan = null;
        }
    }
}
