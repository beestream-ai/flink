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

package org.apache.flink.table.gpu;

import org.apache.flink.table.gpu.operator.FusedAggregateEngine;
import org.apache.flink.table.runtime.gpu.GpuAggregateSpec;
import org.apache.flink.table.runtime.gpu.GpuKernelSource;
import org.apache.flink.table.runtime.gpu.GpuValueType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.RowType;

import java.util.Collections;

/**
 * Drives {@link FusedAggregateEngine} directly, with no Flink around it.
 *
 * <p>Written because four hypotheses were tested against a cluster at a quarter of an hour each and
 * none was right. A hand-built approximation of the same graph -- two tasks, a generated kernel,
 * with and without an explicit grid -- runs correctly, so the fault is not the graph shape. This
 * runs the <em>actual engine</em> instead of something shaped like it, which is the only way to
 * tell an engine bug from a Flink one.
 *
 * <pre>{@code
 * java @$TORNADO_SDK/tornado-argfile -cp <gpu-runtime>:<table-runtime>:<table-common>:<api> \
 *     org.apache.flink.table.gpu.FusedAggregateSweep
 * }</pre>
 *
 * <p>Every row is 1.0, so the sum is the row count and any shortfall says immediately how much of
 * the buffer was actually reduced.
 */
public final class FusedAggregateSweep {

    private static final int ROWS = 1_000_000;
    private static final int BATCH = 262_144;

    private FusedAggregateSweep() {}

    /** {@code SELECT SUM(c0 * 2.0)}: one staged column, one projected column, no filter. */
    private static GpuAggregateSpec spec() {
        String source =
                "import uk.ac.manchester.tornado.api.annotations.Parallel;\n"
                        + "import uk.ac.manchester.tornado.api.types.arrays.DoubleArray;\n\n"
                        + "public final class GpuCalcKernel$Sweep {\n\n"
                        + "    public static void evaluate(DoubleArray c0_in, DoubleArray out0) {\n"
                        + "        for (@Parallel int i = 0; i < c0_in.getSize(); i++) {\n"
                        + "            double c0 = c0_in.get(i);\n"
                        + "            out0.set(i, (c0 * 2.0));\n"
                        + "        }\n"
                        + "    }\n"
                        + "}\n";
        GpuKernelSource kernel =
                new GpuKernelSource(
                        "GpuCalcKernel$Sweep",
                        "evaluate",
                        source,
                        new int[] {0},
                        new GpuValueType[] {GpuValueType.DOUBLE},
                        new GpuValueType[] {GpuValueType.DOUBLE},
                        false,
                        new int[] {-1});
        RowType calcOut =
                RowType.of(
                        new org.apache.flink.table.types.logical.LogicalType[] {new DoubleType()},
                        new String[] {"h"});
        RowType out =
                RowType.of(
                        new org.apache.flink.table.types.logical.LogicalType[] {new DoubleType()},
                        new String[] {"sum"});
        return new GpuAggregateSpec(kernel, new int[] {0}, calcOut, out, BATCH, 100);
    }

    public static void main(String[] args) throws Exception {
        GpuAggregateSpec spec = spec();
        System.out.println("canContract=" + FusedAggregateEngine.canContract(spec));

        try (FusedAggregateEngine engine = new FusedAggregateEngine(spec)) {
            engine.open();
            for (int i = 0; i < ROWS; i++) {
                engine.inputColumn(0).set(i % BATCH, 1.0);
                engine.rowComplete();
            }
            engine.flush();
            double got = engine.total(0);
            double want = 2.0 * ROWS;
            System.out.printf(
                    "rows=%d staged=%d contributed=%d total=%.1f want=%.1f  %s%n",
                    ROWS,
                    engine.stagedRows(),
                    engine.contributingRows(),
                    got,
                    want,
                    got == want ? "OK" : "WRONG, reduced " + (got / 2.0) + " rows");
        }
        Collections.emptyList();
    }
}
