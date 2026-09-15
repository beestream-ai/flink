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

package org.apache.flink.table.runtime.accelerator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.accelerator.AccelNode;
import org.apache.flink.table.accelerator.AccelWorkProfile;
import org.apache.flink.table.types.logical.RowType;

import java.io.Serializable;

/**
 * Everything the runtime needs to offer one Calc to a provider.
 *
 * <p>Note what is <em>not</em> here: no kernel, no buffer layout, no staging widths. Those were
 * TornadoVM's, generated in the planner and carried through core Flink, which is what made the
 * offload a vendor integration rather than an extension point. A provider is handed the subtree and
 * decides all of that for itself.
 *
 * <p>Serializable because it travels in the JobGraph: built on the client, read on a TaskManager.
 */
@Internal
public final class AcceleratedCalcSpec implements Serializable {

    private static final long serialVersionUID = 1L;

    private final AccelNode subtree;
    private final AccelWorkProfile work;
    private final RowType outputType;
    private final int maxBatchSize;
    private final double minSpeedup;

    public AcceleratedCalcSpec(
            AccelNode subtree,
            AccelWorkProfile work,
            RowType outputType,
            int maxBatchSize,
            double minSpeedup) {
        this.subtree = subtree;
        this.work = work;
        this.outputType = outputType;
        this.maxBatchSize = maxBatchSize;
        this.minSpeedup = minSpeedup;
    }

    /** What to run, in accelerator IR. */
    public AccelNode subtree() {
        return subtree;
    }

    /** What it does per row, counted per operation rather than weighted. */
    public AccelWorkProfile work() {
        return work;
    }

    public RowType outputType() {
        return outputType;
    }

    /** Ceiling on rows staged per batch, from configuration. A provider may use fewer. */
    public int maxBatchSize() {
        return maxBatchSize;
    }

    /**
     * How much faster than the CPU a provider must claim to be before Flink prefers it.
     *
     * <p>Policy, not calibration: it means the same on any accelerator. Read from config on the
     * client and shipped, so the TaskManager needs no access to the job's table config.
     */
    public double minSpeedup() {
        return minSpeedup;
    }

    @Override
    public String toString() {
        return "AcceleratedCalcSpec[" + subtree + ", " + work + ", minSpeedup=" + minSpeedup + "]";
    }
}
