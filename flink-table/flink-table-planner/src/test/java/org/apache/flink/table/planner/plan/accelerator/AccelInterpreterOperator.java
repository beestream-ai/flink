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

package org.apache.flink.table.planner.plan.accelerator;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.groups.OperatorIOMetricGroup;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.accelerator.AccelExpression;
import org.apache.flink.table.accelerator.AccelNode;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/** Runs {@link AccelInterpreter} as a Flink operator, so the accelerator path is a real path. */
final class AccelInterpreterOperator extends AbstractStreamOperator<RowData>
        implements OneInputStreamOperator<RowData, RowData> {

    // One row in, at most one row out -- the same contract the code-generated Calc has.

    private static final long serialVersionUID = 1L;

    private final AccelNode subtree;
    private final RowType outputType;
    private final AtomicInteger rowsProcessed;

    private transient List<AccelExpression> projections;
    private transient AccelExpression condition;
    private transient GenericRowData out;
    private transient StreamRecord<RowData> element;

    AccelInterpreterOperator(AccelNode subtree, RowType outputType, AtomicInteger rowsProcessed) {
        this.subtree = subtree;
        this.outputType = outputType;
        this.rowsProcessed = rowsProcessed;
    }

    /**
     * What Flink published about this subtask's decision, by metric name.
     *
     * <p>Static because the operator a test wants to interrogate is built inside a MiniCluster and
     * never handed back. Reading it through a metrics reporter would be the obvious alternative and
     * does not work here: {@code TableEnvironment.create} starts its own cluster, so neither {@code
     * EnvironmentSettings.withConfiguration} nor a configured {@code MiniClusterExtension} reaches
     * the registry that would instantiate one.
     */
    public static final java.util.Map<String, Object> PUBLISHED =
            new java.util.concurrent.ConcurrentHashMap<>();

    /**
     * Records what is registered, and forwards nothing.
     *
     * <p>Flink registers the accelerator gauges on whatever operator the provider returned — which
     * is this one — so overriding the group is how a test sees them.
     */
    private transient CapturingGroup capturing;

    @Override
    public OperatorMetricGroup getMetricGroup() {
        if (capturing == null) {
            capturing = new CapturingGroup(super.getMetricGroup());
        }
        return capturing;
    }

    /**
     * Records the gauges and forwards nothing.
     *
     * <p>Nothing here needs real metrics; what the test needs is to see what Flink registered.
     * Wrapping the real group would be tidier and is not possible — {@code ProxyMetricGroup.gauge}
     * is final.
     */
    private static final class CapturingGroup extends UnregisteredMetricsGroup
            implements OperatorMetricGroup {

        private final OperatorMetricGroup real;

        CapturingGroup(OperatorMetricGroup real) {
            this.real = real;
        }

        @Override
        public <T, G extends Gauge<T>> G gauge(String name, G gauge) {
            PUBLISHED.put(name, gauge.getValue());
            return real.gauge(name, gauge);
        }

        @Override
        public OperatorIOMetricGroup getIOMetricGroup() {
            // Delegated, and it has to be: AbstractStreamOperator counts records through this
            // during setup, so a no-op group takes the operator down before a test sees anything.
            return real.getIOMetricGroup();
        }
    }

    @Override
    public void open() throws Exception {
        super.open();
        projections = AccelInterpreter.projectionsOf(subtree);
        condition = AccelInterpreter.conditionOf(subtree);
        out = new GenericRowData(projections.size());
        element = new StreamRecord<>(null);
    }

    @Override
    public void processElement(StreamRecord<RowData> record) throws Exception {
        RowData row = record.getValue();
        rowsProcessed.incrementAndGet();

        if (condition != null) {
            Double keep = AccelInterpreter.eval(condition, row);
            // SQL three-valued logic: UNKNOWN does not select the row, and neither does FALSE.
            if (keep == null || keep == 0.0) {
                return;
            }
        }
        for (int i = 0; i < projections.size(); i++) {
            Double value = AccelInterpreter.eval(projections.get(i), row);
            out.setField(i, value == null ? null : narrow(value, outputType.getTypeAt(i)));
        }
        output.collect(element.replace(out));
    }

    private static Object narrow(double value, LogicalType type) {
        LogicalTypeRoot root = type.getTypeRoot();
        if (root == LogicalTypeRoot.FLOAT) {
            return (float) value;
        }
        if (root == LogicalTypeRoot.INTEGER) {
            return (int) value;
        }
        if (root == LogicalTypeRoot.BIGINT) {
            return (long) value;
        }
        if (root == LogicalTypeRoot.BOOLEAN) {
            return value != 0.0;
        }
        return value;
    }
}
