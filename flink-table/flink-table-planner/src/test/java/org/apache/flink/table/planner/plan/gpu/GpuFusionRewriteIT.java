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

package org.apache.flink.table.planner.plan.gpu;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.runtime.gpu.GpuOrCpuCalcOperatorFactory;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * M4.1: a fused pair is one node, and the graph says so.
 *
 * <p>Fusion used to happen during translation — the aggregate reached at its input, absorbed it,
 * and emitted one operator for both, while the absorbed {@code Calc} stayed in the graph producing
 * no transformation of its own. The plan then described more operators than the job had, which is
 * blocker C1: {@code EXPLAIN} and the JobGraph disagreed, and there was no way to tell from either
 * which one was lying.
 *
 * <p>These tests count both sides and compare them. Two names need excluding and both for a reason
 * rather than to make the numbers work: {@code Exchange} is realised as a partitioner on an edge
 * and never becomes an operator, and {@code TableToDataStream} and the sink are added by the
 * DataStream conversion below the table plan, so they have no exec node to correspond to.
 */
class GpuFusionRewriteIT {

    private static final String HEAVY =
            "EXP(val) * LN(val) + SIN(val) * COS(val) + POWER(val, 3.0)";
    private static final String QUERY = "SELECT SUM(h) FROM (SELECT " + HEAVY + " AS h FROM t)";

    private static StreamTableEnvironment env(StreamExecutionEnvironment exec, boolean fuse) {
        exec.setRuntimeMode(RuntimeExecutionMode.BATCH);
        exec.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(exec);
        tEnv.executeSql(
                "CREATE TABLE t (id BIGINT NOT NULL, val DOUBLE NOT NULL) WITH ("
                        + "'connector' = 'datagen', 'number-of-rows' = '100')");
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ACCELERATOR_ENABLED, true);
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ACCELERATOR_APPROXIMATE_PROJECTIONS, true);
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ACCELERATOR_FUSE_AGGREGATE, fuse);
        return tEnv;
    }

    /** The node names of the {@code == Optimized Execution Plan ==} section, in order. */
    private static List<String> executionPlanNodes(String explained) {
        int from = explained.indexOf("== Optimized Execution Plan ==");
        assertThat(from).as("no execution plan section in:\n" + explained).isNotNegative();
        int to = explained.indexOf("== GPU Offload ==", from);
        String section = to < 0 ? explained.substring(from) : explained.substring(from, to);
        List<String> names = new ArrayList<>();
        for (String line : section.split("\n")) {
            String trimmed = line.replace("+-", "").replace(":-", "").replace("|", "").trim();
            int paren = trimmed.indexOf('(');
            if (paren <= 0 || trimmed.startsWith("==")) {
                continue;
            }
            String name = trimmed.substring(0, paren);
            // Realised as a partitioner on an edge, never as an operator.
            if (!name.equals("Exchange")) {
                names.add(name);
            }
        }
        return names;
    }

    /**
     * Stream nodes that came from the table plan, so DataStream-side additions are dropped.
     *
     * <p>Takes the graph rather than the environment because {@code getStreamGraph()} clears the
     * transformations it consumed, so a second call finds an empty topology.
     */
    private static List<String> tableStreamNodes(StreamGraph graph) {
        List<String> names = new ArrayList<>();
        for (StreamNode node : graph.getStreamNodes()) {
            String name = node.getOperatorName();
            if (name.startsWith("TableToDataStream") || name.startsWith("Sink:")) {
                continue;
            }
            names.add(name);
        }
        return names;
    }

    @Test
    @DisplayName("with fusion on, every exec node in the plan has exactly one operator")
    void nodeCountEqualsTransformationCountWhenFused() {
        StreamExecutionEnvironment exec = StreamExecutionEnvironment.createLocalEnvironment();
        StreamTableEnvironment tEnv = env(exec, true);
        List<String> planned = executionPlanNodes(tEnv.explainSql(QUERY));
        tEnv.toDataStream(tEnv.sqlQuery(QUERY), Row.class).sinkTo(new DiscardingSink<>());

        assertThat(planned)
                .as("the fused pair must appear once, as one node")
                .anyMatch(n -> n.equals("AcceleratedGroup"));
        assertThat(planned)
                .as("its members must not also stand alone in the plan")
                .doesNotContain("Calc", "LocalHashAggregate");
        assertThat(tableStreamNodes(exec.getStreamGraph()))
                .as("one operator per exec node, which is the whole of C1")
                .hasSameSizeAs(planned);
    }

    @Test
    @DisplayName("and the JobGraph carries the fused node under the name the plan used")
    void explainAndTheJobGraphAgree() {
        StreamExecutionEnvironment exec = StreamExecutionEnvironment.createLocalEnvironment();
        StreamTableEnvironment tEnv = env(exec, true);
        tEnv.toDataStream(tEnv.sqlQuery(QUERY), Row.class).sinkTo(new DiscardingSink<>());
        List<String> operators = tableStreamNodes(exec.getStreamGraph());

        assertThat(operators)
                .as("EXPLAIN says AcceleratedGroup, so the job must contain one")
                .anyMatch(n -> n.startsWith("AcceleratedGroup"));
        assertThat(operators)
                .as("and must not also contain the members it absorbed")
                .noneMatch(n -> n.startsWith("Calc") || n.startsWith("LocalHashAggregate"));
    }

    @Test
    @DisplayName("with fusion off the counts still agree, and the members are their own nodes")
    void nodeCountEqualsTransformationCountWhenUnfused() {
        StreamExecutionEnvironment exec = StreamExecutionEnvironment.createLocalEnvironment();
        StreamTableEnvironment tEnv = env(exec, false);
        List<String> planned = executionPlanNodes(tEnv.explainSql(QUERY));
        tEnv.toDataStream(tEnv.sqlQuery(QUERY), Row.class).sinkTo(new DiscardingSink<>());

        assertThat(planned)
                .contains("Calc", "LocalHashAggregate")
                .doesNotContain("AcceleratedGroup");
        assertThat(tableStreamNodes(exec.getStreamGraph())).hasSameSizeAs(planned);
    }

    /**
     * On default settings the projection keeps its own accelerator path. M4.5.
     *
     * <p>This is the assertion the old default made false. Fusion removes the {@code Calc}'s {@code
     * GpuOrCpuCalcOperatorFactory} — the fused pair has no accelerator implementation to replace it
     * with — so while {@code fuse-aggregate} defaulted to true, the shape the whole fusion
     * machinery exists for was the one shape that could not offload at all. Nothing caught it: the
     * accelerator suites use plain projections, and the fusion suites assert what the processor
     * grouped, which is the same either way.
     */
    @Test
    @DisplayName("on default settings SELECT SUM(heavy) keeps its accelerator path")
    void theDefaultKeepsTheAcceleratorPath() {
        StreamExecutionEnvironment exec = StreamExecutionEnvironment.createLocalEnvironment();
        exec.setRuntimeMode(RuntimeExecutionMode.BATCH);
        exec.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(exec);
        tEnv.executeSql(
                "CREATE TABLE t (id BIGINT NOT NULL, val DOUBLE NOT NULL) WITH ("
                        + "'connector' = 'datagen', 'number-of-rows' = '100')");
        // Only the feature flag. Whether fusion happens must follow from the option's default.
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ACCELERATOR_ENABLED, true);
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ACCELERATOR_APPROXIMATE_PROJECTIONS, true);

        assertThat(
                        tEnv.getConfig()
                                .get(ExecutionConfigOptions.TABLE_EXEC_ACCELERATOR_FUSE_AGGREGATE))
                .as("this test is about the default, so it must not be set here")
                .isFalse();

        tEnv.toDataStream(tEnv.sqlQuery(QUERY), Row.class).sinkTo(new DiscardingSink<>());
        StreamGraph graph = exec.getStreamGraph();

        boolean calcCanOffload = false;
        for (StreamNode node : graph.getStreamNodes()) {
            if (node.getOperatorFactory() instanceof GpuOrCpuCalcOperatorFactory) {
                calcCanOffload = true;
            }
        }
        assertThat(calcCanOffload)
                .as("the projection must still carry both implementations into the JobGraph")
                .isTrue();
    }

    /**
     * The rewrite is skipped while a plan is being compiled.
     *
     * <p>The plan format knows the node types Flink ships and this is not one of them, so a
     * compiled plan containing it could not be read back by any release. M4.4 already settled that
     * a compiled plan does not offload, which makes fusing one pointless as well as unwritable.
     */
    @Test
    @DisplayName("a compiled plan contains no fused node")
    void compiledPlansAreNotFused() {
        StreamExecutionEnvironment exec = StreamExecutionEnvironment.createLocalEnvironment();
        StreamTableEnvironment tEnv = env(exec, true);
        tEnv.executeSql("CREATE TABLE snk (s DOUBLE) WITH ('connector' = 'blackhole')");

        String json = tEnv.compilePlanSql("INSERT INTO snk " + QUERY).asJsonString();

        assertThat(json).doesNotContain("AcceleratedGroup");
    }
}
