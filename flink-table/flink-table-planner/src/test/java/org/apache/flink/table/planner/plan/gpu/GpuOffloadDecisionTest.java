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

import org.apache.flink.table.accelerator.AccelFunction;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What the planner may and may not decide.
 *
 * <p>This class used to test two calibrated floors — 96 weighted operations per row, ten million
 * total — and the arithmetic of comparing a score against them. Those floors were a model of one
 * RTX 4070 living inside Flink, and predicting the behaviour of hardware the client cannot see.
 * They are gone, and so are the tests for them.
 *
 * <p>What remains is the half the planner is actually qualified to judge: whether a subtree can be
 * represented at all, and how much work it performs. Whether that work is worth moving is decided
 * on the TaskManager, against a device, and is tested there.
 */
class GpuOffloadDecisionTest {

    private final RelDataTypeFactory types = new JavaTypeFactoryImpl();
    private final RexBuilder rex = new RexBuilder(types);
    private final GpuOffloadDecision decision = new GpuOffloadDecision();

    private RexNode val() {
        return rex.makeInputRef(types.createSqlType(SqlTypeName.DOUBLE), 1);
    }

    private RexNode lit(double v) {
        return rex.makeLiteral(BigDecimal.valueOf(v), types.createSqlType(SqlTypeName.DOUBLE));
    }

    /** {@code val * 2.0 + 1.0}: three operations, measured at 0.54x against CPU. */
    private RexNode cheap() {
        return rex.makeCall(
                SqlStdOperatorTable.PLUS,
                rex.makeCall(SqlStdOperatorTable.MULTIPLY, val(), lit(2.0)),
                lit(1.0));
    }

    private RexNode heavy() {
        return rex.makeCall(
                SqlStdOperatorTable.PLUS,
                rex.makeCall(SqlStdOperatorTable.SQRT, val()),
                rex.makeCall(SqlStdOperatorTable.EXP, val()));
    }

    @Test
    @DisplayName("an expressible subtree is eligible, and the verdict carries the work")
    void expressibleIsEligible() {
        GpuOffloadDecision.Verdict v = decision.decide(Collections.singletonList(heavy()));

        assertThat(v.offload()).isTrue();
        assertThat(v.work()).isNotNull();
        assertThat(v.work().count(AccelFunction.SQRT)).isEqualTo(1);
        assertThat(v.work().count(AccelFunction.EXP)).isEqualTo(1);
        assertThat(v.work().count(AccelFunction.PLUS)).isEqualTo(1);
        assertThat(v.opsPerRow()).isEqualTo(3);
    }

    @Test
    @DisplayName(
            "the query measured at 0.54x is still eligible: cheapness is not the planner's call")
    void cheapIsStillEligible() {
        // The old behaviour was to refuse this at plan time against a floor of 96. That refusal
        // was right about the outcome and wrong about the authority: the client cannot know what
        // device will run it, or whether that device is fast enough to make three operations pay.
        GpuOffloadDecision.Verdict v = decision.decide(Collections.singletonList(cheap()));

        assertThat(v.offload()).isTrue();
        assertThat(v.opsPerRow()).isEqualTo(2);
        assertThat(v.reason()).contains("TaskManager");
    }

    @Test
    @DisplayName("an inexpressible subtree is refused, and the reason names what stopped it")
    void inexpressibleIsRefused() {
        RexNode decimalColumn =
                rex.makeInputRef(types.createSqlType(SqlTypeName.DECIMAL, 10, 2), 0);

        GpuOffloadDecision.Verdict v = decision.decide(Collections.singletonList(decimalColumn));

        assertThat(v.offload()).isFalse();
        assertThat(v.work()).isNull();
        assertThat(v.reason()).contains("not expressible on device");
    }

    @Test
    @DisplayName("a pure projection has nothing to move")
    void pureProjectionIsRefused() {
        GpuOffloadDecision.Verdict v = decision.decide(Collections.singletonList(val()));

        assertThat(v.offload()).isFalse();
        assertThat(v.reason()).contains("no arithmetic");
    }

    @Test
    @DisplayName("a subtree's work is the sum of its members'")
    void subtreeWorkSums() {
        RowCost one = GpuCostEstimator.estimateAll(Collections.singletonList(heavy()));
        RowCost two = GpuCostEstimator.estimateAll(Collections.singletonList(heavy()));

        GpuOffloadDecision.Verdict v = decision.forSubtree(Arrays.asList(one, two));

        assertThat(v.offload()).isTrue();
        assertThat(v.work().count(AccelFunction.SQRT)).isEqualTo(2);
        assertThat(v.opsPerRow()).isEqualTo(6);
    }

    @Test
    @DisplayName("one ineligible member poisons the subtree, and its reason is the one reported")
    void ineligibleMemberPoisonsSubtree() {
        RowCost good = GpuCostEstimator.estimateAll(Collections.singletonList(heavy()));
        RowCost bad = RowCost.ineligible("a very specific reason");

        GpuOffloadDecision.Verdict v = decision.forSubtree(Arrays.asList(good, bad));

        assertThat(v.offload()).isFalse();
        assertThat(v.reason()).contains("a very specific reason");
    }

    @Test
    @DisplayName("a fallback keeps the work, so EXPLAIN can still say what the node was worth")
    void fallbackKeepsTheWork() {
        GpuOffloadDecision.Verdict selected = decision.decide(Collections.singletonList(heavy()));

        GpuOffloadDecision.Verdict fell =
                GpuOffloadDecision.fallback(selected, "no provider on this TaskManager");

        assertThat(fell.offload()).isFalse();
        assertThat(fell.work()).isEqualTo(selected.work());
        assertThat(fell.reason()).contains("selected but fell back");
        assertThat(fell.reason()).contains("no provider");
    }

    @Test
    @DisplayName("no device constant survives anywhere on this class")
    void noDeviceConstants() {
        // The property the FLIP rests on: Flink holds no calibrated hardware number. A threshold
        // reintroduced here would be invisible in a diff and would undo the whole argument.
        for (Field field : GpuOffloadDecision.class.getDeclaredFields()) {
            assertThat(field.getName().toUpperCase())
                    .as("field " + field.getName())
                    .doesNotContain("MIN_")
                    .doesNotContain("FLOOR")
                    .doesNotContain("BREAK_EVEN")
                    .doesNotContain("THRESHOLD");
        }
        List<String> methods =
                Arrays.stream(GpuOffloadDecision.class.getDeclaredMethods())
                        .map(m -> m.getName().toLowerCase())
                        .collect(java.util.stream.Collectors.toList());
        assertThat(methods).noneMatch(m -> m.contains("floor") || m.contains("threshold"));
    }
}
