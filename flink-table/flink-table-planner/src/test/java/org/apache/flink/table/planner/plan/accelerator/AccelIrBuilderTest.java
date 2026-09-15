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

import org.apache.flink.table.accelerator.AccelCall;
import org.apache.flink.table.accelerator.AccelExpression;
import org.apache.flink.table.accelerator.AccelFunction;
import org.apache.flink.table.accelerator.AccelInputRef;
import org.apache.flink.table.accelerator.AccelWorkProfile;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.utils.PlannerMocks;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The IR round trip: a real {@code RexNode} tree in, accelerator IR out, with nothing lost that a
 * provider needs in order to decide.
 */
class AccelIrBuilderTest {

    private final FlinkTypeFactory types =
            (FlinkTypeFactory) PlannerMocks.newBuilder().build().getPlannerContext()
                    .getTypeFactory();
    private final RexBuilder rex = new RexBuilder(types);

    private RelDataType t(SqlTypeName name, boolean nullable) {
        return types.createTypeWithNullability(types.createSqlType(name), nullable);
    }

    private RexNode col(SqlTypeName name, int index, boolean nullable) {
        return rex.makeInputRef(t(name, nullable), index);
    }

    private RexNode lit(double value) {
        return rex.makeLiteral(BigDecimal.valueOf(value), t(SqlTypeName.DOUBLE, false));
    }

    @Test
    @DisplayName("an arithmetic tree survives conversion with its shape intact")
    void arithmeticRoundTrips() {
        // val * 2.0 + 1.0
        RexNode expr =
                rex.makeCall(
                        SqlStdOperatorTable.PLUS,
                        rex.makeCall(
                                SqlStdOperatorTable.MULTIPLY,
                                col(SqlTypeName.DOUBLE, 1, false),
                                lit(2.0)),
                        lit(1.0));

        AccelExpression ir = AccelIrBuilder.convert(expr).orElseThrow(AssertionError::new);

        assertThat(ir).isInstanceOf(AccelCall.class);
        AccelCall plus = (AccelCall) ir;
        assertThat(plus.function()).isEqualTo(AccelFunction.PLUS);
        assertThat(plus.operands()).hasSize(2);

        AccelCall times = (AccelCall) plus.operands().get(0);
        assertThat(times.function()).isEqualTo(AccelFunction.TIMES);
        assertThat(times.operands().get(0)).isInstanceOf(AccelInputRef.class);
        assertThat(((AccelInputRef) times.operands().get(0)).index()).isEqualTo(1);
    }

    @Test
    @DisplayName("nullability travels with the type, which is the point of reusing LogicalType")
    void nullabilityIsCarried() {
        AccelExpression nullable =
                AccelIrBuilder.convert(col(SqlTypeName.DOUBLE, 0, true))
                        .orElseThrow(AssertionError::new);
        AccelExpression notNull =
                AccelIrBuilder.convert(col(SqlTypeName.DOUBLE, 0, false))
                        .orElseThrow(AssertionError::new);

        assertThat(nullable.outputType().isNullable()).isTrue();
        assertThat(notNull.outputType().isNullable()).isFalse();
        assertThat(notNull.outputType().getTypeRoot()).isEqualTo(LogicalTypeRoot.DOUBLE);
    }

    @Test
    @DisplayName("a built-in maps by identity; SQRT becomes the enum constant, not a string")
    void builtInMapsToEnum() {
        RexNode sqrt =
                rex.makeCall(SqlStdOperatorTable.SQRT, col(SqlTypeName.DOUBLE, 0, false));

        AccelCall ir = (AccelCall) AccelIrBuilder.convert(sqrt).orElseThrow(AssertionError::new);

        assertThat(ir.function()).isEqualTo(AccelFunction.SQRT);
    }

    @Test
    @DisplayName("an operator with no mapping is refused rather than guessed at")
    void unmappedOperatorIsRefused() {
        // CHAR_LENGTH is a perfectly good SQL function with no accelerator representation.
        RexNode unsupported =
                rex.makeCall(
                        SqlStdOperatorTable.CHAR_LENGTH, col(SqlTypeName.VARCHAR, 0, false));

        assertThat(AccelIrBuilder.convert(unsupported)).isEmpty();
    }

    @Test
    @DisplayName("work is counted per operation, not collapsed to a weighted scalar")
    void workIsAHistogram() {
        RexNode expr =
                rex.makeCall(
                        SqlStdOperatorTable.PLUS,
                        rex.makeCall(
                                SqlStdOperatorTable.SQRT, col(SqlTypeName.DOUBLE, 0, false)),
                        rex.makeCall(
                                SqlStdOperatorTable.SQRT, col(SqlTypeName.DOUBLE, 1, false)));

        List<AccelExpression> converted =
                Collections.singletonList(
                        AccelIrBuilder.convert(expr).orElseThrow(AssertionError::new));
        AccelWorkProfile work = AccelIrBuilder.profile(converted, 16, 8, 2_000_000L);

        assertThat(work.count(AccelFunction.SQRT)).isEqualTo(2);
        assertThat(work.count(AccelFunction.PLUS)).isEqualTo(1);
        assertThat(work.count(AccelFunction.TIMES)).isZero();
        assertThat(work.totalOpsPerRow()).isEqualTo(3);
        assertThat(work.inputBytesPerRow()).isEqualTo(16);
        assertThat(work.estimatedRows()).isEqualTo(2_000_000L);
    }

    @Test
    @DisplayName("the IR serializes: it has to travel in the JobGraph")
    void serializes() throws Exception {
        RexNode expr =
                rex.makeCall(
                        SqlStdOperatorTable.MULTIPLY,
                        col(SqlTypeName.DOUBLE, 1, true),
                        lit(2.0));
        AccelExpression ir = AccelIrBuilder.convert(expr).orElseThrow(AssertionError::new);
        AccelWorkProfile work =
                AccelIrBuilder.profile(Collections.singletonList(ir), 8, 8, 100L);

        assertThat(roundTrip(ir)).isInstanceOf(AccelCall.class);
        assertThat(roundTrip(work)).isEqualTo(work);
    }

    @Test
    @DisplayName("every AccelFunction has a slot in the work profile")
    void profileCoversEveryFunction() {
        int[] counts = new int[AccelFunction.values().length];
        Arrays.fill(counts, 1);
        AccelWorkProfile work = new AccelWorkProfile(counts, 8, 8, 1L);

        for (AccelFunction function : AccelFunction.values()) {
            assertThat(work.count(function)).as(function.name()).isEqualTo(1);
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> T roundTrip(T value) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(value);
        }
        try (ObjectInputStream in =
                new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            return (T) in.readObject();
        }
    }
}
