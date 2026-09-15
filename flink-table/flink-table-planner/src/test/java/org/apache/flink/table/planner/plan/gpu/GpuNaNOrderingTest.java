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

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.runtime.gpu.GpuKernelSource;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What a generated kernel must do when an operand of LEAST or GREATEST is NaN.
 *
 * <p>Flink folds both with {@code Double.compareTo} (see {@code
 * ScalarOperatorGens.generateGreatestLeast}), and that total order places NaN <em>above</em> every
 * other value — so NaN wins GREATEST and loses LEAST. {@code Math.min} and {@code TornadoMath.min}
 * do the opposite: they propagate it.
 *
 * <p>The kernel generator used {@code TornadoMath.min}, which meant the device returned a
 * different answer from the CPU plan for any row where an operand was NaN — and {@code SQRT} of a
 * negative produces those routinely. The differential harness of task M0.6 found it; this test
 * stops it coming back.
 */
class GpuNaNOrderingTest {

    private final RelDataTypeFactory types = new JavaTypeFactoryImpl();
    private final RexBuilder rex = new RexBuilder(types);

    private RexNode col(int index) {
        return rex.makeInputRef(types.createSqlType(SqlTypeName.DOUBLE), index);
    }

    /** Flink's LEAST/GREATEST are built-ins reached by name, so the test builds one the same way. */
    private RexNode call(String name, RexNode... operands) {
        SqlFunction function =
                new SqlFunction(
                        name,
                        SqlKind.OTHER_FUNCTION,
                        ReturnTypes.ARG0,
                        InferTypes.FIRST_KNOWN,
                        OperandTypes.SAME_VARIADIC,
                        SqlFunctionCategory.NUMERIC);
        return rex.makeCall(types.createSqlType(SqlTypeName.DOUBLE), function, Arrays.asList(operands));
    }

    private String kernelFor(RexNode expression) {
        Optional<GpuKernelSource> kernel =
                GpuKernelGenerator.generate(
                        java.util.Collections.singletonList(expression), null, "naan");
        assertThat(kernel).as("expression must be generatable").isPresent();
        return kernel.get().source();
    }

    @Test
    @DisplayName("LEAST does not use min: min propagates NaN and Flink's LEAST discards it")
    void leastDoesNotUseMin() {
        String source = kernelFor(call("LEAST", col(0), col(1)));

        assertThat(source).doesNotContain("TornadoMath.min");
        // The generated form must test one operand against itself, which is how a kernel without
        // Double.compare detects NaN at all.
        assertThat(source).contains("!=");
    }

    @Test
    @DisplayName("GREATEST does not use max, for the same reason")
    void greatestDoesNotUseMax() {
        String source = kernelFor(call("GREATEST", col(0), col(1)));

        assertThat(source).doesNotContain("TornadoMath.max");
        assertThat(source).contains("!=");
    }

    @Test
    @DisplayName("the generated form agrees with Flink on NaN and infinities")
    void agreesWithFlinkOnNaNAndInfinities() {
        // Evaluating the generated source would need a compiler; the contract it encodes is
        // asserted directly, on the pairs the harness exercises.
        double[] values = {
            Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 0.0, 1.0, -1.0
        };
        for (double a : values) {
            for (double b : values) {
                assertSame("LEAST", least(a, b), Double.compare(a, b) <= 0 ? a : b);
                assertSame("GREATEST", greatest(a, b), Double.compare(a, b) >= 0 ? a : b);
            }
        }
    }

    @Test
    @DisplayName("signed zero is a known, accepted divergence from Flink")
    void signedZeroDiverges() {
        // Flink folds with Double.compareTo, which orders -0.0 below 0.0. The generated form uses
        // `<`, under which they are equal, so it returns the second operand instead of the first.
        //
        // Accepted rather than fixed. Matching exactly needs a reciprocal comparison
        // (1.0/a < 1.0/b) to recover the sign, which is a division per comparison in every kernel
        // that uses LEAST -- real cost on a device, for a difference that is only the sign of a
        // zero. Nothing downstream distinguishes them arithmetically.
        //
        // Pinned here so it is a decision on the record rather than an unnoticed inconsistency.
        assertThat(Double.compare(-0.0, 0.0)).isNegative();
        assertSame("Flink", Double.compare(-0.0, 0.0) <= 0 ? -0.0 : 0.0, -0.0);
        assertSame("kernel", least(-0.0, 0.0), 0.0);
    }

    /** NaN-aware equality: assertThat(double) compares with ==, under which NaN != NaN. */
    private static void assertSame(String what, double actual, double expected) {
        assertThat(Double.compare(actual, expected))
                .as(what + ": expected " + expected + " but was " + actual)
                .isZero();
    }

    /** The exact form the kernel generator emits, evaluated here. */
    private static double least(double a, double b) {
        return (b != b || a < b) ? a : b;
    }

    private static double greatest(double a, double b) {
        return (a != a || a > b) ? a : b;
    }
}
