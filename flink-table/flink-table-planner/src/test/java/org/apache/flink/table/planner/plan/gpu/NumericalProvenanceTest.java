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

import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Which expressions count as computing a value a device may round differently.
 *
 * <p>The list this guards is duplicated by construction: the provider refuses the same functions in
 * a filter, and this refuses them in a projection. Two enforcement points, one list, and a function
 * added to one and not the other reopens the gap the provenance contract exists to close — so the
 * representative members are pinned here rather than left to a reader to notice.
 */
class NumericalProvenanceTest {

    private final FlinkTypeFactory typeFactory =
            new FlinkTypeFactory(
                    Thread.currentThread().getContextClassLoader(), FlinkTypeSystem.INSTANCE);

    private final RexBuilder rex = new RexBuilder(typeFactory);

    @Test
    @DisplayName("the transcendentals a device is not required to match are approximate")
    void transcendentalsAreApproximate() {
        for (SqlOperator drifting :
                List.of(
                        SqlStdOperatorTable.EXP,
                        SqlStdOperatorTable.LN,
                        SqlStdOperatorTable.SIN,
                        SqlStdOperatorTable.COS,
                        SqlStdOperatorTable.TAN,
                        SqlStdOperatorTable.ASIN,
                        SqlStdOperatorTable.ACOS,
                        SqlStdOperatorTable.ATAN,
                        SqlStdOperatorTable.SQRT,
                        SqlStdOperatorTable.POWER)) {
            assertThat(NumericalProvenance.computesApproximateValues(List.of(call(drifting)), null))
                    .as("%s must be treated as approximate", drifting.getName())
                    .isTrue();
        }
    }

    @Test
    @DisplayName("exact arithmetic is not approximate")
    void exactArithmeticIsExact() {
        // These agree bit for bit on the device -- measured over whole populations, not assumed --
        // and treating them as approximate would refuse every offload there is.
        for (SqlOperator exact :
                List.of(
                        SqlStdOperatorTable.PLUS,
                        SqlStdOperatorTable.MINUS,
                        SqlStdOperatorTable.MULTIPLY,
                        SqlStdOperatorTable.DIVIDE,
                        SqlStdOperatorTable.ABS)) {
            assertThat(NumericalProvenance.computesApproximateValues(List.of(call(exact)), null))
                    .as("%s must not be treated as approximate", exact.getName())
                    .isFalse();
        }
    }

    @Test
    @DisplayName("a transcendental buried inside exact arithmetic is still found")
    void nestingDoesNotHideIt() {
        RexNode nested =
                rex.makeCall(
                        SqlStdOperatorTable.PLUS,
                        literal(),
                        rex.makeCall(
                                SqlStdOperatorTable.MULTIPLY,
                                literal(),
                                call(SqlStdOperatorTable.EXP)));
        assertThat(NumericalProvenance.computesApproximateValues(List.of(nested), null)).isTrue();
    }

    @Test
    @DisplayName("a condition is examined as well as the projections")
    void theConditionCounts() {
        assertThat(
                        NumericalProvenance.computesApproximateValues(
                                Collections.emptyList(), call(SqlStdOperatorTable.SIN)))
                .isTrue();
    }

    /** One call, with the arity the operator actually takes. */
    private RexNode call(SqlOperator operator) {
        final boolean binary =
                operator == SqlStdOperatorTable.PLUS
                        || operator == SqlStdOperatorTable.MINUS
                        || operator == SqlStdOperatorTable.MULTIPLY
                        || operator == SqlStdOperatorTable.DIVIDE
                        || operator == SqlStdOperatorTable.POWER;
        return binary
                ? rex.makeCall(operator, literal(), literal())
                : rex.makeCall(operator, literal());
    }

    private RexNode literal() {
        return rex.makeLiteral(0.5, typeFactory.createSqlType(SqlTypeName.DOUBLE), false);
    }

    @Test
    @DisplayName("drift forwards to the output columns whose expressions read it")
    void driftForwardsThroughAProjection() {
        // in: [0] = clean, [1] = drifting. Out: pass [1] through, and combine [0] with [1].
        final RexNode clean = rex.makeInputRef(doubleType(), 0);
        final RexNode dirty = rex.makeInputRef(doubleType(), 1);
        final List<RexNode> projection =
                List.of(
                        clean,
                        dirty,
                        rex.makeCall(SqlStdOperatorTable.PLUS, clean, dirty),
                        rex.makeCall(SqlStdOperatorTable.PLUS, clean, clean));

        assertThat(NumericalProvenance.forwardDrift(projection, null, Set.of(1)))
                .withFailMessage(
                        "a column drifts if its expression reads a drifting input, and only then")
                .containsExactlyInAnyOrder(1, 2);
    }

    @Test
    @DisplayName("a projection that computes a transcendental of its own starts the drift")
    void aFreshTranscendentalDrifts() {
        final RexNode clean = rex.makeInputRef(doubleType(), 0);
        final List<RexNode> projection =
                List.of(clean, rex.makeCall(SqlStdOperatorTable.SIN, clean));

        // Nothing is drifting on the way in, so this is the composability case: a chain of
        // projections each adding a little has to accumulate rather than reset at every node.
        assertThat(NumericalProvenance.forwardDrift(projection, null, Set.of()))
                .containsExactly(1);
    }

    @Test
    @DisplayName("a filter reading a drifting column forwards nothing, because it decides rows")
    void aFilterOverDriftIsUnknown() {
        final RexNode clean = rex.makeInputRef(doubleType(), 0);
        final RexNode dirty = rex.makeInputRef(doubleType(), 1);
        final RexNode condition =
                rex.makeCall(
                        SqlStdOperatorTable.GREATER_THAN,
                        dirty,
                        rex.makeLiteral(
                                java.math.BigDecimal.ONE,
                                doubleType(),
                                true));

        // Null rather than an empty set: an empty set would say "nothing downstream drifts", and
        // the rows reaching downstream are already the wrong ones.
        assertThat(NumericalProvenance.forwardDrift(List.of(clean, dirty), condition, Set.of(1)))
                .withFailMessage("a WHERE over a drifting value is the case this analysis refuses")
                .isNull();
    }

    @Test
    @DisplayName("a filter that does not read the drift lets it through")
    void aFilterElsewhereIsFine() {
        final RexNode clean = rex.makeInputRef(doubleType(), 0);
        final RexNode dirty = rex.makeInputRef(doubleType(), 1);
        final RexNode condition =
                rex.makeCall(
                        SqlStdOperatorTable.GREATER_THAN,
                        clean,
                        rex.makeLiteral(java.math.BigDecimal.ONE, doubleType(), true));

        assertThat(NumericalProvenance.forwardDrift(List.of(clean, dirty), condition, Set.of(1)))
                .containsExactly(1);
    }

    private org.apache.calcite.rel.type.RelDataType doubleType() {
        return typeFactory.createSqlType(SqlTypeName.DOUBLE);
    }
}
