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
import org.apache.flink.table.accelerator.AccelFilter;
import org.apache.flink.table.accelerator.AccelFunction;
import org.apache.flink.table.accelerator.AccelInput;
import org.apache.flink.table.accelerator.AccelInputRef;
import org.apache.flink.table.accelerator.AccelLiteral;
import org.apache.flink.table.accelerator.AccelNode;
import org.apache.flink.table.accelerator.AccelProject;
import org.apache.flink.table.accelerator.AccelWorkProfile;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Converts the planner's {@code RexNode} trees into accelerator IR.
 *
 * <p>This class is the only place Calcite and the IR meet, and it lives in the planner for that
 * reason: {@code flink-table-common} holds the IR and must not learn about the optimizer.
 *
 * <h2>The mapping is the type check</h2>
 *
 * <p>An operator becomes an {@link AccelFunction} only if it appears in one of three maps, keyed on
 * {@link SqlKind}, on the identity of a Calcite operator, or on the identity of a Flink function
 * definition — never on a name string. A catalog function called {@code SQRT} is a different object
 * from {@code SqlStdOperatorTable.SQRT}, so it simply has no mapping and the conversion fails,
 * rather than being rendered as the builtin and quietly returning a different answer.
 *
 * <p>That used to take an explicit guard — recognise the impostor, then trust the name. Keying on
 * identity removes the impostor as a category: there is nothing to recognise, because only the
 * built-in objects are in the maps.
 *
 * <p>Failure to convert is an ordinary outcome: it means this subtree is not offloadable, which is
 * what an empty {@link Optional} says.
 */
public final class AccelIrBuilder {

    /** Operators recognised by kind. */
    private static final Map<SqlKind, AccelFunction> BY_KIND = new HashMap<>();

    /**
     * Calcite operators, keyed on the operator object itself.
     *
     * <p>An {@link IdentityHashMap} states the intent; {@link org.apache.calcite.sql.SqlOperator}
     * does not override {@code equals}, so a {@code HashMap} would behave the same and say less.
     */
    private static final Map<SqlOperator, AccelFunction> BY_CALCITE_OPERATOR =
            new IdentityHashMap<>();

    /** Flink function definitions, keyed on the definition object itself. */
    private static final Map<FunctionDefinition, AccelFunction> BY_DEFINITION =
            new IdentityHashMap<>();

    static {
        BY_KIND.put(SqlKind.PLUS, AccelFunction.PLUS);
        BY_KIND.put(SqlKind.MINUS, AccelFunction.MINUS);
        BY_KIND.put(SqlKind.TIMES, AccelFunction.TIMES);
        BY_KIND.put(SqlKind.DIVIDE, AccelFunction.DIVIDE);
        BY_KIND.put(SqlKind.MINUS_PREFIX, AccelFunction.NEGATE);
        BY_KIND.put(SqlKind.EQUALS, AccelFunction.EQUALS);
        BY_KIND.put(SqlKind.NOT_EQUALS, AccelFunction.NOT_EQUALS);
        BY_KIND.put(SqlKind.LESS_THAN, AccelFunction.LESS_THAN);
        BY_KIND.put(SqlKind.LESS_THAN_OR_EQUAL, AccelFunction.LESS_OR_EQUAL);
        BY_KIND.put(SqlKind.GREATER_THAN, AccelFunction.GREATER_THAN);
        BY_KIND.put(SqlKind.GREATER_THAN_OR_EQUAL, AccelFunction.GREATER_OR_EQUAL);
        BY_KIND.put(SqlKind.AND, AccelFunction.AND);
        BY_KIND.put(SqlKind.OR, AccelFunction.OR);
        BY_KIND.put(SqlKind.NOT, AccelFunction.NOT);
        BY_KIND.put(SqlKind.CAST, AccelFunction.CAST);
        BY_KIND.put(SqlKind.FLOOR, AccelFunction.FLOOR);
        BY_KIND.put(SqlKind.CEIL, AccelFunction.CEIL);
        BY_KIND.put(SqlKind.LEAST, AccelFunction.LEAST);
        BY_KIND.put(SqlKind.GREATEST, AccelFunction.GREATEST);

        // Identity, not name. Every entry below is the very operator object the planner puts in
        // the RexCall, so a match means "this is that function", not "this is called that".
        //
        // Which table an operator comes from is not guessable and was measured rather than
        // assumed: most arrive as Calcite's own, LEAST and GREATEST as Flink function definitions
        // behind a bridging operator, and LOG2 and TANH from Flink's Calcite-side table, which is
        // neither. Getting one wrong costs nothing visible -- the function is simply never
        // offloaded again -- which is exactly how the earlier bug survived, so
        // AccelIrBuilderTest walks this map and checks every entry still resolves.
        BY_CALCITE_OPERATOR.put(SqlStdOperatorTable.ABS, AccelFunction.ABS);
        BY_CALCITE_OPERATOR.put(SqlStdOperatorTable.SQRT, AccelFunction.SQRT);
        BY_CALCITE_OPERATOR.put(SqlStdOperatorTable.EXP, AccelFunction.EXP);
        BY_CALCITE_OPERATOR.put(SqlStdOperatorTable.LN, AccelFunction.LN);
        BY_CALCITE_OPERATOR.put(SqlStdOperatorTable.POWER, AccelFunction.POWER);
        BY_CALCITE_OPERATOR.put(SqlStdOperatorTable.SIN, AccelFunction.SIN);
        BY_CALCITE_OPERATOR.put(SqlStdOperatorTable.COS, AccelFunction.COS);
        BY_CALCITE_OPERATOR.put(SqlStdOperatorTable.TAN, AccelFunction.TAN);
        BY_CALCITE_OPERATOR.put(SqlStdOperatorTable.ASIN, AccelFunction.ASIN);
        BY_CALCITE_OPERATOR.put(SqlStdOperatorTable.ACOS, AccelFunction.ACOS);
        BY_CALCITE_OPERATOR.put(SqlStdOperatorTable.ATAN, AccelFunction.ATAN);
        BY_CALCITE_OPERATOR.put(SqlStdOperatorTable.ATAN2, AccelFunction.ATAN2);
        BY_CALCITE_OPERATOR.put(SqlStdOperatorTable.SIGN, AccelFunction.SIGN);

        // Flink's own, on the Calcite side: no SqlStdOperatorTable entry exists for either.
        BY_CALCITE_OPERATOR.put(FlinkSqlOperatorTable.LOG2, AccelFunction.LOG2);
        BY_CALCITE_OPERATOR.put(FlinkSqlOperatorTable.TANH, AccelFunction.TANH);

        // And Flink's own, on the Table API side. These arrive as a BridgingSqlFunction wrapping
        // the definition, so the identity that means anything is the definition's, not the
        // operator's -- a bridging operator is constructed per call site.
        BY_DEFINITION.put(BuiltInFunctionDefinitions.LEAST, AccelFunction.LEAST);
        BY_DEFINITION.put(BuiltInFunctionDefinitions.GREATEST, AccelFunction.GREATEST);
    }

    private AccelIrBuilder() {}

    /** Converts one expression, or empty when any part of it has no IR representation. */
    public static Optional<AccelExpression> convert(RexNode node) {
        if (node instanceof RexInputRef) {
            RexInputRef ref = (RexInputRef) node;
            return logicalType(node).map(type -> new AccelInputRef(ref.getIndex(), type));
        }
        if (node instanceof RexLiteral) {
            return convertLiteral((RexLiteral) node);
        }
        if (!(node instanceof RexCall)) {
            return Optional.empty();
        }
        RexCall call = (RexCall) node;
        AccelFunction function = functionOf(call);
        if (function == null) {
            return Optional.empty();
        }
        List<AccelExpression> operands = new ArrayList<>(call.getOperands().size());
        for (RexNode operand : call.getOperands()) {
            Optional<AccelExpression> converted = convert(operand);
            if (!converted.isPresent()) {
                return Optional.empty();
            }
            operands.add(converted.get());
        }
        return logicalType(call).map(type -> new AccelCall(function, operands, type));
    }

    /**
     * Builds the subtree for one Calc: a projection, optionally over a filter, over the input.
     *
     * <p>This is what a provider is handed. It is a subtree rather than a bare expression list
     * because a provider must be able to recognise shapes spanning operators — and because the
     * filter's position relative to the projection is information it needs.
     *
     * @return empty when any expression has no representation, which is an ordinary outcome
     */
    public static Optional<AccelNode> buildCalc(
            List<RexNode> projection,
            @Nullable RexNode condition,
            RowType inputType,
            RowType outputType) {

        AccelNode input = new AccelInput(inputType);
        if (condition != null) {
            Optional<AccelExpression> filter = convert(condition);
            if (!filter.isPresent()) {
                return Optional.empty();
            }
            input = new AccelFilter(filter.get(), input, inputType);
        }
        List<AccelExpression> projections = new ArrayList<>(projection.size());
        for (RexNode expression : projection) {
            Optional<AccelExpression> converted = convert(expression);
            if (!converted.isPresent()) {
                return Optional.empty();
            }
            projections.add(converted.get());
        }
        return Optional.of(new AccelProject(projections, input, outputType));
    }

    /**
     * Whether this call has an IR representation at all.
     *
     * <p>The single admission list. Before this existed the vocabulary was stated three times -- in
     * the cost estimator's weight table, in the kernel generator's switch, and implicitly in
     * whatever the runtime could execute -- and nothing kept them in step.
     */
    public static boolean supports(RexCall call) {
        return functionOf(call) != null;
    }

    /** Counts the work a set of expressions performs on each input row. */
    public static AccelWorkProfile profile(
            List<AccelExpression> expressions,
            int inputBytesPerRow,
            int outputBytesPerRow,
            long estimatedRows) {
        int[] counts = new int[AccelFunction.values().length];
        for (AccelExpression expression : expressions) {
            count(expression, counts);
        }
        return new AccelWorkProfile(counts, inputBytesPerRow, outputBytesPerRow, estimatedRows);
    }

    private static void count(AccelExpression expression, int[] counts) {
        if (expression instanceof AccelCall) {
            AccelCall call = (AccelCall) expression;
            counts[call.function().ordinal()]++;
            for (AccelExpression operand : call.operands()) {
                count(operand, counts);
            }
        }
    }

    private static @Nullable AccelFunction functionOf(RexCall call) {
        AccelFunction byKind = BY_KIND.get(call.getKind());
        if (byKind != null) {
            return byKind;
        }
        SqlOperator operator = call.getOperator();
        if (operator instanceof BridgingSqlFunction) {
            // The bridging operator is built per call site, so its identity means nothing; the
            // definition behind it is the stable object. A user's catalog function reaches here
            // the same way with a definition that is in no map, and is refused by not matching --
            // it no longer has to be recognised as an impostor first.
            return BY_DEFINITION.get(((BridgingSqlFunction) operator).getDefinition());
        }
        return BY_CALCITE_OPERATOR.get(operator);
    }

    private static Optional<AccelExpression> convertLiteral(RexLiteral literal) {
        Optional<LogicalType> type = logicalType(literal);
        if (!type.isPresent()) {
            return Optional.empty();
        }
        Object value = literal.getValue3();
        Serializable box;
        if (value == null) {
            box = null;
        } else if (value instanceof BigDecimal) {
            box = ((BigDecimal) value).doubleValue();
        } else if (value instanceof Number) {
            box = ((Number) value).doubleValue();
        } else if (value instanceof Boolean) {
            box = (Boolean) value;
        } else {
            return Optional.empty();
        }
        return Optional.of(new AccelLiteral(box, type.get()));
    }

    private static Optional<LogicalType> logicalType(RexNode node) {
        try {
            return Optional.of(FlinkTypeFactory.toLogicalType(node.getType()));
        } catch (RuntimeException unsupported) {
            // A type the planner can name but Flink's logical type system cannot carry.
            return Optional.empty();
        }
    }
}
