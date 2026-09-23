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
import org.apache.flink.table.accelerator.AccelInput;
import org.apache.flink.table.accelerator.AccelInputRef;
import org.apache.flink.table.accelerator.AccelLiteral;
import org.apache.flink.table.accelerator.AccelNode;
import org.apache.flink.table.accelerator.AccelProject;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import java.util.List;

/**
 * Evaluates accelerator IR on the host, one row at a time.
 *
 * <p>Correctness, not speed: this exists to prove the IR carries enough to execute a subtree, and
 * to give tests an accelerator path that needs no accelerator. A real provider compiles the same IR
 * into something that runs on a device.
 */
final class AccelInterpreter {

    /**
     * When set, TIMES evaluates as addition.
     *
     * <p>A differential harness that has never caught anything is indistinguishable from one that
     * cannot. This exists so a test can inject a wrong answer and assert the harness notices --
     * evidence that green means something.
     */
    static volatile boolean corrupt = false;

    private AccelInterpreter() {}

    /** Whether this subtree is shaped like the one thing the interpreter handles. */
    static boolean canInterpret(AccelNode node) {
        if (!(node instanceof AccelProject)) {
            return false;
        }
        AccelNode input = ((AccelProject) node).inputs().get(0);
        if (input instanceof AccelFilter) {
            input = input.inputs().get(0);
        }
        return input instanceof AccelInput;
    }

    /** The condition of the filter beneath the projection, or null when there is none. */
    static AccelExpression conditionOf(AccelNode project) {
        AccelNode input = project.inputs().get(0);
        return input instanceof AccelFilter ? ((AccelFilter) input).condition() : null;
    }

    static List<AccelExpression> projectionsOf(AccelNode project) {
        return ((AccelProject) project).projections();
    }

    /**
     * Evaluates one expression against one row.
     *
     * <p>Returns null for SQL NULL, which propagates: any null operand makes the result null. The
     * current offload path does not do this — it reads a null {@code DOUBLE} as 0.0 and computes on
     * it — which is review finding A1. Doing it correctly here means a test can tell the two
     * behaviours apart.
     *
     * <p><b>Four functions interrupt that propagation</b>, and they have to, because propagation is
     * what they exist to answer questions about: {@code IS NULL} and {@code IS NOT NULL} report on
     * a null instead of being swallowed by one, and {@code AND} and {@code OR} are three-valued —
     * {@code unknown AND false} is false and {@code unknown OR true} is true, because in each case
     * the known operand settles the answer whatever the unknown one is.
     *
     * <p>Before {@code AcceleratorConformanceIT} they were not: {@code IS NULL} had no case here at
     * all and both connectives propagated, so {@code WHERE a IS NULL} matched nothing and {@code
     * WHERE a > 0 OR b > 0} dropped every row whose {@code a} was absent. Every planner test that
     * compares against this interpreter was checking null behaviour against a wrong oracle.
     */
    static Double eval(AccelExpression expression, RowData row) {
        if (expression instanceof AccelInputRef) {
            AccelInputRef ref = (AccelInputRef) expression;
            if (row.isNullAt(ref.index())) {
                return null;
            }
            return readAsDouble(row, ref.index(), expression);
        }
        if (expression instanceof AccelLiteral) {
            Object value = ((AccelLiteral) expression).value();
            return value == null ? null : ((Number) value).doubleValue();
        }
        AccelCall call = (AccelCall) expression;
        List<AccelExpression> operands = call.operands();
        Double a = eval(operands.get(0), row);
        Double b = operands.size() > 1 ? eval(operands.get(1), row) : null;

        // The three functions that must see a null rather than be short-circuited by one. They are
        // handled before the propagation guard below, because propagation is exactly what they
        // exist to interrupt: an IS NULL that returned null would never be true, and a
        // three-valued OR whose true branch was swallowed would drop rows SQL keeps.
        switch (call.function()) {
            case IS_NULL:
                return bool(a == null);
            case IS_NOT_NULL:
                return bool(a != null);
            case AND:
                // FALSE dominates: unknown AND false is false, whichever side is unknown.
                if (isFalse(a) || isFalse(b)) {
                    return bool(false);
                }
                return a == null || b == null ? null : bool(a != 0.0 && b != 0.0);
            case OR:
                // TRUE dominates, symmetrically.
                if (isTrue(a) || isTrue(b)) {
                    return bool(true);
                }
                return a == null || b == null ? null : bool(a != 0.0 || b != 0.0);
            default:
                break;
        }

        if (a == null || (operands.size() > 1 && b == null)) {
            return null;
        }
        switch (call.function()) {
            case PLUS:
                return a + b;
            case MINUS:
                return a - b;
            case TIMES:
                return corrupt ? a + b : a * b;
            case DIVIDE:
                return a / b;
            case NEGATE:
                return -a;
            case ABS:
                return Math.abs(a);
            case SQRT:
                return Math.sqrt(a);
            case EXP:
                return Math.exp(a);
            case LN:
                return Math.log(a);
            case LOG2:
                return Math.log(a) / Math.log(2);
            case POWER:
                return Math.pow(a, b);
            case SIN:
                return Math.sin(a);
            case COS:
                return Math.cos(a);
            case TAN:
                return Math.tan(a);
            case ASIN:
                return Math.asin(a);
            case ACOS:
                return Math.acos(a);
            case ATAN:
                return Math.atan(a);
            case ATAN2:
                return Math.atan2(a, b);
            case TANH:
                return Math.tanh(a);
            case FLOOR:
                return Math.floor(a);
            case CEIL:
                return Math.ceil(a);
            case SIGN:
                return Math.signum(a);
            case LEAST:
                // Double.compare, not Math.min. Flink folds these with Double.compareTo, whose
                // total order puts NaN above everything -- so NaN loses LEAST and wins GREATEST.
                return Double.compare(a, b) <= 0 ? a : b;
            case GREATEST:
                return Double.compare(a, b) >= 0 ? a : b;
            case CAST:
                return a;
            case EQUALS:
                return totalOrder(operands)
                        ? bool(Double.compare(a, b) == 0)
                        : bool(a.doubleValue() == b.doubleValue());
            case NOT_EQUALS:
                return totalOrder(operands)
                        ? bool(Double.compare(a, b) != 0)
                        : bool(a.doubleValue() != b.doubleValue());
            case LESS_THAN:
                return totalOrder(operands) ? bool(Double.compare(a, b) < 0) : bool(a < b);
            case LESS_OR_EQUAL:
                return totalOrder(operands) ? bool(Double.compare(a, b) <= 0) : bool(a <= b);
            case GREATER_THAN:
                return totalOrder(operands) ? bool(Double.compare(a, b) > 0) : bool(a > b);
            case GREATER_OR_EQUAL:
                return totalOrder(operands) ? bool(Double.compare(a, b) >= 0) : bool(a >= b);
            case NOT:
                return bool(a == 0.0);
            default:
                throw new UnsupportedOperationException(
                        "no host evaluation for " + call.function());
        }
    }

    private static Double bool(boolean value) {
        return value ? 1.0 : 0.0;
    }

    /**
     * Whether this comparison is one Flink evaluates as a total order rather than with IEEE rules.
     *
     * <p>It does so whenever either operand is {@code DECIMAL}, and a bare {@code 0.5} in SQL is a
     * decimal literal — so {@code WHERE val > 0.5} is one of these and {@code WHERE val > CAST(0.5
     * AS DOUBLE)} is not. {@code ScalarOperatorGens.generateComparisonSameType} takes the decimal
     * branch and emits {@code DecimalDataUtils.compare(l, r) op 0}, which for an approximate
     * numeric against a decimal is {@code Double.compare} — a total order in which NaN is greater
     * than everything and equal to itself, and {@code -0.0} is strictly less than {@code 0.0}.
     *
     * <p>Matching it is not a preference. An accelerated predicate that used IEEE semantics here
     * would drop the rows Flink keeps whenever a NaN reached the comparison, which is a changed
     * query result rather than a rounding difference. Found by {@code AcceleratorConformanceIT};
     * the IR carries the literal's declared type, so nothing new had to be added to express it.
     */
    private static boolean totalOrder(List<AccelExpression> operands) {
        for (AccelExpression operand : operands) {
            if (operand.outputType().getTypeRoot() == LogicalTypeRoot.DECIMAL) {
                return true;
            }
        }
        return false;
    }

    /** A known FALSE, as against a null that is merely not TRUE. */
    private static boolean isFalse(Double value) {
        return value != null && value == 0.0;
    }

    /** A known TRUE. NaN counts, which cannot arise: a comparison yields 1.0 or 0.0. */
    private static boolean isTrue(Double value) {
        return value != null && value != 0.0;
    }

    private static double readAsDouble(RowData row, int index, AccelExpression expression) {
        LogicalTypeRoot root = expression.outputType().getTypeRoot();
        switch (root) {
            case INTEGER:
                return row.getInt(index);
            case BIGINT:
                return row.getLong(index);
            case FLOAT:
                return row.getFloat(index);
            case BOOLEAN:
                return row.getBoolean(index) ? 1.0 : 0.0;
            default:
                return row.getDouble(index);
        }
    }
}
