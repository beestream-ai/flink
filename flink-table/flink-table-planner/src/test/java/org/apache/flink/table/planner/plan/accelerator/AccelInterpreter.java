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
 * to give tests an accelerator path that needs no accelerator. A real provider compiles the same
 * IR into something that runs on a device.
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
                return bool(a.doubleValue() == b.doubleValue());
            case NOT_EQUALS:
                return bool(a.doubleValue() != b.doubleValue());
            case LESS_THAN:
                return bool(a < b);
            case LESS_OR_EQUAL:
                return bool(a <= b);
            case GREATER_THAN:
                return bool(a > b);
            case GREATER_OR_EQUAL:
                return bool(a >= b);
            case AND:
                return bool(a != 0.0 && b != 0.0);
            case OR:
                return bool(a != 0.0 || b != 0.0);
            case NOT:
                return bool(a == 0.0);
            default:
                throw new UnsupportedOperationException("no host evaluation for " + call.function());
        }
    }

    private static Double bool(boolean value) {
        return value ? 1.0 : 0.0;
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
