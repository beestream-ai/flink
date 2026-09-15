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

import java.util.Random;

/**
 * Generates random SQL expressions over the vocabulary the accelerator IR admits.
 *
 * <p>SQL text rather than {@code RexNode}s, so the expressions travel the whole path a user's query
 * does: parser, optimiser, IR conversion, execution. A generator that built RexNodes directly would
 * skip the two stages most likely to disagree.
 *
 * <p>Seeded, because a differential failure is worthless if it cannot be reproduced.
 */
final class AccelExpressionGenerator {

    /**
     * Binary operators over doubles.
     *
     * <p>Division is included deliberately: {@code w} starts at zero in the fixture, so it
     * generates infinities, and both paths must agree on those as readily as on finite values.
     */
    private static final String[] BINARY = {"+", "-", "*", "/"};

    /** Unary functions. Domain errors are deliberately allowed: both paths must agree on NaN. */
    private static final String[] UNARY = {
        "ABS", "SQRT", "EXP", "LN", "SIN", "COS", "TAN", "ATAN", "TANH", "FLOOR", "CEIL", "SIGN"
    };

    /**
     * Binary functions.
     *
     * <p>LEAST and GREATEST are back, and are worth generating precisely because they were the
     * first real disagreement this harness found: Flink folds them with {@code Double.compareTo},
     * whose total order puts NaN above everything, while every naive implementation reaches for
     * {@code min}/{@code max}, which propagate it.
     */
    private static final String[] BINARY_FN = {"POWER", "LEAST", "GREATEST"};

    private final Random random;

    AccelExpressionGenerator(long seed) {
        this.random = new Random(seed);
    }

    /**
     * One expression of at most {@code depth} nesting levels.
     *
     * <p>Columns are the NOT NULL doubles of the fixture. Nullable columns are deliberately not
     * referenced: the estimator refuses them as expression inputs, so an expression over one would
     * never reach the accelerator and the comparison would be CPU against CPU.
     */
    String expression(int depth) {
        if (depth <= 0 || random.nextInt(100) < 25) {
            return leaf();
        }
        int choice = random.nextInt(10);
        if (choice < 4) {
            return "("
                    + expression(depth - 1)
                    + " "
                    + BINARY[random.nextInt(BINARY.length)]
                    + " "
                    + expression(depth - 1)
                    + ")";
        }
        if (choice < 8) {
            return UNARY[random.nextInt(UNARY.length)] + "(" + expression(depth - 1) + ")";
        }
        return BINARY_FN[random.nextInt(BINARY_FN.length)]
                + "("
                + expression(depth - 1)
                + ", "
                + expression(depth - 1)
                + ")";
    }

    /** A boolean expression for a WHERE clause. */
    String condition() {
        String[] comparisons = {"<", "<=", ">", ">=", "=", "<>"};
        return expression(2)
                + " "
                + comparisons[random.nextInt(comparisons.length)]
                + " "
                + expression(1);
    }

    private String leaf() {
        int choice = random.nextInt(10);
        if (choice < 4) {
            return "v";
        }
        if (choice < 7) {
            return "w";
        }
        if (choice < 8) {
            // An INT column, to exercise the widening the IR performs on read.
            return "CAST(i AS DOUBLE)";
        }
        // Cast, because an unsuffixed SQL decimal is typed DECIMAL -- and a bare DECIMAL literal
        // as a whole projection makes the expression inexpressible for the entirely correct reason
        // that DECIMAL has no accelerator representation. That is the estimator working, not a bug,
        // but it is not what this generator is trying to exercise.
        double[] constants = {0.0, 1.0, 2.0, 0.5, -1.0, 3.25, 100.0};
        return "CAST(" + constants[random.nextInt(constants.length)] + " AS DOUBLE)";
    }
}
