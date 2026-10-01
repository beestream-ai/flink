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

package org.apache.flink.table.accelerator;

import org.apache.flink.annotation.PublicEvolving;

/**
 * The closed vocabulary of operations an accelerator IR expression can perform.
 *
 * <p>Closed on purpose. A planner builds the IR by mapping a known built-in function definition
 * onto a constant here; a catalog function has no mapping and therefore <em>cannot be
 * represented</em>. That makes "a user function named SQRT is mistaken for the builtin" a
 * compile-time impossibility rather than a check someone has to remember to write.
 *
 * <p>Adding a constant is a compatible change for Flink and an incompatible one for providers,
 * which is why {@link AccelIrVersion} exists.
 */
@PublicEvolving
public enum AccelFunction {
    PLUS,
    MINUS,
    TIMES,
    DIVIDE,
    /**
     * Integer division: truncated toward zero, as SQL means it on an integral type.
     *
     * <p>Distinct from {@link #DIVIDE} because a kernel computing in double would answer 3.5 where
     * SQL answers 3, and the difference is invisible until a grouping key is wrong.
     */
    INT_DIVIDE,
    /** Remainder, truncated toward zero, matching SQL's {@code MOD} on an integral type. */
    MOD,
    NEGATE,

    EQUALS,
    NOT_EQUALS,
    LESS_THAN,
    LESS_OR_EQUAL,
    GREATER_THAN,
    GREATER_OR_EQUAL,

    AND,
    OR,
    NOT,

    ABS,
    SQRT,
    EXP,
    LN,
    LOG2,
    POWER,

    SIN,
    COS,
    TAN,
    ASIN,
    ACOS,
    ATAN,
    ATAN2,
    TANH,

    IS_NULL,
    IS_NOT_NULL,

    FLOOR,
    CEIL,
    SIGN,
    LEAST,
    GREATEST,

    /**
     * {@code REGEXP(string, pattern)} — true when the pattern matches anywhere in the string.
     *
     * <p>The first function here whose operand is a character string rather than a number, and the
     * first whose second operand must be a literal: a pattern is compiled, and compiling one per
     * row is not a thing any implementation does. A provider that cannot compile the pattern, or
     * that implements a dialect the query's engine does not, must decline the expression rather
     * than match approximately — Flink evaluates this with {@code java.util.regex}, and a provider
     * whose regex differs in lookaround or Unicode classes would return a different answer, not a
     * faster one.
     */
    REGEXP,
    CAST
}
