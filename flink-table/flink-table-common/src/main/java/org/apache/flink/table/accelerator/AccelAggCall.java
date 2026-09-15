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
import org.apache.flink.table.types.logical.LogicalType;

import java.io.Serializable;

/** One aggregate call: a function over one input field, producing one output field. */
@PublicEvolving
public final class AccelAggCall implements Serializable {

    private static final long serialVersionUID = 1L;

    /** {@link #inputField()} for a call that reads no field, i.e. COUNT(*). */
    public static final int NO_INPUT_FIELD = -1;

    private final AccelAggFunction function;
    private final int inputField;
    private final LogicalType outputType;

    public AccelAggCall(AccelAggFunction function, int inputField, LogicalType outputType) {
        this.function = function;
        this.inputField = inputField;
        this.outputType = outputType;
    }

    public AccelAggFunction function() {
        return function;
    }

    public int inputField() {
        return inputField;
    }

    public LogicalType outputType() {
        return outputType;
    }

    @Override
    public String toString() {
        return function + "(" + (inputField == NO_INPUT_FIELD ? "*" : "$" + inputField) + ")";
    }
}
