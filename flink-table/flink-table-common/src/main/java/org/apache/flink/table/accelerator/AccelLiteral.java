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

import javax.annotation.Nullable;

import java.io.Serializable;

/**
 * A constant folded into the expression.
 *
 * <p>The value is held as a {@link Serializable} box rather than a primitive so one class covers
 * every admitted type. A null value is legal and means the SQL literal {@code NULL}.
 */
@PublicEvolving
public final class AccelLiteral extends AccelExpression {

    private static final long serialVersionUID = 1L;

    private final @Nullable Serializable value;

    public AccelLiteral(@Nullable Serializable value, LogicalType outputType) {
        super(outputType);
        this.value = value;
    }

    public @Nullable Serializable value() {
        return value;
    }

    @Override
    public String toString() {
        return String.valueOf(value);
    }
}
