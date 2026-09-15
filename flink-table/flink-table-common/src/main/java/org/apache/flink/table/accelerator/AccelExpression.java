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

/**
 * One node of an accelerator IR expression tree.
 *
 * <p>The type system is Flink's own {@link LogicalType} rather than a parallel invention: it is
 * already independent of the optimizer's own expression representation, already
 * serializable, and already carries {@link
 * LogicalType#isNullable()} — which is precisely what a provider needs and what the current
 * offload path does not look at.
 *
 * <p>Serializable because the IR travels in the JobGraph: the planner builds it on the client and
 * a TaskManager hands it to a provider.
 *
 * <p>The constructor is package-private. That is this module's stand-in for {@code sealed}, which
 * Java 11 does not have: the hierarchy is closed to implementors outside this package.
 */
@PublicEvolving
public abstract class AccelExpression implements Serializable {

    private static final long serialVersionUID = 1L;

    private final LogicalType outputType;

    AccelExpression(LogicalType outputType) {
        this.outputType = outputType;
    }

    /** The type this expression produces, nullability included. */
    public LogicalType outputType() {
        return outputType;
    }
}
