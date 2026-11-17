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

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A call to a user's scalar function, named rather than described.
 *
 * <h2>Why this is not an {@link AccelCall}</h2>
 *
 * <p>{@link AccelFunction} is a closed set of scalar SQL built-ins, and an expression tree over it
 * can hold only what SQL expressions can hold. A user's {@code eval} body is a method: it can
 * contain a loop over an array, an accumulator, an early return. No number of additional members
 * on that enum would change that, because the limit is the shape of a tree rather than the size of
 * a vocabulary.
 *
 * <p>So this node does not describe the arithmetic at all. It names the method, and leaves deciding
 * whether that method can run on a device to the provider -- which is the same division of labour
 * the rest of this SPI uses: the planner ships what the work <em>is</em> and the machine that has
 * the device decides what it costs.
 *
 * <h2>What travels, and why it is a name</h2>
 *
 * <p>A class name and a method descriptor, not a {@code Method} and not a function instance. The
 * IR travels in the JobGraph and has to serialize; a TaskManager resolves the name against the user
 * code classloader it already has. That also keeps {@code flink-table-common} free of any
 * dependency on how a provider intends to read the body.
 *
 * <h2>What a planner may assume</h2>
 *
 * <p>Nothing about whether it will be accepted. A provider that cannot read bytecode, or reads it
 * and finds an allocation, returns empty, and the ordinary code-generated operator runs -- exactly
 * as for any other subtree a provider declines.
 */
@PublicEvolving
public final class AccelUdfCall extends AccelExpression {

    private static final long serialVersionUID = 1L;

    private final String className;
    private final String methodName;
    private final String methodDescriptor;
    private final List<AccelExpression> operands;

    /**
     * @param className the binary name of the class declaring the method
     * @param methodName normally {@code eval}
     * @param methodDescriptor a JVM method descriptor, which is what distinguishes one overload
     *     of {@code eval} from another -- a user function may have several
     */
    public AccelUdfCall(
            String className,
            String methodName,
            String methodDescriptor,
            List<AccelExpression> operands,
            LogicalType outputType) {
        super(outputType);
        this.className = Objects.requireNonNull(className, "className");
        this.methodName = Objects.requireNonNull(methodName, "methodName");
        this.methodDescriptor = Objects.requireNonNull(methodDescriptor, "methodDescriptor");
        this.operands =
                Collections.unmodifiableList(
                        new java.util.ArrayList<>(Objects.requireNonNull(operands, "operands")));
    }

    public String className() {
        return className;
    }

    public String methodName() {
        return methodName;
    }

    public String methodDescriptor() {
        return methodDescriptor;
    }

    public List<AccelExpression> operands() {
        return operands;
    }

    @Override
    public String toString() {
        return className + "#" + methodName + methodDescriptor + operands;
    }
}
