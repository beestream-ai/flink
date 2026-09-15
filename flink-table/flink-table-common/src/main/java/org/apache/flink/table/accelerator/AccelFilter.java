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
import org.apache.flink.table.types.logical.RowType;

import java.util.Collections;
import java.util.List;

/**
 * Keeps the rows for which {@code condition} is true.
 *
 * <p>SQL three-valued logic applies: a condition evaluating to UNKNOWN does not select the row. A
 * provider that cannot represent the distinction between FALSE and UNKNOWN must decline any subtree
 * whose condition can be UNKNOWN, which it can see from the operands' nullability.
 */
@PublicEvolving
public final class AccelFilter extends AccelNode {

    private static final long serialVersionUID = 1L;

    private final AccelExpression condition;
    private final AccelNode input;
    private final RowType outputType;

    public AccelFilter(AccelExpression condition, AccelNode input, RowType outputType) {
        this.condition = condition;
        this.input = input;
        this.outputType = outputType;
    }

    public AccelExpression condition() {
        return condition;
    }

    @Override
    public List<AccelNode> inputs() {
        return Collections.singletonList(input);
    }

    @Override
    public RowType outputType() {
        return outputType;
    }

    @Override
    public String toString() {
        return "Filter(" + condition + ", " + input + ")";
    }
}
