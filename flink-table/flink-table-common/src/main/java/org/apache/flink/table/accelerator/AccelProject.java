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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** One output row per input row: each field is an expression over the input. */
@PublicEvolving
public final class AccelProject extends AccelNode {

    private static final long serialVersionUID = 1L;

    private final List<AccelExpression> projections;
    private final AccelNode input;
    private final RowType outputType;

    public AccelProject(
            List<AccelExpression> projections, AccelNode input, RowType outputType) {
        this.projections = Collections.unmodifiableList(new ArrayList<>(projections));
        this.input = input;
        this.outputType = outputType;
    }

    public List<AccelExpression> projections() {
        return projections;
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
        return "Project" + projections + "(" + input + ")";
    }
}
