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
 * The leaf: rows arriving from upstream, already on the host.
 *
 * <p>Deliberately says nothing about where they came from. Whether the source is a file, a shuffle
 * or another operator is Flink's business and changes nothing a provider decides.
 */
@PublicEvolving
public final class AccelInput extends AccelNode {

    private static final long serialVersionUID = 1L;

    private final RowType rowType;

    public AccelInput(RowType rowType) {
        this.rowType = rowType;
    }

    @Override
    public List<AccelNode> inputs() {
        return Collections.emptyList();
    }

    @Override
    public RowType outputType() {
        return rowType;
    }

    @Override
    public String toString() {
        return "Input" + rowType.getFieldNames();
    }
}
