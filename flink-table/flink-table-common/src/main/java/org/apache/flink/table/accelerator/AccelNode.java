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

import java.io.Serializable;
import java.util.List;

/**
 * One node of an accelerator IR plan subtree.
 *
 * <p>The unit handed to a provider is a <em>subtree</em>, never a single operator. That is what
 * lets a provider answer "I will serve this whole projection-and-sum with one library call" as an
 * ordinary answer rather than as a special case bolted on later.
 *
 * <p>The constructor is package-private, which is this module's stand-in for {@code sealed}:
 * nothing outside this package can add a node kind. Version skew between a provider and a newer
 * Flink is handled by {@link AccelIrVersion} instead.
 */
@PublicEvolving
public abstract class AccelNode implements Serializable {

    private static final long serialVersionUID = 1L;

    AccelNode() {}

    /** Inputs, in order. Empty for {@link AccelInput}. */
    public abstract List<AccelNode> inputs();

    /** The row this node produces. */
    public abstract RowType outputType();
}
