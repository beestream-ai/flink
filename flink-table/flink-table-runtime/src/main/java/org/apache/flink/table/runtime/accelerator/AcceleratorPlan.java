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

package org.apache.flink.table.runtime.accelerator;

import org.apache.flink.annotation.PublicEvolving;

import java.io.Serializable;

/**
 * A provider's answer that it will serve a subtree, and what it expects that to cost.
 *
 * <p>The payload is deliberately opaque. Flink does not know, and must not need to know, whether a
 * provider compiled a kernel, selected a library entry point, or matched a pattern it has seen
 * before -- only what the choice is expected to cost. That opacity is what allows a provider to
 * answer "I will serve this whole projection-and-sum with one library call" without Flink ever
 * acquiring a concept of a library.
 */
@PublicEvolving
public interface AcceleratorPlan extends Serializable {

    /** What the provider claims this plan costs. Never a verdict -- Flink decides. */
    AcceleratorCost cost();

    /** Opaque to Flink. Handed back to the same provider in {@code createOperator}. */
    Serializable payload();
}
