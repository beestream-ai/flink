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
import org.apache.flink.table.types.logical.RowType;

/**
 * What a provider is told about the task it is being asked to run in.
 *
 * <p>Supplied on the TaskManager, where these facts exist. None of them are knowable to the
 * planner: the row type is, but the memory budget and the classloader are properties of the slot
 * the scheduler happened to choose.
 */
@PublicEvolving
public interface AcceleratorContext {

    /** The row the operator must emit. */
    RowType outputType();

    /**
     * The largest batch the provider may stage, in rows.
     *
     * <p>A ceiling rather than an instruction. It is derived from the slot's memory budget and the
     * row width, so a provider that wants smaller batches may use them; one that wants larger may
     * not.
     */
    int maxBatchSize();

    /** The loader that supplied the provider, for anything it must load reflectively. */
    ClassLoader userCodeClassLoader();
}
