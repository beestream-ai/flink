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

/**
 * Version of the accelerator IR a provider was compiled against.
 *
 * <p>Java 11 has no {@code sealed}, so the node hierarchy is closed with package-private
 * constructors instead: nothing outside this package can add a node kind. That stops an outside
 * implementor inventing a node, but it does not stop a provider built against an older IR meeting a
 * newer one at run time. This constant is how that mismatch is detected rather than discovered.
 */
@PublicEvolving
public final class AccelIrVersion {

    /** Input, Project, Filter, and an ungrouped Aggregate. */
    public static final int V1 = 1;

    /**
     * Adds {@link AccelFunction#IS_NULL} and {@link AccelFunction#IS_NOT_NULL}, and stops refusing
     * a nullable value before it reaches a provider.
     *
     * <p>A provider built against {@link #V1} has never been asked to serve a value that might be
     * absent — Flink refused those itself — so handing it one would be handing it a case it has no
     * reason to have considered. Hence the bump: this is exactly the mismatch this constant exists
     * to catch.
     *
     * <p>It works because Java inlines a compile-time constant. A provider that writes {@code
     * AccelIrVersion.CURRENT} compiles the value it was built against into its own class file, so a
     * jar built against V1 still answers 1 when it meets a Flink that emits 2, and is rejected.
     */
    public static final int V2 = 2;

    /** The version this Flink emits. */
    public static final int CURRENT = V2;

    private AccelIrVersion() {}
}
