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
 * Aggregate functions an accelerator IR can express.
 *
 * <p>Two, in V1. Both have a contraction a linear-algebra library can serve directly, which is the
 * property that made them worth admitting first. MIN and MAX have no such entry point, and
 * DISTINCT and the order statistics need sorting or hashing.
 */
@PublicEvolving
public enum AccelAggFunction {
    /** Over no rows this is NULL, not zero. */
    SUM,

    /** COUNT(*) — free: the operator already knows how many rows it staged. */
    COUNT_STAR
}
