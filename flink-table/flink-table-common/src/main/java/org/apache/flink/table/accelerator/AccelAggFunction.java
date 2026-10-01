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
 * property that made them worth admitting first. DISTINCT and the order statistics need sorting or
 * hashing and are still outside this.
 *
 * <p>{@link #MAX} joined them in V11, and the reason it was excluded is worth recording because it
 * turned out to be the wrong test. It was excluded for having "no entry point in a linear-algebra
 * library", which is true of the aggregate read on its own and false of the shape it appears in: a
 * nearest-neighbour query is {@code MAX} over an inner product, the inner product is a GEMM, and
 * the maximum is the GEMM's epilogue. The library serves the expensive half and a kernel reduces
 * what it produced. What makes an aggregate admissible is whether it is associative and
 * commutative over a partition, which {@code MAX} is; whether a library computes it directly is a
 * question about one provider.
 */
@PublicEvolving
public enum AccelAggFunction {
    /** Over no rows this is NULL, not zero. */
    SUM,

    /** COUNT(*) — free: the operator already knows how many rows it staged. */
    COUNT_STAR,

    /**
     * Over no rows this is NULL, and over a partition it combines by taking the larger — so a
     * partial per batch is a valid answer for a {@code Final} aggregate above to combine.
     */
    MAX
}
