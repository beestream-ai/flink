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

package org.apache.flink.table.planner.plan.gpu;

import org.apache.flink.table.accelerator.AccelWorkProfile;

import javax.annotation.Nullable;

import java.util.Objects;

/**
 * What the planner can say about a subtree: either the work it performs, or why it cannot be
 * represented for an accelerator at all.
 *
 * <p>Note what is absent. There is no verdict here, and no threshold. The planner counts and
 * describes; whether the work is worth moving depends on a device it cannot see, so that judgement
 * belongs to the TaskManager that will run the task. This class used to carry a weighted scalar
 * compared against a floor calibrated on one particular card — a hardware constant living inside
 * Flink, which could not be right for every accelerator and could not be recalibrated without
 * changing Flink.
 *
 * <p>The two are one type because a single walk of the expression tree decides both. Splitting them
 * into separate visitors would let the eligibility rules and the counting drift apart.
 *
 * <p>Ineligible dominates: adding anything to an ineligible cost keeps the first rejection reason,
 * so the message names the expression that actually caused it rather than the last one visited.
 */
public final class RowCost {

    /** Eligible, no work. Identity for {@link #plus}. */
    public static final RowCost ZERO = new RowCost(null, null);

    private final @Nullable AccelWorkProfile work;
    private final @Nullable String rejection;

    private RowCost(@Nullable AccelWorkProfile work, @Nullable String rejection) {
        this.work = work;
        this.rejection = rejection;
    }

    /** Eligible, performing the given work per row. */
    public static RowCost of(AccelWorkProfile work) {
        return new RowCost(Objects.requireNonNull(work), null);
    }

    /**
     * @param reason what about the expression cannot be represented; surfaced in EXPLAIN
     */
    public static RowCost ineligible(String reason) {
        return new RowCost(null, Objects.requireNonNull(reason));
    }

    public boolean isEligible() {
        return rejection == null;
    }

    /**
     * The work this subtree performs per input row, counted per operation.
     *
     * <p>Null for {@link #ZERO} and for an ineligible cost. A provider weighs these counts with its
     * own hardware's costs; Flink does not weigh them at all.
     */
    public @Nullable AccelWorkProfile work() {
        return work;
    }

    /** Total operations per row, or 0 when there is no work. For logs and EXPLAIN. */
    public int opsPerRow() {
        return work == null ? 0 : work.totalOpsPerRow();
    }

    /** Null when eligible. */
    public @Nullable String rejection() {
        return rejection;
    }

    public RowCost plus(RowCost other) {
        if (!isEligible()) {
            return this;
        }
        if (!other.isEligible()) {
            return other;
        }
        if (work == null) {
            return other;
        }
        if (other.work == null) {
            return this;
        }
        return of(work.plus(other.work));
    }

    @Override
    public String toString() {
        return isEligible()
                ? "RowCost[" + (work == null ? "no work" : work.toString()) + "]"
                : "RowCost[ineligible: " + rejection + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof RowCost)) {
            return false;
        }
        RowCost other = (RowCost) o;
        return Objects.equals(other.work, work) && Objects.equals(other.rejection, rejection);
    }

    @Override
    public int hashCode() {
        return Objects.hash(work, rejection);
    }
}
