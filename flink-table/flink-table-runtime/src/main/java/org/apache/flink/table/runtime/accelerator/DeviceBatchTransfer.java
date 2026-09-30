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
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.device.DeviceBatch;
import org.apache.flink.table.types.logical.RowType;

import java.util.List;
import java.util.function.Consumer;

/**
 * Moves rows between the host and a device, in whichever direction a plan needs.
 *
 * <p>{@link DeviceBatch} deliberately exposes no accessor, so the only way a value gets back to the
 * host is through a provider that knows what the handles mean. This is that route, named and
 * priced, rather than an accessor that would pay for the copy invisibly at whatever call site
 * happened to ask for a value.
 *
 * <p>Implemented by a provider. Flink calls it and times it; it never reads a device pointer.
 */
@PublicEvolving
public interface DeviceBatchTransfer {

    /**
     * Copies a batch to the host and hands each row to {@code out}, in the batch's own row order.
     *
     * <p>The rows handed out are valid only until the next call — a caller that keeps one keeps a
     * reference into a buffer the next batch will reuse, which is the convention the rest of
     * Flink's operators already follow for {@code RowData}.
     *
     * <p>Does not close the batch. Ownership is the caller's, which is the contract
     * {@code DeviceBatch} states, and a transfer that closed its input would take it away from a
     * caller that may still want the batch's metadata afterwards.
     */
    void toHost(DeviceBatch batch, Consumer<RowData> out);

    /**
     * Stages host rows onto the device as one batch, which the caller then owns and closes.
     *
     * <p>This is the expensive direction and the one M9 exists to avoid: measured at 17 ns a field
     * on the arrival path, which is ~4.6 s for 8M rows of 31 columns. It is here because a plan
     * that offloads an operator whose input is not already on the device still has to get it
     * there, and because a transition that only exists in one direction cannot be priced against
     * one that exists in both.
     */
    DeviceBatch toDevice(List<RowData> rows, RowType rowType);

    /**
     * Whether this transfer understands validity masks.
     *
     * <p>Default false, which matches the operand contract every provider here states: dense and
     * non-null. It is asked rather than assumed because the failure it prevents is silent — a
     * masked column read as dense returns whatever the padding happened to be, in the positions
     * that were null, and no row count or checksum of the right length catches it.
     *
     * <p>A transfer that returns false is never handed a masked batch; the transition refuses it
     * instead, and the refusal names the column.
     */
    default boolean supportsNulls() {
        return false;
    }

    /**
     * Whether this transfer can serve {@code rowType} at all.
     *
     * <p>Asked before a plan commits to a transition, so an unsupported type is a decline rather
     * than an exception halfway through a batch.
     */
    boolean supports(RowType rowType);
}
