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

package org.apache.flink.table.data.device;

import org.apache.flink.annotation.PublicEvolving;

/**
 * A region of memory on an accelerator, named but not described.
 *
 * <p>Flink deliberately cannot read this. A device pointer means something only to the runtime
 * that allocated it, and any accessor Flink offered here would either be a lie or would force a
 * copy back to the host — which is the cost {@link DeviceBatch} exists to avoid. So this interface
 * carries no data accessor at all: it says how large the region is, and it releases it.
 *
 * <p>Implementations live in a provider, never here. Nothing in this package names a vendor.
 */
@PublicEvolving
public interface DeviceBuffer {

    /** How many bytes the region occupies on the device. */
    long sizeInBytes();

    /**
     * Releases the region.
     *
     * <p>Must tolerate being called more than once and free the region exactly once — a second
     * free is a crash rather than a leak, and the batch this buffer belongs to may be closed by a
     * failure path that cannot know whether the normal path ran first.
     */
    void release();
}
