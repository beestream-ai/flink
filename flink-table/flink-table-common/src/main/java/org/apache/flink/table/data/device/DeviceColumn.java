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
import org.apache.flink.table.types.logical.LogicalType;

import javax.annotation.Nullable;

import java.util.Objects;
import java.util.Optional;

/**
 * One column of a {@link DeviceBatch}: its logical type, and an opaque handle to where its values
 * live on the device.
 *
 * <p>The type is Flink's, the values are the provider's. That split is the whole point — a
 * consumer can decide what to do with a column, and check that it is the column it expected,
 * without the values crossing the interconnect.
 */
@PublicEvolving
public final class DeviceColumn {

    private final LogicalType type;

    private final DeviceBuffer values;

    /**
     * Null when every value is present.
     *
     * <p>Left unset by every producer today: the operand contract the providers state is dense and
     * non-null. It is a field rather than a later addition so that admitting nulls does not change
     * this type's shape.
     */
    @Nullable private final DeviceBuffer validity;

    public DeviceColumn(LogicalType type, DeviceBuffer values) {
        this(type, values, null);
    }

    public DeviceColumn(LogicalType type, DeviceBuffer values, @Nullable DeviceBuffer validity) {
        this.type = Objects.requireNonNull(type, "type");
        this.values = Objects.requireNonNull(values, "values");
        this.validity = validity;
    }

    public LogicalType type() {
        return type;
    }

    /** The values, as a handle. There is no accessor that reads one. */
    public DeviceBuffer values() {
        return values;
    }

    /** The validity mask, when the column admits nulls. Empty means every value is present. */
    public Optional<DeviceBuffer> validity() {
        return Optional.ofNullable(validity);
    }

    /** Whether this column can contain a null, which decides whether a consumer may ignore it. */
    public boolean mayContainNulls() {
        return validity != null;
    }

    @Override
    public String toString() {
        return type + (validity == null ? "" : " NULLABLE") + "@device";
    }
}
