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

package org.apache.flink.table.runtime.typeutils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.table.data.device.DeviceBatch;

import java.io.IOException;

/**
 * The serializer for a record type that cannot be serialized, and says so.
 *
 * <p>A {@link DeviceBatch} is a set of handles into another processor's memory. It is meaningful
 * in the process that allocated it and nowhere else, so it can travel between two operators of one
 * chain — where Flink passes records by reference — and across nothing else. Without this class
 * that limit still holds, but it is discovered as a Kryo failure deep inside a shuffle, naming a
 * field of a field and not the actual mistake.
 *
 * <p>So: {@link #copy} hands back the same instance, and {@link #serialize} refuses with the
 * reason. Copying is identity on purpose — a device batch owns its memory, and a copy that handed
 * back a second object holding the same handles would give two owners one allocation and a double
 * free at whichever closed second.
 */
@Internal
public final class DeviceBatchSerializer extends TypeSerializer<DeviceBatch> {

    private static final long serialVersionUID = 1L;

    private static final String WHY =
            "A DeviceBatch holds device memory and cannot cross a task boundary. It is valid only "
                    + "inside one operator chain, where Flink passes records by reference. A plan "
                    + "that puts a shuffle, a rebalance or a network exchange between a device "
                    + "source and its consumer must insert a transition to rows first.";

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TypeSerializer<DeviceBatch> duplicate() {
        return this;
    }

    @Override
    public DeviceBatch createInstance() {
        throw new UnsupportedOperationException(WHY);
    }

    /** Identity. A device batch has one owner, and a copy would give its memory two. */
    @Override
    public DeviceBatch copy(DeviceBatch from) {
        return from;
    }

    @Override
    public DeviceBatch copy(DeviceBatch from, DeviceBatch reuse) {
        return from;
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(DeviceBatch record, DataOutputView target) throws IOException {
        throw new UnsupportedOperationException(WHY);
    }

    @Override
    public DeviceBatch deserialize(DataInputView source) throws IOException {
        throw new UnsupportedOperationException(WHY);
    }

    @Override
    public DeviceBatch deserialize(DeviceBatch reuse, DataInputView source) throws IOException {
        throw new UnsupportedOperationException(WHY);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        throw new UnsupportedOperationException(WHY);
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof DeviceBatchSerializer;
    }

    @Override
    public int hashCode() {
        return DeviceBatchSerializer.class.hashCode();
    }

    @Override
    public TypeSerializerSnapshot<DeviceBatch> snapshotConfiguration() {
        return new DeviceBatchSerializerSnapshot();
    }

    /** Present because the interface requires one; a device batch is never in state. */
    public static final class DeviceBatchSerializerSnapshot
            extends SimpleTypeSerializerSnapshot<DeviceBatch> {
        public DeviceBatchSerializerSnapshot() {
            super(DeviceBatchSerializer::new);
        }
    }
}
