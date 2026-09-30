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

import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.data.device.DeviceBatch;
import org.apache.flink.table.data.device.DeviceBuffer;
import org.apache.flink.table.data.device.DeviceColumn;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link DeviceBatchSerializer} — M9.7. */
class DeviceBatchSerializerTest {

    private static DeviceBatch batch() {
        DeviceBuffer buffer =
                new DeviceBuffer() {
                    @Override
                    public long sizeInBytes() {
                        return 4;
                    }

                    @Override
                    public void release() {}
                };
        return new DeviceBatch(
                RowType.of(new IntType(false)),
                Arrays.asList(new DeviceColumn(new IntType(false), buffer)),
                1);
    }

    /** One owner, one allocation: a copy that produced a second object would double free. */
    @Test
    void copyIsIdentity() {
        DeviceBatchSerializer serializer = new DeviceBatchSerializer();
        DeviceBatch original = batch();
        assertThat(serializer.copy(original)).isSameAs(original);
        assertThat(serializer.copy(original, batch())).isSameAs(original);
    }

    /**
     * The message is the point. Without it this limit surfaces as a Kryo failure inside a shuffle,
     * naming a field of a field rather than the mistake.
     */
    @Test
    void serializingRefusesAndSaysWhy() {
        DeviceBatchSerializer serializer = new DeviceBatchSerializer();
        assertThatThrownBy(() -> serializer.serialize(batch(), new DataOutputSerializer(16)))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("cannot cross a task boundary")
                .hasMessageContaining("transition to rows");
    }

    @Test
    void itIsItsOwnDuplicateAndEqualToAnother() {
        DeviceBatchSerializer serializer = new DeviceBatchSerializer();
        assertThat(serializer.duplicate()).isSameAs(serializer);
        assertThat(serializer).isEqualTo(new DeviceBatchSerializer());
        assertThat(serializer.getLength()).isEqualTo(-1);
    }

    @Test
    void itHasASnapshotEvenThoughABatchIsNeverInState() {
        assertThat(new DeviceBatchSerializer().snapshotConfiguration())
                .isInstanceOf(DeviceBatchSerializer.DeviceBatchSerializerSnapshot.class);
    }
}
