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

import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;

import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link DeviceBatch} — M9.4. */
class DeviceBatchTest {

    /** A buffer that counts its releases, so a double free is visible to a test. */
    private static final class CountingBuffer implements DeviceBuffer {
        private final long bytes;
        private int releases;

        CountingBuffer(long bytes) {
            this.bytes = bytes;
        }

        @Override
        public long sizeInBytes() {
            return bytes;
        }

        @Override
        public void release() {
            releases++;
        }
    }

    private static RowType rowType() {
        return RowType.of(new IntType(false), new DoubleType(false));
    }

    private static DeviceBatch batch(CountingBuffer keys, CountingBuffer values, int rows) {
        List<DeviceColumn> columns =
                Arrays.asList(
                        new DeviceColumn(new IntType(false), keys),
                        new DeviceColumn(new DoubleType(false), values));
        return new DeviceBatch(rowType(), columns, rows);
    }

    @Test
    void metadataIsReadableWithoutTouchingTheData() {
        CountingBuffer keys = new CountingBuffer(8_000_000L * 4);
        CountingBuffer values = new CountingBuffer(8_000_000L * 8);
        try (DeviceBatch batch = batch(keys, values, 8_000_000)) {
            assertThat(batch.rowCount()).isEqualTo(8_000_000);
            assertThat(batch.rowType()).isEqualTo(rowType());
            assertThat(batch.columns()).hasSize(2);
            assertThat(batch.column(0).type()).isEqualTo(new IntType(false));
            assertThat(batch.column(1).mayContainNulls()).isFalse();
            assertThat(batch.sizeInBytes()).isEqualTo(8_000_000L * 12);
        }
    }

    /**
     * The point of the type: nothing on it, or on what it hands back, returns a value. An accessor
     * that did would have to copy the column to the host, which is the cost the type exists to
     * avoid, and it would do it invisibly.
     */
    @Test
    void noAccessorMaterialisesARow() {
        List<String> offenders = new ArrayList<>();
        for (Class<?> type :
                Arrays.asList(DeviceBatch.class, DeviceColumn.class, DeviceBuffer.class)) {
            for (Method method : type.getDeclaredMethods()) {
                if (method.isSynthetic() || method.getParameterCount() > 1) {
                    continue;
                }
                String name = method.getName();
                boolean reads =
                        name.startsWith("getRow")
                                || name.startsWith("getInt")
                                || name.startsWith("getDouble")
                                || name.startsWith("getLong")
                                || name.startsWith("getFloat")
                                || name.startsWith("getBoolean")
                                || name.startsWith("getString")
                                || name.startsWith("getDecimal")
                                || name.equals("isNullAt")
                                || name.equals("toArray")
                                || name.equals("copyToHost");
                if (reads) {
                    offenders.add(type.getSimpleName() + "." + name);
                }
            }
        }
        assertThat(offenders).isEmpty();
    }

    /** A device pointer cannot travel in a JobGraph, so the type must not claim it can. */
    @Test
    void isNotSerializable() {
        assertThat(Serializable.class.isAssignableFrom(DeviceBatch.class)).isFalse();
        assertThat(Serializable.class.isAssignableFrom(DeviceColumn.class)).isFalse();
        assertThat(Serializable.class.isAssignableFrom(DeviceBuffer.class)).isFalse();
    }

    @Test
    void closeReleasesEveryBufferExactlyOnceHoweverOftenItIsCalled() {
        CountingBuffer keys = new CountingBuffer(4);
        CountingBuffer values = new CountingBuffer(8);
        DeviceBatch batch = batch(keys, values, 1);

        batch.close();
        batch.close();
        batch.close();

        assertThat(keys.releases).isEqualTo(1);
        assertThat(values.releases).isEqualTo(1);
        assertThat(batch.isClosed()).isTrue();
        assertThat(batch.columns()).isEmpty();
        assertThatThrownBy(() -> batch.column(0)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void aValidityMaskIsCountedAndReleasedWithItsColumn() {
        CountingBuffer values = new CountingBuffer(80);
        CountingBuffer validity = new CountingBuffer(10);
        DeviceColumn column = new DeviceColumn(new DoubleType(true), values, validity);
        DeviceBatch batch =
                new DeviceBatch(RowType.of(new DoubleType(true)), Arrays.asList(column), 10);

        assertThat(column.mayContainNulls()).isTrue();
        assertThat(batch.sizeInBytes()).isEqualTo(90);

        batch.close();
        assertThat(values.releases).isEqualTo(1);
        assertThat(validity.releases).isEqualTo(1);
    }

    @Test
    void aBatchWhoseColumnsDoNotMatchItsRowTypeIsRefused() {
        CountingBuffer buffer = new CountingBuffer(4);
        assertThatThrownBy(
                        () ->
                                new DeviceBatch(
                                        rowType(),
                                        Arrays.asList(new DeviceColumn(new IntType(false), buffer)),
                                        1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("2 fields but 1 columns");

        assertThatThrownBy(
                        () ->
                                new DeviceBatch(
                                        rowType(),
                                        Arrays.asList(
                                                new DeviceColumn(new DoubleType(false), buffer),
                                                new DeviceColumn(new DoubleType(false), buffer)),
                                        1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("column 0");
    }

    @Test
    void aNegativeRowCountIsRefused() {
        CountingBuffer keys = new CountingBuffer(4);
        CountingBuffer values = new CountingBuffer(8);
        assertThatThrownBy(() -> batch(keys, values, -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("rowCount");
    }

    /**
     * Every buffer is released even when one throws, because a leaked device allocation outlives
     * the job and a failure path is exactly where this happens.
     */
    @Test
    void oneBufferThrowingDoesNotStrandTheOthers() {
        CountingBuffer good = new CountingBuffer(8);
        DeviceBuffer bad =
                new DeviceBuffer() {
                    @Override
                    public long sizeInBytes() {
                        return 4;
                    }

                    @Override
                    public void release() {
                        throw new IllegalStateException("device gone");
                    }
                };
        DeviceBatch batch =
                new DeviceBatch(
                        rowType(),
                        Arrays.asList(
                                new DeviceColumn(new IntType(false), bad),
                                new DeviceColumn(new DoubleType(false), good)),
                        1);

        assertThatThrownBy(batch::close).isInstanceOf(IllegalStateException.class);
        assertThat(good.releases).isEqualTo(1);
    }
}
