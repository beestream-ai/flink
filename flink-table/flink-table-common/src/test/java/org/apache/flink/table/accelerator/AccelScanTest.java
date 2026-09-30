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

import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link AccelScan} — M9.5. */
class AccelScanTest {

    private static RowType produced() {
        return RowType.of(new IntType(false), new DoubleType(false));
    }

    private static AccelScan scan() {
        return new AccelScan(
                "parquet",
                Arrays.asList("file:/data/part-0", "file:/data/part-1"),
                new int[] {0, 7},
                produced());
    }

    @Test
    void itIsALeafThatCarriesWhatAProviderNeedsToAnswer() {
        AccelScan scan = scan();
        assertThat(scan.inputs()).isEmpty();
        assertThat(scan.format()).isEqualTo("parquet");
        assertThat(scan.paths()).hasSize(2);
        assertThat(scan.projectedFields()).containsExactly(0, 7);
        assertThat(scan.outputType()).isEqualTo(produced());
    }

    /** It travels in the JobGraph, as every other IR node does. */
    @Test
    void itRoundTripsThroughSerialization() throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(scan());
        }
        try (ObjectInputStream in =
                new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            AccelScan copy = (AccelScan) in.readObject();
            assertThat(copy.format()).isEqualTo("parquet");
            assertThat(copy.paths()).containsExactly("file:/data/part-0", "file:/data/part-1");
            assertThat(copy.projectedFields()).containsExactly(0, 7);
            assertThat(copy.outputType()).isEqualTo(produced());
        }
    }

    @Test
    void aScanOfNothingIsRefused() {
        assertThatThrownBy(
                        () ->
                                new AccelScan(
                                        "parquet",
                                        Collections.emptyList(),
                                        new int[0],
                                        RowType.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at least one path");
    }

    @Test
    void aProjectionThatDoesNotMatchTheProducedRowIsRefused() {
        assertThatThrownBy(
                        () ->
                                new AccelScan(
                                        "parquet",
                                        Arrays.asList("file:/data/part-0"),
                                        new int[] {0},
                                        produced()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("1 entries but the produced row has 2");
    }

    /** A node that travels in a plan must not be alterable by whoever handed it its arguments. */
    @Test
    void itCopiesWhatItIsGivenAndWhatItHandsBack() {
        int[] fields = {0, 7};
        List<String> paths = new java.util.ArrayList<>(Arrays.asList("a", "b"));
        AccelScan scan = new AccelScan("parquet", paths, fields, produced());

        fields[0] = 99;
        paths.set(0, "mutated");
        scan.projectedFields()[1] = 99;

        assertThat(scan.projectedFields()).containsExactly(0, 7);
        assertThat(scan.paths()).containsExactly("a", "b");
        assertThatThrownBy(() -> scan.paths().add("c"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    /** {@link AccelInput} keeps its meaning, so a provider serving only inputs is unaffected. */
    @Test
    void theInputLeafIsUnchanged() {
        AccelInput input = new AccelInput(produced());
        assertThat(input.inputs()).isEmpty();
        assertThat(input.outputType()).isEqualTo(produced());
    }

    @Test
    void theIrVersionAnnouncesTheNewLeaf() {
        assertThat(AccelIrVersion.CURRENT).isEqualTo(AccelIrVersion.V9);
        assertThat(AccelIrVersion.V9).isGreaterThan(AccelIrVersion.V8);
    }
}
