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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecCalc;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecCalc;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecCalc;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.RowType;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Offload is a batch-only capability, and since M2.6 it is one by construction.
 *
 * <p>It was always batch-only in effect: {@code GpuOffloadProcessor} is added in {@code
 * BatchPlanner} and nowhere else, so no streaming node was ever selected. But the machinery lived
 * in {@link CommonExecCalc}, which {@link StreamExecCalc} extends, so what kept streaming away from
 * it was a line in a planner's constructor. A registration is a thing somebody changes by accident,
 * and the streaming operator is precisely the one with a {@code RowKind} this path does not
 * preserve — see M2.7.
 *
 * <p>These are reflection assertions, which is unusual and deliberate. What is under test is the
 * <em>shape</em> of the class hierarchy rather than any behaviour: that the substitution hook is
 * overridden in the batch node and not in the streaming one. A behavioural test could only show
 * that streaming does not offload today, which was already true and was never the point.
 */
class GpuBatchOnlyTest {

    private static final String HOOK = "substituteOperator";

    @Test
    @DisplayName("the batch node overrides the operator-substitution hook")
    void batchOverridesTheHook() {
        assertThat(declares(BatchExecCalc.class, HOOK))
                .as("BatchExecCalc must override %s; without it nothing offloads at all", HOOK)
                .isTrue();
    }

    @Test
    @DisplayName("the streaming node does not, so it runs the generated operator and nothing else")
    void streamDoesNotOverrideTheHook() {
        assertThat(declares(StreamExecCalc.class, HOOK))
                .as(
                        "StreamExecCalc must not override %s. Offload does not preserve RowKind, so"
                                + " a changelog row could silently become an insert",
                        HOOK)
                .isFalse();
    }

    @Test
    @DisplayName("and the shared base class has no offload surface left to inherit")
    void theBaseClassCarriesNothing() {
        assertThat(
                        Arrays.stream(CommonExecCalc.class.getDeclaredMethods())
                                .map(Method::getName)
                                .filter(
                                        name ->
                                                name.toLowerCase().contains("gpu")
                                                        || name.toLowerCase().contains("device")))
                .as("CommonExecCalc must not declare offload methods for StreamExecCalc to inherit")
                .isEmpty();
    }

    /**
     * The node that can offload is the one whose generated operator discards {@code RowKind}.
     *
     * <p>This is M2.7, and the pairing is the whole argument. {@code retainHeader} is what decides
     * whether the code generator emits
     *
     * <pre>result.setRowKind(input.getRowKind());</pre>
     *
     * <p>and for a batch Calc it is {@code false}, so the operator Flink would have run emits an
     * insert whatever came in. The accelerated operator builds a fresh {@code GenericRowData},
     * which is an insert by default. Those are the same behaviour, so offloading a batch Calc
     * cannot change a row's kind relative to not offloading it — there is nothing to preserve that
     * was being preserved before.
     *
     * <p>The streaming Calc is {@code retainHeader = true} and would have a header to lose. It
     * cannot reach the path, which is what the two tests above establish, and this is why that
     * matters rather than being housekeeping: a {@code -U} becoming a {@code +I} is a changelog
     * that no longer describes the same table.
     */
    @Test
    @DisplayName("the batch node discards RowKind anyway, so offloading it changes nothing")
    void theOffloadableNodeDoesNotRetainTheHeader() throws Exception {
        assertThat(retainsHeader(batchCalc()))
                .as(
                        "a batch Calc must not retain the header, or the accelerated operator -- which"
                                + " always emits an insert -- would differ from the one it replaces")
                .isFalse();
        assertThat(retainsHeader(streamCalc()))
                .as("and the streaming Calc must, which is exactly why it must not offload")
                .isTrue();
    }

    private static CommonExecCalc batchCalc() {
        return new BatchExecCalc(
                new Configuration(),
                Collections.emptyList(),
                null,
                InputProperty.DEFAULT,
                RowType.of(new DoubleType()),
                "batch calc under test");
    }

    private static CommonExecCalc streamCalc() {
        return new StreamExecCalc(
                new Configuration(),
                Collections.emptyList(),
                null,
                InputProperty.DEFAULT,
                RowType.of(new DoubleType()),
                "stream calc under test");
    }

    private static boolean retainsHeader(CommonExecCalc calc) throws Exception {
        Field field = CommonExecCalc.class.getDeclaredField("retainHeader");
        field.setAccessible(true);
        return field.getBoolean(calc);
    }

    private static boolean declares(Class<?> type, String method) {
        return Arrays.stream(type.getDeclaredMethods()).anyMatch(m -> m.getName().equals(method));
    }
}
