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

package org.apache.flink.table.planner.plan.accelerator;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.DecimalDataUtils;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The double a DECIMAL literal becomes in the accelerator IR.
 *
 * <p>It must be the double <em>Flink</em> computes with, which is not always the nearest one.
 * {@code DecimalDataUtils.doubleValue} converts a compact decimal as {@code ((double) unscaledLong)
 * / POW10[scale]} — a division that rounds a second time — while {@code BigDecimal.doubleValue()}
 * is correctly rounded. For some literals those are different doubles, and a kernel built from the
 * better one computes a different query.
 *
 * <p>This was not a theory. A screen over 10M rows disagreed between CPU and device on every single
 * row for one probe constant, while seven others agreed exactly; the constant below is that one.
 */
class AccelDecimalLiteralTest {

    /** The literal that exposed it, and the one that hid it. */
    private static final String DIVERGES = "0.9909456437464825";

    private static final String AGREES = "0.3729116549965876";

    @Test
    @DisplayName("a DECIMAL literal becomes the double Flink uses, not the nearest one")
    void matchesFlinksOwnConversion() {
        final BigDecimal value = new BigDecimal(DIVERGES);
        final DecimalType type = new DecimalType(false, 17, 16);

        final double viaFlink =
                DecimalDataUtils.doubleValue(DecimalData.fromBigDecimal(value, 17, 16));
        final double correctlyRounded = value.doubleValue();

        assertThat(viaFlink)
                .as("this literal is only interesting because the two conversions differ")
                .isNotEqualTo(correctlyRounded);
        assertThat(AccelIrBuilder.asFlinkDouble(value, type))
                .as("the IR must carry what the CPU will compute with")
                .isEqualTo(viaFlink);
    }

    @Test
    @DisplayName("a literal where the conversions agree is unaffected")
    void theAgreeingLiteralIsUnchanged() {
        // Guards against a fix that changes every literal rather than the ones that need it: most
        // constants convert identically either way and must keep doing so.
        final BigDecimal value = new BigDecimal(AGREES);
        final DecimalType type = new DecimalType(false, 17, 16);
        assertThat(AccelIrBuilder.asFlinkDouble(value, type)).isEqualTo(value.doubleValue());
    }

    @Test
    @DisplayName("a non-DECIMAL declared type keeps the correctly rounded conversion")
    void nonDecimalTypesAreNotTouched() {
        // A DOUBLE literal is already a double; Flink does no scaled-long division for it, so
        // applying the decimal rule here would invent a difference rather than reproduce one.
        final BigDecimal value = new BigDecimal(DIVERGES);
        assertThat(AccelIrBuilder.asFlinkDouble(value, new DoubleType(false)))
                .isEqualTo(value.doubleValue());
    }
}
