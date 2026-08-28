/*
 * Copyright 2026 StreamFusion Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package tech.streamfusion.flink.calc;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.stream.Stream;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.proto.plan.v1.ComparisonOperator;

class StreamFusionBinaryComparisonTest {
    @ParameterizedTest
    @MethodSource("orderedCases")
    void comparesVarbinaryColumnsAndLiteralsAsUnsignedBytes(
            byte[] left, byte[] right, ComparisonOperator operator, boolean expected) {
        GenericRowData row = GenericRowData.of(left, right);
        StreamFusionColumnComparison columns = new StreamFusionColumnComparison(0, 1, new VarBinaryType(), operator);
        StreamFusionBinaryComparison literal = new StreamFusionBinaryComparison(0, right, operator, true);

        assertThat(columns.evaluate(row)).isEqualTo(expected);
        assertThat(literal.evaluate(row)).isEqualTo(expected);
        assertThat(columns.expression().hasComparison()).isTrue();
        assertThat(literal.expression().hasComparison()).isTrue();
    }

    @Test
    void preservesNullAndReversedOperandSemantics() {
        StreamFusionBinaryComparison reversed = new StreamFusionBinaryComparison(
                0, new byte[] {2}, ComparisonOperator.COMPARISON_OPERATOR_LESS_THAN, false);

        assertThat(reversed.evaluate(GenericRowData.of(new byte[] {1}))).isFalse();
        assertThat(reversed.evaluate(GenericRowData.of((Object) null))).isNull();
        assertThat(StreamFusionColumnComparison.supports(LogicalTypeRoot.VARBINARY))
                .isTrue();
    }

    private static Stream<Arguments> orderedCases() {
        return Stream.of(
                Arguments.of(new byte[0], new byte[] {0}, ComparisonOperator.COMPARISON_OPERATOR_LESS_THAN, true),
                Arguments.of(new byte[] {0, 1}, new byte[] {0, 1}, ComparisonOperator.COMPARISON_OPERATOR_EQUAL, true),
                Arguments.of(new byte[] {0}, new byte[] {0, 1}, ComparisonOperator.COMPARISON_OPERATOR_NOT_EQUAL, true),
                Arguments.of(
                        new byte[] {(byte) 0x80},
                        new byte[] {0x7f},
                        ComparisonOperator.COMPARISON_OPERATOR_GREATER_THAN,
                        true),
                Arguments.of(
                        new byte[] {(byte) 0xff},
                        new byte[] {(byte) 0xff},
                        ComparisonOperator.COMPARISON_OPERATOR_GREATER_THAN_OR_EQUAL,
                        true));
    }
}
