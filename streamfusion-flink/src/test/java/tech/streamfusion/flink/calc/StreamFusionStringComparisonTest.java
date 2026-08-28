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
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.proto.plan.v1.ComparisonOperator;

class StreamFusionStringComparisonTest {
    @ParameterizedTest
    @MethodSource("orderedCases")
    void comparesVarcharColumnsAndLiteralsWithFlinkBinaryOrdering(
            String left, String right, ComparisonOperator operator, boolean expected) {
        GenericRowData row = GenericRowData.of(StringData.fromString(left), StringData.fromString(right));
        StreamFusionColumnComparison columns = new StreamFusionColumnComparison(0, 1, new VarCharType(), operator);
        StreamFusionStringComparison literal = new StreamFusionStringComparison(0, right, operator, true);

        assertThat(columns.evaluate(row)).isEqualTo(expected);
        assertThat(literal.evaluate(row)).isEqualTo(expected);
        assertThat(columns.expression().hasComparison()).isTrue();
        assertThat(literal.expression().hasComparison()).isTrue();
    }

    @Test
    void preservesNullAndReversedOperandSemantics() {
        StreamFusionStringComparison reversed =
                new StreamFusionStringComparison(0, "beta", ComparisonOperator.COMPARISON_OPERATOR_LESS_THAN, false);

        assertThat(reversed.evaluate(GenericRowData.of(StringData.fromString("alpha"))))
                .isFalse();
        assertThat(reversed.evaluate(GenericRowData.of((Object) null))).isNull();
        assertThat(StreamFusionColumnComparison.supports(LogicalTypeRoot.VARCHAR))
                .isTrue();
    }

    @ParameterizedTest
    @MethodSource("nullSafeCases")
    void preservesNullSafeVarcharSemantics(String left, String right, ComparisonOperator operator, boolean expected) {
        GenericRowData row = GenericRowData.of(stringData(left), stringData(right));

        assertThat(new StreamFusionColumnComparison(0, 1, new VarCharType(), operator).evaluate(row))
                .isEqualTo(expected);
        assertThat(new StreamFusionStringComparison(0, right, operator, true)
                        .evaluate(GenericRowData.of(stringData(left))))
                .isEqualTo(expected);
    }

    private static Stream<Arguments> orderedCases() {
        return Stream.of(
                Arguments.of("", "alpha", ComparisonOperator.COMPARISON_OPERATOR_LESS_THAN, true),
                Arguments.of("alpha", "alpha", ComparisonOperator.COMPARISON_OPERATOR_EQUAL, true),
                Arguments.of("alpha", "beta", ComparisonOperator.COMPARISON_OPERATOR_NOT_EQUAL, true),
                Arguments.of("beta", "alpha", ComparisonOperator.COMPARISON_OPERATOR_GREATER_THAN, true),
                Arguments.of("éclair", "élan", ComparisonOperator.COMPARISON_OPERATOR_LESS_THAN_OR_EQUAL, true),
                Arguments.of("東京", "東京", ComparisonOperator.COMPARISON_OPERATOR_GREATER_THAN_OR_EQUAL, true));
    }

    private static Stream<Arguments> nullSafeCases() {
        return Stream.of(
                Arguments.of("alpha", "beta", ComparisonOperator.COMPARISON_OPERATOR_IS_DISTINCT_FROM, true),
                Arguments.of("alpha", "alpha", ComparisonOperator.COMPARISON_OPERATOR_IS_NOT_DISTINCT_FROM, true),
                Arguments.of(null, "alpha", ComparisonOperator.COMPARISON_OPERATOR_IS_DISTINCT_FROM, true));
    }

    private static StringData stringData(String value) {
        return value == null ? null : StringData.fromString(value);
    }
}
