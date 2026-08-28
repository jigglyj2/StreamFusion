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
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.proto.plan.v1.ComparisonOperator;

class StreamFusionNarrowIntegerColumnComparisonTest {
    @ParameterizedTest
    @MethodSource("comparisonCases")
    void matchesFlinkComparisonAndNullSemantics(
            LogicalType type, Object left, Object right, ComparisonOperator operator, Boolean expected) {
        StreamFusionColumnComparison comparison = new StreamFusionColumnComparison(0, 1, type, operator);

        assertThat(comparison.evaluate(GenericRowData.of(left, right))).isEqualTo(expected);
        assertThat(comparison.expression().hasComparison()).isTrue();
    }

    private static Stream<Arguments> comparisonCases() {
        return Stream.of(new TinyIntType(), new SmallIntType()).flatMap(type -> {
            Object lower = value(type, -1);
            Object equal = value(type, 1);
            Object higher = value(type, 2);
            return Stream.of(
                    Arguments.of(type, lower, equal, ComparisonOperator.COMPARISON_OPERATOR_LESS_THAN, true),
                    Arguments.of(type, lower, equal, ComparisonOperator.COMPARISON_OPERATOR_LESS_THAN_OR_EQUAL, true),
                    Arguments.of(type, equal, equal, ComparisonOperator.COMPARISON_OPERATOR_EQUAL, true),
                    Arguments.of(type, lower, equal, ComparisonOperator.COMPARISON_OPERATOR_NOT_EQUAL, true),
                    Arguments.of(type, higher, equal, ComparisonOperator.COMPARISON_OPERATOR_GREATER_THAN, true),
                    Arguments.of(
                            type, higher, equal, ComparisonOperator.COMPARISON_OPERATOR_GREATER_THAN_OR_EQUAL, true),
                    Arguments.of(type, null, equal, ComparisonOperator.COMPARISON_OPERATOR_EQUAL, null),
                    Arguments.of(type, lower, equal, ComparisonOperator.COMPARISON_OPERATOR_IS_DISTINCT_FROM, true),
                    Arguments.of(type, equal, equal, ComparisonOperator.COMPARISON_OPERATOR_IS_NOT_DISTINCT_FROM, true),
                    Arguments.of(type, null, equal, ComparisonOperator.COMPARISON_OPERATOR_IS_DISTINCT_FROM, true),
                    Arguments.of(type, null, null, ComparisonOperator.COMPARISON_OPERATOR_IS_NOT_DISTINCT_FROM, true));
        });
    }

    private static Object value(LogicalType type, int value) {
        if (type instanceof TinyIntType) {
            return Byte.valueOf((byte) value);
        }
        return Short.valueOf((short) value);
    }
}
