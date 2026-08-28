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

import java.util.Random;
import java.util.stream.Stream;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.runtime.operators.sort.SortUtil;
import org.apache.flink.table.types.logical.BinaryType;
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

    @ParameterizedTest
    @MethodSource("nullSafeCases")
    void preservesNullSafeVarbinarySemantics(byte[] left, byte[] right, ComparisonOperator operator, boolean expected) {
        GenericRowData row = GenericRowData.of(left, right);

        assertThat(new StreamFusionColumnComparison(0, 1, new VarBinaryType(), operator).evaluate(row))
                .isEqualTo(expected);
        assertThat(new StreamFusionBinaryComparison(0, right, operator, true).evaluate(GenericRowData.of(left)))
                .isEqualTo(expected);
    }

    @ParameterizedTest
    @MethodSource("fixedWidthCases")
    void comparesFixedWidthBinaryColumnsAndLiterals(
            byte[] left, byte[] right, ComparisonOperator operator, boolean expected) {
        BinaryType type = new BinaryType(3);
        GenericRowData row = GenericRowData.of(left, right);
        StreamFusionBinaryComparison literal = new StreamFusionBinaryComparison(0, right, true, 3, operator, true);

        assertThat(new StreamFusionColumnComparison(0, 1, type, operator).evaluate(row))
                .isEqualTo(expected);
        assertThat(literal.evaluate(row)).isEqualTo(expected);
        assertThat(literal.expression()
                        .getComparison()
                        .getRight()
                        .getBinaryLiteral()
                        .getFixedWidth())
                .isTrue();
        assertThat(literal.expression()
                        .getComparison()
                        .getRight()
                        .getBinaryLiteral()
                        .getLength())
                .isEqualTo(3);
        assertThat(StreamFusionColumnComparison.supports(LogicalTypeRoot.BINARY))
                .isTrue();
    }

    @Test
    void rejectsIncorrectFixedWidthLiteral() {
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> new StreamFusionBinaryComparison(
                        0, new byte[] {1}, true, 2, ComparisonOperator.COMPARISON_OPERATOR_EQUAL, true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("BINARY(2) literal has 1 bytes");
    }

    @Test
    void generatedFixedWidthComparisonsMatchFlink() {
        Random random = new Random(0x5F10L);
        for (int width : new int[] {1, 3, 8, 9, 32}) {
            BinaryType type = new BinaryType(width);
            for (int caseIndex = 0; caseIndex < 256; caseIndex++) {
                byte[] left = new byte[width];
                byte[] right = new byte[width];
                random.nextBytes(left);
                random.nextBytes(right);
                GenericRowData row = GenericRowData.of(left, right);
                int flinkComparison = SortUtil.compareBinary(left, right);

                for (ComparisonOperator operator : orderedOperators()) {
                    boolean expected = evaluateComparison(flinkComparison, operator);
                    assertThat(new StreamFusionColumnComparison(0, 1, type, operator).evaluate(row))
                            .isEqualTo(expected);
                    assertThat(new StreamFusionBinaryComparison(0, right, true, width, operator, true).evaluate(row))
                            .isEqualTo(expected);
                }
            }
        }
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

    private static Stream<Arguments> nullSafeCases() {
        return Stream.of(
                Arguments.of(
                        new byte[] {0}, new byte[] {1}, ComparisonOperator.COMPARISON_OPERATOR_IS_DISTINCT_FROM, true),
                Arguments.of(
                        new byte[] {1},
                        new byte[] {1},
                        ComparisonOperator.COMPARISON_OPERATOR_IS_NOT_DISTINCT_FROM,
                        true),
                Arguments.of(null, new byte[] {1}, ComparisonOperator.COMPARISON_OPERATOR_IS_DISTINCT_FROM, true));
    }

    private static Stream<Arguments> fixedWidthCases() {
        return Stream.of(
                Arguments.of(
                        new byte[] {0, 0, 0},
                        new byte[] {0, 0, 1},
                        ComparisonOperator.COMPARISON_OPERATOR_LESS_THAN,
                        true),
                Arguments.of(
                        new byte[] {0, 1, 0}, new byte[] {0, 1, 0}, ComparisonOperator.COMPARISON_OPERATOR_EQUAL, true),
                Arguments.of(
                        new byte[] {(byte) 0x80, 0, 0},
                        new byte[] {0x7f, (byte) 0xff, (byte) 0xff},
                        ComparisonOperator.COMPARISON_OPERATOR_GREATER_THAN,
                        true));
    }

    private static ComparisonOperator[] orderedOperators() {
        return new ComparisonOperator[] {
            ComparisonOperator.COMPARISON_OPERATOR_EQUAL,
            ComparisonOperator.COMPARISON_OPERATOR_NOT_EQUAL,
            ComparisonOperator.COMPARISON_OPERATOR_LESS_THAN,
            ComparisonOperator.COMPARISON_OPERATOR_LESS_THAN_OR_EQUAL,
            ComparisonOperator.COMPARISON_OPERATOR_GREATER_THAN,
            ComparisonOperator.COMPARISON_OPERATOR_GREATER_THAN_OR_EQUAL
        };
    }

    private static boolean evaluateComparison(int comparison, ComparisonOperator operator) {
        switch (operator) {
            case COMPARISON_OPERATOR_EQUAL:
                return comparison == 0;
            case COMPARISON_OPERATOR_NOT_EQUAL:
                return comparison != 0;
            case COMPARISON_OPERATOR_LESS_THAN:
                return comparison < 0;
            case COMPARISON_OPERATOR_LESS_THAN_OR_EQUAL:
                return comparison <= 0;
            case COMPARISON_OPERATOR_GREATER_THAN:
                return comparison > 0;
            case COMPARISON_OPERATOR_GREATER_THAN_OR_EQUAL:
                return comparison >= 0;
            default:
                throw new IllegalArgumentException("Not an ordered operator: " + operator);
        }
    }
}
