/*
 * Copyright 2026 StreamFusion Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package tech.streamfusion.flink.arrow;

import com.google.protobuf.ByteString;
import java.util.List;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import tech.streamfusion.proto.plan.v1.Arithmetic;
import tech.streamfusion.proto.plan.v1.ArithmeticOperator;
import tech.streamfusion.proto.plan.v1.BinaryLiteral;
import tech.streamfusion.proto.plan.v1.Calc;
import tech.streamfusion.proto.plan.v1.Cast;
import tech.streamfusion.proto.plan.v1.CastKind;
import tech.streamfusion.proto.plan.v1.Comparison;
import tech.streamfusion.proto.plan.v1.ComparisonOperator;
import tech.streamfusion.proto.plan.v1.DecimalLiteral;
import tech.streamfusion.proto.plan.v1.DoubleLiteral;
import tech.streamfusion.proto.plan.v1.EmptyType;
import tech.streamfusion.proto.plan.v1.Expression;
import tech.streamfusion.proto.plan.v1.FloatLiteral;
import tech.streamfusion.proto.plan.v1.GreaterThanOrEqual;
import tech.streamfusion.proto.plan.v1.Input;
import tech.streamfusion.proto.plan.v1.InputReference;
import tech.streamfusion.proto.plan.v1.IntegerLiteral;
import tech.streamfusion.proto.plan.v1.LogicalType;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.NullCheck;
import tech.streamfusion.proto.plan.v1.Operator;
import tech.streamfusion.proto.plan.v1.StringLiteral;
import tech.streamfusion.proto.plan.v1.TimeLiteral;
import tech.streamfusion.proto.plan.v1.TimestampLiteral;
import tech.streamfusion.proto.plan.v1.TruthTest;
import tech.streamfusion.proto.plan.v1.TruthTestOperator;

abstract class ArrowCDataBridgeTestSupport {
    protected static GenericRowData row(int id, String text, long decimal, String arrayValue) {
        return GenericRowData.of(
                id,
                StringData.fromString(text),
                DecimalData.fromUnscaledLong(decimal, 10, 2),
                new GenericArrayData(new StringData[] {StringData.fromString(arrayValue), null}));
    }

    protected static byte[] plan(int inputIndex, int minimum) {
        LogicalType integer = LogicalType.newBuilder()
                .setNullable(false)
                .setInteger(EmptyType.getDefaultInstance())
                .build();
        Expression reference = Expression.newBuilder()
                .setInputReference(
                        InputReference.newBuilder().setIndex(inputIndex).setType(integer))
                .build();
        Calc calc = Calc.newBuilder()
                .setInput(Operator.newBuilder().setInput(Input.newBuilder()))
                .addProjections(reference)
                .setCondition(Expression.newBuilder()
                        .setGreaterThanOrEqual(GreaterThanOrEqual.newBuilder()
                                .setLeft(reference)
                                .setRight(Expression.newBuilder()
                                        .setIntegerLiteral(
                                                IntegerLiteral.newBuilder().setValue(minimum)))))
                .build();
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setCalc(calc))
                .build()
                .toByteArray();
    }

    protected static byte[] selectionPlan() {
        LogicalType integer = LogicalType.newBuilder()
                .setNullable(false)
                .setInteger(EmptyType.getDefaultInstance())
                .build();
        Expression value = Expression.newBuilder()
                .setInputReference(InputReference.newBuilder().setIndex(0).setType(integer))
                .build();
        Expression ordinal = Expression.newBuilder()
                .setInputReference(InputReference.newBuilder().setIndex(1).setType(integer))
                .build();
        Calc calc = Calc.newBuilder()
                .setInput(Operator.newBuilder().setInput(Input.newBuilder()))
                .addProjections(value)
                .addProjections(ordinal)
                .setCondition(Expression.newBuilder()
                        .setComparison(Comparison.newBuilder()
                                .setLeft(value)
                                .setRight(Expression.newBuilder()
                                        .setIntegerLiteral(
                                                IntegerLiteral.newBuilder().setValue(2)))
                                .setOperator(ComparisonOperator.COMPARISON_OPERATOR_GREATER_THAN)))
                .build();
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setCalc(calc))
                .build()
                .toByteArray();
    }

    protected static byte[] chainedSelectionPlan() {
        LogicalType integer = LogicalType.newBuilder()
                .setNullable(false)
                .setInteger(EmptyType.getDefaultInstance())
                .build();
        Expression inputValue = Expression.newBuilder()
                .setInputReference(InputReference.newBuilder().setIndex(0).setType(integer))
                .build();
        Expression inputOrdinal = Expression.newBuilder()
                .setInputReference(InputReference.newBuilder().setIndex(1).setType(integer))
                .build();
        Expression incrementedValue = Expression.newBuilder()
                .setArithmetic(Arithmetic.newBuilder()
                        .setLeft(inputValue)
                        .setRight(Expression.newBuilder()
                                .setIntegerLiteral(IntegerLiteral.newBuilder().setValue(10)))
                        .setOperator(ArithmeticOperator.ARITHMETIC_OPERATOR_ADD))
                .build();
        Calc inner = Calc.newBuilder()
                .setInput(Operator.newBuilder().setInput(Input.newBuilder()))
                .addProjections(incrementedValue)
                .addProjections(inputOrdinal)
                .build();
        Expression innerValue = Expression.newBuilder()
                .setInputReference(InputReference.newBuilder().setIndex(0).setType(integer))
                .build();
        Expression innerOrdinal = Expression.newBuilder()
                .setInputReference(InputReference.newBuilder().setIndex(1).setType(integer))
                .build();
        Calc outer = Calc.newBuilder()
                .setInput(Operator.newBuilder().setCalc(inner))
                .addProjections(innerValue)
                .addProjections(innerOrdinal)
                .setCondition(Expression.newBuilder()
                        .setComparison(Comparison.newBuilder()
                                .setLeft(innerValue)
                                .setRight(Expression.newBuilder()
                                        .setIntegerLiteral(
                                                IntegerLiteral.newBuilder().setValue(11)))
                                .setOperator(ComparisonOperator.COMPARISON_OPERATOR_GREATER_THAN)))
                .build();
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setCalc(outer))
                .build()
                .toByteArray();
    }

    protected static byte[] comparisonPlan(ComparisonOperator operator) {
        LogicalType integer = LogicalType.newBuilder()
                .setNullable(false)
                .setInteger(EmptyType.getDefaultInstance())
                .build();
        Expression reference = Expression.newBuilder()
                .setInputReference(InputReference.newBuilder().setIndex(0).setType(integer))
                .build();
        Expression literal = Expression.newBuilder()
                .setIntegerLiteral(IntegerLiteral.newBuilder().setValue(2))
                .build();
        Calc calc = Calc.newBuilder()
                .setInput(Operator.newBuilder().setInput(Input.newBuilder()))
                .addProjections(reference)
                .setCondition(Expression.newBuilder()
                        .setComparison(Comparison.newBuilder()
                                .setLeft(reference)
                                .setRight(literal)
                                .setOperator(operator)))
                .build();
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setCalc(calc))
                .build()
                .toByteArray();
    }

    protected static byte[] charComparisonPlan() {
        LogicalType character = LogicalType.newBuilder()
                .setNullable(false)
                .setVarchar(EmptyType.getDefaultInstance())
                .build();
        Expression reference = Expression.newBuilder()
                .setInputReference(InputReference.newBuilder().setIndex(0).setType(character))
                .build();
        Expression literal = Expression.newBuilder()
                .setStringLiteral(StringLiteral.newBuilder().setValue("m    "))
                .build();
        Calc calc = Calc.newBuilder()
                .setInput(Operator.newBuilder().setInput(Input.newBuilder()))
                .addProjections(reference)
                .setCondition(Expression.newBuilder()
                        .setComparison(Comparison.newBuilder()
                                .setLeft(reference)
                                .setRight(literal)
                                .setOperator(ComparisonOperator.COMPARISON_OPERATOR_GREATER_THAN_OR_EQUAL)))
                .build();
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setCalc(calc))
                .build()
                .toByteArray();
    }

    protected static byte[] fixedBinaryComparisonPlan() {
        LogicalType binary = LogicalType.newBuilder()
                .setNullable(false)
                .setBinary(EmptyType.getDefaultInstance())
                .build();
        Expression reference = Expression.newBuilder()
                .setInputReference(InputReference.newBuilder().setIndex(0).setType(binary))
                .build();
        Expression literal = Expression.newBuilder()
                .setBinaryLiteral(BinaryLiteral.newBuilder()
                        .setValue(ByteString.copyFrom(new byte[] {(byte) 0x80, 0, 0}))
                        .setFixedWidth(true)
                        .setLength(3))
                .build();
        Calc calc = Calc.newBuilder()
                .setInput(Operator.newBuilder().setInput(Input.newBuilder()))
                .addProjections(reference)
                .setCondition(Expression.newBuilder()
                        .setComparison(Comparison.newBuilder()
                                .setLeft(reference)
                                .setRight(literal)
                                .setOperator(ComparisonOperator.COMPARISON_OPERATOR_LESS_THAN)))
                .build();
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setCalc(calc))
                .build()
                .toByteArray();
    }

    protected static byte[] projectionPlan(int... inputIndexes) {
        Calc.Builder calc = Calc.newBuilder().setInput(Operator.newBuilder().setInput(Input.newBuilder()));
        for (int inputIndex : inputIndexes) {
            calc.addProjections(Expression.newBuilder()
                    .setInputReference(InputReference.newBuilder().setIndex(inputIndex)));
        }
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setCalc(calc))
                .build()
                .toByteArray();
    }

    protected static byte[] truthTestPlan() {
        Expression reference = Expression.newBuilder()
                .setInputReference(InputReference.newBuilder().setIndex(0))
                .build();
        Calc.Builder calc = Calc.newBuilder().setInput(Operator.newBuilder().setInput(Input.newBuilder()));
        for (TruthTestOperator operator : List.of(
                TruthTestOperator.TRUTH_TEST_OPERATOR_IS_TRUE,
                TruthTestOperator.TRUTH_TEST_OPERATOR_IS_FALSE,
                TruthTestOperator.TRUTH_TEST_OPERATOR_IS_NOT_TRUE,
                TruthTestOperator.TRUTH_TEST_OPERATOR_IS_NOT_FALSE)) {
            calc.addProjections(Expression.newBuilder()
                    .setTruthTest(TruthTest.newBuilder().setOperand(reference).setOperator(operator)));
        }
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setCalc(calc))
                .build()
                .toByteArray();
    }

    protected static byte[] nullSafeComparisonPlan() {
        Expression reference = Expression.newBuilder()
                .setInputReference(InputReference.newBuilder().setIndex(0))
                .build();
        Expression literal = Expression.newBuilder()
                .setIntegerLiteral(IntegerLiteral.newBuilder().setValue(2))
                .build();
        Calc.Builder calc = Calc.newBuilder().setInput(Operator.newBuilder().setInput(Input.newBuilder()));
        for (ComparisonOperator operator : List.of(
                ComparisonOperator.COMPARISON_OPERATOR_IS_DISTINCT_FROM,
                ComparisonOperator.COMPARISON_OPERATOR_IS_NOT_DISTINCT_FROM)) {
            calc.addProjections(Expression.newBuilder()
                    .setComparison(Comparison.newBuilder()
                            .setLeft(reference)
                            .setRight(literal)
                            .setOperator(operator)));
        }
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setCalc(calc))
                .build()
                .toByteArray();
    }

    protected static byte[] integerWideningPlan() {
        Calc.Builder calc = Calc.newBuilder().setInput(Operator.newBuilder().setInput(Input.newBuilder()));
        addCast(
                calc,
                0,
                LogicalType.newBuilder().setSmallint(EmptyType.getDefaultInstance()),
                CastKind.CAST_KIND_TINYINT_TO_SMALLINT);
        addCast(
                calc,
                0,
                LogicalType.newBuilder().setInteger(EmptyType.getDefaultInstance()),
                CastKind.CAST_KIND_TINYINT_TO_INTEGER);
        addCast(
                calc,
                0,
                LogicalType.newBuilder().setBigint(EmptyType.getDefaultInstance()),
                CastKind.CAST_KIND_TINYINT_TO_BIGINT);
        addCast(
                calc,
                1,
                LogicalType.newBuilder().setInteger(EmptyType.getDefaultInstance()),
                CastKind.CAST_KIND_SMALLINT_TO_INTEGER);
        addCast(
                calc,
                1,
                LogicalType.newBuilder().setBigint(EmptyType.getDefaultInstance()),
                CastKind.CAST_KIND_SMALLINT_TO_BIGINT);
        addCast(
                calc,
                2,
                LogicalType.newBuilder().setBigint(EmptyType.getDefaultInstance()),
                CastKind.CAST_KIND_INTEGER_TO_BIGINT);
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setCalc(calc))
                .build()
                .toByteArray();
    }

    protected static byte[] integerToFloatingPlan() {
        Calc.Builder calc = Calc.newBuilder().setInput(Operator.newBuilder().setInput(Input.newBuilder()));
        addCast(
                calc,
                0,
                LogicalType.newBuilder().setFloat(EmptyType.getDefaultInstance()),
                CastKind.CAST_KIND_TINYINT_TO_FLOAT);
        addCast(
                calc,
                0,
                LogicalType.newBuilder().setDouble(EmptyType.getDefaultInstance()),
                CastKind.CAST_KIND_TINYINT_TO_DOUBLE);
        addCast(
                calc,
                1,
                LogicalType.newBuilder().setFloat(EmptyType.getDefaultInstance()),
                CastKind.CAST_KIND_SMALLINT_TO_FLOAT);
        addCast(
                calc,
                1,
                LogicalType.newBuilder().setDouble(EmptyType.getDefaultInstance()),
                CastKind.CAST_KIND_SMALLINT_TO_DOUBLE);
        addCast(
                calc,
                2,
                LogicalType.newBuilder().setDouble(EmptyType.getDefaultInstance()),
                CastKind.CAST_KIND_INTEGER_TO_DOUBLE);
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setCalc(calc))
                .build()
                .toByteArray();
    }

    protected static byte[] floatToDoublePlan() {
        Calc.Builder calc = Calc.newBuilder().setInput(Operator.newBuilder().setInput(Input.newBuilder()));
        addCast(
                calc,
                0,
                LogicalType.newBuilder().setDouble(EmptyType.getDefaultInstance()),
                CastKind.CAST_KIND_FLOAT_TO_DOUBLE);
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setCalc(calc))
                .build()
                .toByteArray();
    }

    protected static byte[] doubleToFloatPlan() {
        Calc.Builder calc = Calc.newBuilder().setInput(Operator.newBuilder().setInput(Input.newBuilder()));
        addCast(
                calc,
                0,
                LogicalType.newBuilder().setFloat(EmptyType.getDefaultInstance()),
                CastKind.CAST_KIND_DOUBLE_TO_FLOAT);
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setCalc(calc))
                .build()
                .toByteArray();
    }

    protected static byte[] integerNarrowingPlan() {
        Calc.Builder calc = Calc.newBuilder().setInput(Operator.newBuilder().setInput(Input.newBuilder()));
        addCast(
                calc,
                0,
                LogicalType.newBuilder().setTinyint(EmptyType.getDefaultInstance()),
                CastKind.CAST_KIND_INTEGER_TO_TINYINT);
        addCast(
                calc,
                0,
                LogicalType.newBuilder().setSmallint(EmptyType.getDefaultInstance()),
                CastKind.CAST_KIND_INTEGER_TO_SMALLINT);
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setCalc(calc))
                .build()
                .toByteArray();
    }

    protected static byte[] smallintToTinyintPlan() {
        Calc.Builder calc = Calc.newBuilder().setInput(Operator.newBuilder().setInput(Input.newBuilder()));
        addCast(
                calc,
                0,
                LogicalType.newBuilder().setTinyint(EmptyType.getDefaultInstance()),
                CastKind.CAST_KIND_SMALLINT_TO_TINYINT);
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setCalc(calc))
                .build()
                .toByteArray();
    }

    protected static byte[] bigintNarrowingPlan() {
        Calc.Builder calc = Calc.newBuilder().setInput(Operator.newBuilder().setInput(Input.newBuilder()));
        addCast(
                calc,
                0,
                LogicalType.newBuilder().setTinyint(EmptyType.getDefaultInstance()),
                CastKind.CAST_KIND_BIGINT_TO_TINYINT);
        addCast(
                calc,
                0,
                LogicalType.newBuilder().setSmallint(EmptyType.getDefaultInstance()),
                CastKind.CAST_KIND_BIGINT_TO_SMALLINT);
        addCast(
                calc,
                0,
                LogicalType.newBuilder().setInteger(EmptyType.getDefaultInstance()),
                CastKind.CAST_KIND_BIGINT_TO_INTEGER);
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setCalc(calc))
                .build()
                .toByteArray();
    }

    protected static byte[] wideIntegerToFloatingPlan() {
        Calc.Builder calc = Calc.newBuilder().setInput(Operator.newBuilder().setInput(Input.newBuilder()));
        addCast(
                calc,
                0,
                LogicalType.newBuilder().setFloat(EmptyType.getDefaultInstance()),
                CastKind.CAST_KIND_INTEGER_TO_FLOAT);
        addCast(
                calc,
                1,
                LogicalType.newBuilder().setFloat(EmptyType.getDefaultInstance()),
                CastKind.CAST_KIND_BIGINT_TO_FLOAT);
        addCast(
                calc,
                1,
                LogicalType.newBuilder().setDouble(EmptyType.getDefaultInstance()),
                CastKind.CAST_KIND_BIGINT_TO_DOUBLE);
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setCalc(calc))
                .build()
                .toByteArray();
    }

    protected static void addCast(Calc.Builder calc, int inputIndex, LogicalType.Builder targetType, CastKind kind) {
        calc.addProjections(Expression.newBuilder()
                .setCast(Cast.newBuilder()
                        .setOperand(Expression.newBuilder()
                                .setInputReference(InputReference.newBuilder().setIndex(inputIndex)))
                        .setTargetType(targetType.setNullable(false))
                        .setKind(kind)));
    }

    protected static byte[] nullCheckPlan(boolean negated) {
        Expression reference = Expression.newBuilder()
                .setInputReference(InputReference.newBuilder().setIndex(0))
                .build();
        Calc calc = Calc.newBuilder()
                .setInput(Operator.newBuilder().setInput(Input.newBuilder()))
                .addProjections(reference)
                .setCondition(Expression.newBuilder()
                        .setNullCheck(
                                NullCheck.newBuilder().setOperand(reference).setNegated(negated)))
                .build();
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setCalc(calc))
                .build()
                .toByteArray();
    }

    protected static byte[] timePlan(int precision) {
        Expression reference = Expression.newBuilder()
                .setInputReference(InputReference.newBuilder().setIndex(0))
                .build();
        Expression noon = Expression.newBuilder()
                .setTimeLiteral(
                        TimeLiteral.newBuilder().setMillisecondOfDay(43_200_000).setPrecision(precision))
                .build();
        Calc calc = Calc.newBuilder()
                .setInput(Operator.newBuilder().setInput(Input.newBuilder()))
                .addProjections(reference)
                .setCondition(Expression.newBuilder()
                        .setComparison(Comparison.newBuilder()
                                .setLeft(reference)
                                .setRight(noon)
                                .setOperator(ComparisonOperator.COMPARISON_OPERATOR_GREATER_THAN_OR_EQUAL)))
                .build();
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setCalc(calc))
                .build()
                .toByteArray();
    }

    protected static byte[] timestampPlan(int precision, TimestampData literal) {
        Expression reference = Expression.newBuilder()
                .setInputReference(InputReference.newBuilder().setIndex(0))
                .build();
        Expression timestamp = Expression.newBuilder()
                .setTimestampLiteral(TimestampLiteral.newBuilder()
                        .setEpochMillisecond(literal.getMillisecond())
                        .setNanoOfMillisecond(literal.getNanoOfMillisecond())
                        .setPrecision(precision))
                .build();
        Calc calc = Calc.newBuilder()
                .setInput(Operator.newBuilder().setInput(Input.newBuilder()))
                .addProjections(reference)
                .setCondition(Expression.newBuilder()
                        .setComparison(Comparison.newBuilder()
                                .setLeft(reference)
                                .setRight(timestamp)
                                .setOperator(ComparisonOperator.COMPARISON_OPERATOR_GREATER_THAN_OR_EQUAL)))
                .build();
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setCalc(calc))
                .build()
                .toByteArray();
    }

    protected static byte[] decimalPlan(int precision, int scale, java.math.BigDecimal literal) {
        Expression reference = Expression.newBuilder()
                .setInputReference(InputReference.newBuilder().setIndex(0))
                .build();
        Expression decimal = Expression.newBuilder()
                .setDecimalLiteral(DecimalLiteral.newBuilder()
                        .setUnscaledValue(literal.unscaledValue().toString())
                        .setPrecision(precision)
                        .setScale(scale))
                .build();
        Calc calc = Calc.newBuilder()
                .setInput(Operator.newBuilder().setInput(Input.newBuilder()))
                .addProjections(reference)
                .setCondition(Expression.newBuilder()
                        .setComparison(Comparison.newBuilder()
                                .setLeft(reference)
                                .setRight(decimal)
                                .setOperator(ComparisonOperator.COMPARISON_OPERATOR_GREATER_THAN_OR_EQUAL)))
                .build();
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setCalc(calc))
                .build()
                .toByteArray();
    }

    protected static byte[] floatingPointArithmeticPlan(boolean singlePrecision) {
        Expression reference = Expression.newBuilder()
                .setInputReference(InputReference.newBuilder().setIndex(0))
                .build();
        Expression literal = singlePrecision
                ? Expression.newBuilder()
                        .setFloatLiteral(FloatLiteral.newBuilder().setValue(1.5F))
                        .build()
                : Expression.newBuilder()
                        .setDoubleLiteral(DoubleLiteral.newBuilder().setValue(1.5D))
                        .build();
        Expression addition = Expression.newBuilder()
                .setArithmetic(Arithmetic.newBuilder()
                        .setLeft(reference)
                        .setRight(literal)
                        .setOperator(ArithmeticOperator.ARITHMETIC_OPERATOR_ADD))
                .build();
        Calc calc = Calc.newBuilder()
                .setInput(Operator.newBuilder().setInput(Input.newBuilder()))
                .addProjections(addition)
                .build();
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setCalc(calc))
                .build()
                .toByteArray();
    }
}
