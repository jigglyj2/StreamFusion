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

import com.google.protobuf.ByteString;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import tech.streamfusion.proto.plan.v1.Arithmetic;
import tech.streamfusion.proto.plan.v1.ArithmeticOperator;
import tech.streamfusion.proto.plan.v1.BinaryLiteral;
import tech.streamfusion.proto.plan.v1.BooleanLiteral;
import tech.streamfusion.proto.plan.v1.BooleanOperator;
import tech.streamfusion.proto.plan.v1.ComparisonOperator;
import tech.streamfusion.proto.plan.v1.DateLiteral;
import tech.streamfusion.proto.plan.v1.DecimalLiteral;
import tech.streamfusion.proto.plan.v1.DoubleLiteral;
import tech.streamfusion.proto.plan.v1.Expression;
import tech.streamfusion.proto.plan.v1.FloatLiteral;
import tech.streamfusion.proto.plan.v1.IntegerLiteral;
import tech.streamfusion.proto.plan.v1.LongLiteral;
import tech.streamfusion.proto.plan.v1.StringLiteral;
import tech.streamfusion.proto.plan.v1.TimeLiteral;
import tech.streamfusion.proto.plan.v1.TimestampLiteral;
import tech.streamfusion.proto.plan.v1.UnaryMinus;

/** Reflection entry point called by the small Flink planner patch for eligible calc nodes. */
public final class StreamFusionCalcTranslator {
    private StreamFusionCalcTranslator() {}

    public static Transformation<RowData> translate(
            Transformation<RowData> input,
            RowType inputType,
            RowType outputType,
            List<?> projections,
            Object condition) {
        if (!isSupportedCalc(inputType, outputType, projections, condition)) {
            return null;
        }

        List<Expression> nativeProjections = new ArrayList<>(projections.size());
        for (int outputIndex = 0; outputIndex < projections.size(); outputIndex++) {
            nativeProjections.add(projectionExpression(
                    projections.get(outputIndex),
                    inputType,
                    outputType.getTypeAt(outputIndex).getTypeRoot()));
        }
        StreamFusionCondition nativeCondition = condition(condition, inputType);
        StreamFusionIdentityCalcOperator operator =
                new StreamFusionIdentityCalcOperator(inputType, outputType, nativeProjections, nativeCondition);
        OneInputTransformation<RowData, RowData> transformation = new OneInputTransformation<>(
                input,
                "streamfusion-identity-calc",
                operator,
                InternalTypeInfo.of(outputType),
                input.getParallelism(),
                false);
        transformation.declareManagedMemoryUseCaseAtOperatorScope(ManagedMemoryUseCase.OPERATOR, 1);
        return transformation;
    }

    public static boolean canTranslate(RowType inputType, RowType outputType, List<?> projections, Object condition) {
        return isSupportedCalc(inputType, outputType, projections, condition);
    }

    private static boolean isSupportedCalc(
            RowType inputType, RowType outputType, List<?> projections, Object condition) {
        if (projections.isEmpty() || outputType.getFieldCount() != projections.size()) {
            return false;
        }
        for (int outputIndex = 0; outputIndex < projections.size(); outputIndex++) {
            Object projection = projections.get(outputIndex);
            int inputIndex = inputIndex(projection);
            if (inputIndex >= 0) {
                if (inputIndex >= inputType.getFieldCount()
                        || !isSupportedProjectionType(
                                inputType.getTypeAt(inputIndex).getTypeRoot())
                        || !inputType.getTypeAt(inputIndex).equals(outputType.getTypeAt(outputIndex))) {
                    return false;
                }
            } else {
                LogicalTypeRoot outputRoot = outputType.getTypeAt(outputIndex).getTypeRoot();
                if ((outputRoot != LogicalTypeRoot.INTEGER
                                && outputRoot != LogicalTypeRoot.BIGINT
                                && outputRoot != LogicalTypeRoot.FLOAT
                                && outputRoot != LogicalTypeRoot.DOUBLE
                                && outputRoot != LogicalTypeRoot.DECIMAL
                                && outputRoot != LogicalTypeRoot.BOOLEAN
                                && outputRoot != LogicalTypeRoot.DATE
                                && outputRoot != LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE
                                && outputRoot != LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE
                                && outputRoot != LogicalTypeRoot.CHAR
                                && outputRoot != LogicalTypeRoot.VARCHAR
                                && outputRoot != LogicalTypeRoot.BINARY
                                && outputRoot != LogicalTypeRoot.VARBINARY)
                        || projectionExpression(projection, inputType, outputRoot) == null) {
                    return false;
                }
            }
        }
        if (condition == null) {
            return true;
        }
        return condition(condition, inputType) != null;
    }

    private static StreamFusionCondition condition(Object condition, RowType inputType) {
        if (condition == null) {
            return null;
        }
        int directInputIndex = inputIndex(condition);
        if (directInputIndex >= 0
                && directInputIndex < inputType.getFieldCount()
                && inputType.getTypeAt(directInputIndex).getTypeRoot() == LogicalTypeRoot.BOOLEAN) {
            return new StreamFusionBooleanColumnCondition(
                    directInputIndex,
                    StreamFusionIdentityCalcOperator.inputReference(
                            directInputIndex,
                            StreamFusionIdentityCalcOperator.logicalType(inputType, directInputIndex)));
        }
        StreamFusionCondition comparison = comparison(condition, inputType);
        if (comparison != null) {
            return comparison;
        }
        String kind = invoke(condition, "getKind").toString();
        if ("AND".equals(kind) || "OR".equals(kind)) {
            List<?> operands = (List<?>) invoke(condition, "getOperands");
            if (operands.size() != 2) {
                return null;
            }
            StreamFusionCondition left = condition(operands.get(0), inputType);
            StreamFusionCondition right = condition(operands.get(1), inputType);
            if (left == null || right == null) {
                return null;
            }
            return StreamFusionBooleanCondition.binary(
                    left,
                    right,
                    "AND".equals(kind) ? BooleanOperator.BOOLEAN_OPERATOR_AND : BooleanOperator.BOOLEAN_OPERATOR_OR);
        }
        if ("NOT".equals(kind)) {
            List<?> operands = (List<?>) invoke(condition, "getOperands");
            if (operands.size() != 1) {
                return null;
            }
            StreamFusionCondition operand = condition(operands.get(0), inputType);
            return operand == null ? null : StreamFusionBooleanCondition.not(operand);
        }
        if (!"IS_NULL".equals(kind) && !"IS_NOT_NULL".equals(kind)) {
            return null;
        }
        List<?> operands = (List<?>) invoke(condition, "getOperands");
        if (operands.size() != 1) {
            return null;
        }
        int inputIndex = inputIndex(operands.get(0));
        if (inputIndex < 0
                || inputIndex >= inputType.getFieldCount()
                || !isSupportedProjectionType(inputType.getTypeAt(inputIndex).getTypeRoot())) {
            return null;
        }
        return new StreamFusionNullCondition(
                inputIndex,
                "IS_NOT_NULL".equals(kind),
                StreamFusionIdentityCalcOperator.inputReference(
                        inputIndex, StreamFusionIdentityCalcOperator.logicalType(inputType, inputIndex)));
    }

    private static Expression projectionExpression(Object expression, RowType inputType, LogicalTypeRoot expectedType) {
        if (expectedType == LogicalTypeRoot.DECIMAL) {
            return decimalProjectionExpression(expression, inputType);
        }
        int inputIndex = inputIndex(expression);
        if (inputIndex >= 0) {
            if (inputIndex >= inputType.getFieldCount()
                    || inputType.getTypeAt(inputIndex).getTypeRoot() != expectedType) {
                return null;
            }
            return StreamFusionIdentityCalcOperator.inputReference(
                    inputIndex, StreamFusionIdentityCalcOperator.logicalType(inputType, inputIndex));
        }
        if (expectedType == LogicalTypeRoot.BINARY || expectedType == LogicalTypeRoot.VARBINARY) {
            byte[] literal = literal(expression, byte[].class);
            int length = (int) invoke(invoke(expression, "getType"), "getPrecision");
            if (literal != null && length >= literal.length) {
                return Expression.newBuilder()
                        .setBinaryLiteral(BinaryLiteral.newBuilder()
                                .setValue(ByteString.copyFrom(literal))
                                .setFixedWidth(expectedType == LogicalTypeRoot.BINARY)
                                .setLength(length))
                        .build();
            }
        } else if (expectedType == LogicalTypeRoot.CHAR || expectedType == LogicalTypeRoot.VARCHAR) {
            String literal = literal(expression, String.class);
            if (literal != null) {
                return Expression.newBuilder()
                        .setStringLiteral(StringLiteral.newBuilder().setValue(literal))
                        .build();
            }
        } else if (expectedType == LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE) {
            TimestampData timestamp = timestampLiteral(expression);
            int precision = (int) invoke(invoke(expression, "getType"), "getPrecision");
            if (timestamp != null && precision >= 0 && precision <= 9) {
                return Expression.newBuilder()
                        .setTimestampLiteral(TimestampLiteral.newBuilder()
                                .setEpochMillisecond(timestamp.getMillisecond())
                                .setNanoOfMillisecond(timestamp.getNanoOfMillisecond())
                                .setPrecision(precision))
                        .build();
            }
        } else if (expectedType == LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE) {
            Integer millis = integerLiteral(expression);
            int precision = (int) invoke(invoke(expression, "getType"), "getPrecision");
            if (millis != null && precision >= 0 && precision <= 9) {
                return Expression.newBuilder()
                        .setTimeLiteral(TimeLiteral.newBuilder()
                                .setMillisecondOfDay(millis)
                                .setPrecision(precision))
                        .build();
            }
        } else if (expectedType == LogicalTypeRoot.DATE) {
            Integer epochDay = integerLiteral(expression);
            if (epochDay != null) {
                return Expression.newBuilder()
                        .setDateLiteral(DateLiteral.newBuilder().setEpochDay(epochDay))
                        .build();
            }
        } else if (expectedType == LogicalTypeRoot.BOOLEAN) {
            Boolean literal = literal(expression, Boolean.class);
            if (literal != null) {
                return Expression.newBuilder()
                        .setBooleanLiteral(BooleanLiteral.newBuilder().setValue(literal))
                        .build();
            }
        } else if (expectedType == LogicalTypeRoot.INTEGER) {
            Integer literal = integerLiteral(expression);
            if (literal != null) {
                return Expression.newBuilder()
                        .setIntegerLiteral(IntegerLiteral.newBuilder().setValue(literal))
                        .build();
            }
        } else if (expectedType == LogicalTypeRoot.BIGINT) {
            Long literal = longLiteral(expression);
            if (literal != null) {
                return Expression.newBuilder()
                        .setLongLiteral(LongLiteral.newBuilder().setValue(literal))
                        .build();
            }
        } else if (expectedType == LogicalTypeRoot.FLOAT) {
            Float literal = literal(expression, Float.class);
            if (literal != null && Float.isFinite(literal)) {
                return Expression.newBuilder()
                        .setFloatLiteral(FloatLiteral.newBuilder().setValue(literal))
                        .build();
            }
        } else if (expectedType == LogicalTypeRoot.DOUBLE) {
            Double literal = literal(expression, Double.class);
            if (literal != null && Double.isFinite(literal)) {
                return Expression.newBuilder()
                        .setDoubleLiteral(DoubleLiteral.newBuilder().setValue(literal))
                        .build();
            }
        } else {
            return null;
        }
        String kind = invoke(expression, "getKind").toString();
        List<?> operands = (List<?>) invoke(expression, "getOperands");
        if ("MINUS_PREFIX".equals(kind)) {
            if (operands.size() != 1) {
                return null;
            }
            Expression operand = projectionExpression(operands.get(0), inputType, expectedType);
            return operand == null
                    ? null
                    : Expression.newBuilder()
                            .setUnaryMinus(UnaryMinus.newBuilder().setOperand(operand))
                            .build();
        }
        if ("DIVIDE".equals(kind) || "MOD".equals(kind)) {
            if (operands.size() != 2) {
                return null;
            }
            boolean nonzeroLiteralDivisor = expectedType == LogicalTypeRoot.INTEGER
                    ? integerLiteral(operands.get(1)) != null && integerLiteral(operands.get(1)) != 0
                    : expectedType == LogicalTypeRoot.BIGINT
                            && longLiteral(operands.get(1)) != null
                            && longLiteral(operands.get(1)) != 0;
            if (!nonzeroLiteralDivisor) {
                return null;
            }
        }
        ArithmeticOperator operator = arithmeticOperator(kind);
        if (operator == null) {
            return null;
        }
        if (operands.size() != 2) {
            return null;
        }
        Expression left = projectionExpression(operands.get(0), inputType, expectedType);
        Expression right = projectionExpression(operands.get(1), inputType, expectedType);
        if (left == null || right == null) {
            return null;
        }
        return Expression.newBuilder()
                .setArithmetic(
                        Arithmetic.newBuilder().setLeft(left).setRight(right).setOperator(operator))
                .build();
    }

    private static Expression decimalProjectionExpression(Object expression, RowType inputType) {
        Object expressionType = invoke(expression, "getType");
        if (!"DECIMAL".equals(invoke(expressionType, "getSqlTypeName").toString())) {
            return null;
        }
        int precision = (int) invoke(expressionType, "getPrecision");
        int scale = (int) invoke(expressionType, "getScale");
        if (precision < 1 || precision > DecimalType.MAX_PRECISION || scale < 0 || scale > precision) {
            return null;
        }

        int inputIndex = inputIndex(expression);
        if (inputIndex >= 0) {
            if (inputIndex >= inputType.getFieldCount() || !(inputType.getTypeAt(inputIndex) instanceof DecimalType)) {
                return null;
            }
            DecimalType inputDecimal = (DecimalType) inputType.getTypeAt(inputIndex);
            if (inputDecimal.getPrecision() != precision || inputDecimal.getScale() != scale) {
                return null;
            }
            return StreamFusionIdentityCalcOperator.inputReference(
                    inputIndex, StreamFusionIdentityCalcOperator.logicalType(inputType, inputIndex));
        }

        BigDecimal literal = literal(expression, BigDecimal.class);
        if (literal != null) {
            if (literal.scale() != scale || literal.precision() > precision) {
                return null;
            }
            return Expression.newBuilder()
                    .setDecimalLiteral(DecimalLiteral.newBuilder()
                            .setUnscaledValue(literal.unscaledValue().toString())
                            .setScale(scale)
                            .setPrecision(precision))
                    .build();
        }

        String kind = invoke(expression, "getKind").toString();
        List<?> operands = (List<?>) invoke(expression, "getOperands");
        if ("MINUS_PREFIX".equals(kind)) {
            if (operands.size() != 1) {
                return null;
            }
            Expression operand = decimalProjectionExpression(operands.get(0), inputType);
            return operand == null
                    ? null
                    : Expression.newBuilder()
                            .setUnaryMinus(UnaryMinus.newBuilder().setOperand(operand))
                            .build();
        }
        if ("DIVIDE".equals(kind) || "MOD".equals(kind)) {
            return null;
        }
        ArithmeticOperator operator = arithmeticOperator(kind);
        if (operator == null) {
            return null;
        }
        if (operands.size() != 2) {
            return null;
        }
        Expression left = decimalProjectionExpression(operands.get(0), inputType);
        Expression right = decimalProjectionExpression(operands.get(1), inputType);
        if (left == null || right == null) {
            return null;
        }
        return Expression.newBuilder()
                .setArithmetic(
                        Arithmetic.newBuilder().setLeft(left).setRight(right).setOperator(operator))
                .build();
    }

    private static ArithmeticOperator arithmeticOperator(String kind) {
        switch (kind) {
            case "PLUS":
                return ArithmeticOperator.ARITHMETIC_OPERATOR_ADD;
            case "MINUS":
                return ArithmeticOperator.ARITHMETIC_OPERATOR_SUBTRACT;
            case "TIMES":
                return ArithmeticOperator.ARITHMETIC_OPERATOR_MULTIPLY;
            case "DIVIDE":
                return ArithmeticOperator.ARITHMETIC_OPERATOR_DIVIDE;
            case "MOD":
                return ArithmeticOperator.ARITHMETIC_OPERATOR_MODULO;
            default:
                return null;
        }
    }

    private static boolean isSupportedProjectionType(LogicalTypeRoot type) {
        return type == LogicalTypeRoot.INTEGER
                || type == LogicalTypeRoot.TINYINT
                || type == LogicalTypeRoot.SMALLINT
                || type == LogicalTypeRoot.BIGINT
                || type == LogicalTypeRoot.FLOAT
                || type == LogicalTypeRoot.DOUBLE
                || type == LogicalTypeRoot.BOOLEAN
                || type == LogicalTypeRoot.CHAR
                || type == LogicalTypeRoot.VARCHAR
                || type == LogicalTypeRoot.BINARY
                || type == LogicalTypeRoot.VARBINARY
                || type == LogicalTypeRoot.DECIMAL
                || type == LogicalTypeRoot.DATE
                || type == LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE
                || type == LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE
                || type == LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
    }

    private static StreamFusionCondition comparison(Object condition, RowType inputType) {
        if (condition == null) {
            return null;
        }
        ComparisonOperator operator =
                comparisonOperator(invoke(condition, "getKind").toString());
        if (operator == null) {
            return null;
        }
        List<?> operands = (List<?>) invoke(condition, "getOperands");
        if (operands.size() != 2) {
            return null;
        }
        int leftInput = inputIndex(operands.get(0));
        int rightInput = inputIndex(operands.get(1));
        if (leftInput >= 0 && rightInput >= 0) {
            if (leftInput >= inputType.getFieldCount()
                    || rightInput >= inputType.getFieldCount()
                    || !inputType.getTypeAt(leftInput).equals(inputType.getTypeAt(rightInput))
                    || !StreamFusionColumnComparison.supports(
                            inputType.getTypeAt(leftInput).getTypeRoot())
                    || (inputType.getTypeAt(leftInput).getTypeRoot() == LogicalTypeRoot.BOOLEAN
                            && operator != ComparisonOperator.COMPARISON_OPERATOR_EQUAL
                            && operator != ComparisonOperator.COMPARISON_OPERATOR_NOT_EQUAL)) {
                return null;
            }
            return new StreamFusionColumnComparison(leftInput, rightInput, inputType.getTypeAt(leftInput), operator);
        }
        if (leftInput >= 0 && rightInput < 0) {
            return comparison(leftInput, operands.get(1), operator, true, inputType);
        }
        if (rightInput >= 0 && leftInput < 0) {
            return comparison(rightInput, operands.get(0), operator, false, inputType);
        }
        return null;
    }

    private static StreamFusionOrderedComparison comparison(
            int inputIndex,
            Object literalExpression,
            ComparisonOperator operator,
            boolean inputOnLeft,
            RowType inputType) {
        if (inputIndex >= inputType.getFieldCount()) {
            return null;
        }
        LogicalTypeRoot type = inputType.getTypeAt(inputIndex).getTypeRoot();
        if (type == LogicalTypeRoot.TINYINT) {
            Byte literal = literal(literalExpression, Byte.class);
            return literal == null ? null : new StreamFusionByteComparison(inputIndex, literal, operator, inputOnLeft);
        }
        if (type == LogicalTypeRoot.SMALLINT) {
            Short literal = literal(literalExpression, Short.class);
            return literal == null ? null : new StreamFusionShortComparison(inputIndex, literal, operator, inputOnLeft);
        }
        if (type == LogicalTypeRoot.INTEGER) {
            Integer literal = integerLiteral(literalExpression);
            return literal == null ? null : new StreamFusionIntComparison(inputIndex, literal, operator, inputOnLeft);
        }
        if (type == LogicalTypeRoot.BIGINT) {
            Long literal = longLiteral(literalExpression);
            return literal == null ? null : new StreamFusionLongComparison(inputIndex, literal, operator, inputOnLeft);
        }
        if (type == LogicalTypeRoot.FLOAT) {
            Float literal = literal(literalExpression, Float.class);
            return literal == null || !Float.isFinite(literal)
                    ? null
                    : new StreamFusionFloatComparison(inputIndex, literal, operator, inputOnLeft);
        }
        if (type == LogicalTypeRoot.DOUBLE) {
            Double literal = literal(literalExpression, Double.class);
            return literal == null || !Double.isFinite(literal)
                    ? null
                    : new StreamFusionDoubleComparison(inputIndex, literal, operator, inputOnLeft);
        }
        if (type == LogicalTypeRoot.DATE) {
            Integer epochDay = integerLiteral(literalExpression);
            return epochDay == null
                    ? null
                    : new StreamFusionDateComparison(inputIndex, epochDay, operator, inputOnLeft);
        }
        if (type == LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE) {
            Integer millis = integerLiteral(literalExpression);
            int precision = ((TimeType) inputType.getTypeAt(inputIndex)).getPrecision();
            return millis == null
                    ? null
                    : new StreamFusionTimeComparison(inputIndex, millis, precision, operator, inputOnLeft);
        }
        if (type == LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE) {
            TimestampData timestamp = timestampLiteral(literalExpression);
            int precision = ((TimestampType) inputType.getTypeAt(inputIndex)).getPrecision();
            return timestamp == null
                    ? null
                    : new StreamFusionTimestampComparison(inputIndex, timestamp, precision, operator, inputOnLeft);
        }
        if (type == LogicalTypeRoot.DECIMAL) {
            BigDecimal decimal = literal(literalExpression, BigDecimal.class);
            DecimalType decimalType = (DecimalType) inputType.getTypeAt(inputIndex);
            if (decimal == null || decimal.scale() != decimalType.getScale()) {
                return null;
            }
            return new StreamFusionDecimalComparison(
                    inputIndex, decimal, decimalType.getPrecision(), decimalType.getScale(), operator, inputOnLeft);
        }
        return null;
    }

    private static Integer integerLiteral(Object expression) {
        if (!expression.getClass().getSimpleName().equals("RexLiteral")) {
            return null;
        }
        Object value = invoke(expression, "getValueAs", Class.class, Integer.class);
        return value instanceof Integer ? (Integer) value : null;
    }

    private static Long longLiteral(Object expression) {
        return literal(expression, Long.class);
    }

    private static <T> T literal(Object expression, Class<T> literalType) {
        if (!expression.getClass().getSimpleName().equals("RexLiteral")) {
            return null;
        }
        Object value = invoke(expression, "getValueAs", Class.class, literalType);
        return literalType.isInstance(value) ? literalType.cast(value) : null;
    }

    private static TimestampData timestampLiteral(Object expression) {
        if (!expression.getClass().getSimpleName().equals("RexLiteral")) {
            return null;
        }
        try {
            Class<?> timestampString = Class.forName(
                    "org.apache.calcite.util.TimestampString",
                    false,
                    expression.getClass().getClassLoader());
            Object value = invoke(expression, "getValueAs", Class.class, timestampString);
            if (value == null) {
                return null;
            }
            return TimestampData.fromLocalDateTime(
                    LocalDateTime.parse(value.toString().replace(' ', 'T')));
        } catch (ClassNotFoundException | RuntimeException exception) {
            return null;
        }
    }

    private static ComparisonOperator comparisonOperator(String kind) {
        switch (kind) {
            case "EQUALS":
                return ComparisonOperator.COMPARISON_OPERATOR_EQUAL;
            case "NOT_EQUALS":
                return ComparisonOperator.COMPARISON_OPERATOR_NOT_EQUAL;
            case "LESS_THAN":
                return ComparisonOperator.COMPARISON_OPERATOR_LESS_THAN;
            case "LESS_THAN_OR_EQUAL":
                return ComparisonOperator.COMPARISON_OPERATOR_LESS_THAN_OR_EQUAL;
            case "GREATER_THAN":
                return ComparisonOperator.COMPARISON_OPERATOR_GREATER_THAN;
            case "GREATER_THAN_OR_EQUAL":
                return ComparisonOperator.COMPARISON_OPERATOR_GREATER_THAN_OR_EQUAL;
            default:
                return null;
        }
    }

    private static int inputIndex(Object expression) {
        if (!expression.getClass().getSimpleName().equals("RexInputRef")) {
            return -1;
        }
        Object index = invoke(expression, "getIndex");
        return index instanceof Integer ? (Integer) index : -1;
    }

    private static Object invoke(Object target, String methodName) {
        return invoke(target, methodName, null, null);
    }

    private static Object invoke(Object target, String methodName, Class<?> parameterType, Object argument) {
        try {
            Method method = parameterType == null
                    ? target.getClass().getMethod(methodName)
                    : target.getClass().getMethod(methodName, parameterType);
            return parameterType == null ? method.invoke(target) : method.invoke(target, argument);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException exception) {
            throw new IllegalStateException(
                    "Could not inspect planner expression " + target.getClass().getName(), exception);
        }
    }
}
