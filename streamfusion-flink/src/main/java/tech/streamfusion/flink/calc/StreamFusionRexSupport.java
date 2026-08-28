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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import tech.streamfusion.proto.plan.v1.BooleanOperator;
import tech.streamfusion.proto.plan.v1.ComparisonOperator;

/** Reflection entry point called by the small Flink planner patch for eligible calc nodes. */
abstract class StreamFusionRexSupport {
    protected static boolean hasNoArgMethod(Object target, String methodName) {
        try {
            target.getClass().getMethod(methodName);
            return true;
        } catch (NoSuchMethodException ignored) {
            return false;
        }
    }

    protected static String functionName(Object expression) {
        if (!hasNoArgMethod(expression, "getOperands") || !hasNoArgMethod(expression, "getOperator")) {
            return null;
        }
        Object operator = invoke(expression, "getOperator");
        return hasNoArgMethod(operator, "getName") ? invoke(operator, "getName").toString() : null;
    }

    protected static StreamFusionCondition search(Object condition, RowType inputType) {
        try {
            List<?> operands = (List<?>) invoke(condition, "getOperands");
            if (operands.size() != 2) {
                return null;
            }
            int inputIndex = inputIndex(operands.get(0));
            if (inputIndex < 0
                    || inputIndex >= inputType.getFieldCount()
                    || !supportsSearch(inputType.getTypeAt(inputIndex).getTypeRoot())) {
                return null;
            }
            Object sarg = invoke(operands.get(1), "getValue");
            if (sarg == null || !"UNKNOWN".equals(publicField(sarg, "nullAs").toString())) {
                return null;
            }
            if ((inputType.getTypeAt(inputIndex).getTypeRoot() == LogicalTypeRoot.VARBINARY
                            || inputType.getTypeAt(inputIndex).getTypeRoot() == LogicalTypeRoot.BINARY)
                    && ((boolean) invoke(sarg, "isPoints") || (boolean) invoke(sarg, "isComplementedPoints"))) {
                return null;
            }
            Object ranges = invoke(publicField(sarg, "rangeSet"), "asRanges");
            StreamFusionCondition result = null;
            for (Object range : (Iterable<?>) ranges) {
                StreamFusionCondition rangeCondition = null;
                if ((boolean) invoke(range, "hasLowerBound")) {
                    boolean closed =
                            "CLOSED".equals(invoke(range, "lowerBoundType").toString());
                    rangeCondition = searchComparison(
                            inputIndex,
                            invoke(range, "lowerEndpoint"),
                            closed
                                    ? ComparisonOperator.COMPARISON_OPERATOR_GREATER_THAN_OR_EQUAL
                                    : ComparisonOperator.COMPARISON_OPERATOR_GREATER_THAN,
                            inputType);
                    if (rangeCondition == null) {
                        return null;
                    }
                }
                if ((boolean) invoke(range, "hasUpperBound")) {
                    boolean closed =
                            "CLOSED".equals(invoke(range, "upperBoundType").toString());
                    StreamFusionCondition upper = searchComparison(
                            inputIndex,
                            invoke(range, "upperEndpoint"),
                            closed
                                    ? ComparisonOperator.COMPARISON_OPERATOR_LESS_THAN_OR_EQUAL
                                    : ComparisonOperator.COMPARISON_OPERATOR_LESS_THAN,
                            inputType);
                    if (upper == null) {
                        return null;
                    }
                    rangeCondition = rangeCondition == null
                            ? upper
                            : StreamFusionBooleanCondition.binary(
                                    rangeCondition, upper, BooleanOperator.BOOLEAN_OPERATOR_AND);
                }
                if (rangeCondition == null) {
                    return null;
                }
                result = result == null
                        ? rangeCondition
                        : StreamFusionBooleanCondition.binary(
                                result, rangeCondition, BooleanOperator.BOOLEAN_OPERATOR_OR);
            }
            return result;
        } catch (RuntimeException exception) {
            return null;
        }
    }

    protected static boolean supportsSearch(LogicalTypeRoot type) {
        return type == LogicalTypeRoot.TINYINT
                || type == LogicalTypeRoot.SMALLINT
                || type == LogicalTypeRoot.INTEGER
                || type == LogicalTypeRoot.BIGINT
                || type == LogicalTypeRoot.DECIMAL
                || type == LogicalTypeRoot.DATE
                || type == LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE
                || type == LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE
                || type == LogicalTypeRoot.CHAR
                || type == LogicalTypeRoot.VARCHAR
                || type == LogicalTypeRoot.BINARY
                || type == LogicalTypeRoot.VARBINARY;
    }

    protected static StreamFusionCondition searchComparison(
            int inputIndex, Object endpoint, ComparisonOperator operator, RowType inputType) {
        switch (inputType.getTypeAt(inputIndex).getTypeRoot()) {
            case TINYINT:
                return new StreamFusionByteComparison(
                        inputIndex, ((BigDecimal) endpoint).byteValueExact(), operator, true);
            case SMALLINT:
                return new StreamFusionShortComparison(
                        inputIndex, ((BigDecimal) endpoint).shortValueExact(), operator, true);
            case INTEGER:
                return new StreamFusionIntComparison(
                        inputIndex, ((BigDecimal) endpoint).intValueExact(), operator, true);
            case BIGINT:
                return new StreamFusionLongComparison(
                        inputIndex, ((BigDecimal) endpoint).longValueExact(), operator, true);
            case DECIMAL:
                BigDecimal value = (BigDecimal) endpoint;
                DecimalType decimalType = (DecimalType) inputType.getTypeAt(inputIndex);
                if (value.scale() != decimalType.getScale() || value.precision() > decimalType.getPrecision()) {
                    return null;
                }
                return new StreamFusionDecimalComparison(
                        inputIndex, value, decimalType.getPrecision(), decimalType.getScale(), operator, true);
            case DATE:
                return new StreamFusionDateComparison(
                        inputIndex,
                        Math.toIntExact(LocalDate.parse(endpoint.toString()).toEpochDay()),
                        operator,
                        true);
            case TIME_WITHOUT_TIME_ZONE:
                int precision = ((TimeType) inputType.getTypeAt(inputIndex)).getPrecision();
                int millisecondOfDay =
                        Math.toIntExact(LocalTime.parse(endpoint.toString()).toNanoOfDay() / 1_000_000);
                return new StreamFusionTimeComparison(inputIndex, millisecondOfDay, precision, operator, true);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                int timestampPrecision = ((TimestampType) inputType.getTypeAt(inputIndex)).getPrecision();
                TimestampData timestamp = TimestampData.fromLocalDateTime(
                        LocalDateTime.parse(endpoint.toString().replace(' ', 'T')));
                return new StreamFusionTimestampComparison(inputIndex, timestamp, timestampPrecision, operator, true);
            case VARCHAR:
                return new StreamFusionStringComparison(
                        inputIndex, invoke(endpoint, "getValue").toString(), operator, true);
            case CHAR:
                return new StreamFusionStringComparison(
                        inputIndex,
                        invoke(endpoint, "getValue").toString(),
                        ((CharType) inputType.getTypeAt(inputIndex)).getLength(),
                        operator,
                        true);
            case VARBINARY:
                return new StreamFusionBinaryComparison(
                        inputIndex, (byte[]) invoke(endpoint, "getBytes"), operator, true);
            case BINARY:
                return new StreamFusionBinaryComparison(
                        inputIndex,
                        (byte[]) invoke(endpoint, "getBytes"),
                        true,
                        ((BinaryType) inputType.getTypeAt(inputIndex)).getLength(),
                        operator,
                        true);
            default:
                return null;
        }
    }

    protected static Object publicField(Object target, String fieldName) {
        try {
            return target.getClass().getField(fieldName).get(target);
        } catch (NoSuchFieldException | IllegalAccessException exception) {
            throw new IllegalStateException(exception);
        }
    }

    protected static StreamFusionOrderedComparison comparison(
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
        if (type == LogicalTypeRoot.CHAR || type == LogicalTypeRoot.VARCHAR) {
            String literal = literal(literalExpression, String.class);
            if (literal == null) {
                return null;
            }
            if (type == LogicalTypeRoot.CHAR) {
                int length =
                        ((org.apache.flink.table.types.logical.CharType) inputType.getTypeAt(inputIndex)).getLength();
                return org.apache.flink.table.data.binary.BinaryStringData.fromString(literal)
                                        .numChars()
                                == length
                        ? new StreamFusionStringComparison(inputIndex, literal, length, operator, inputOnLeft)
                        : null;
            }
            return new StreamFusionStringComparison(inputIndex, literal, operator, inputOnLeft);
        }
        if (type == LogicalTypeRoot.BINARY || type == LogicalTypeRoot.VARBINARY) {
            byte[] literal = literal(literalExpression, byte[].class);
            LogicalType logicalType = inputType.getTypeAt(inputIndex);
            boolean fixedWidth = type == LogicalTypeRoot.BINARY;
            int length = fixedWidth ? ((BinaryType) logicalType).getLength() : literal == null ? 0 : literal.length;
            return literal == null || (fixedWidth && literal.length != length)
                    ? null
                    : new StreamFusionBinaryComparison(inputIndex, literal, fixedWidth, length, operator, inputOnLeft);
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

    protected static Integer integerLiteral(Object expression) {
        if (!expression.getClass().getSimpleName().equals("RexLiteral")) {
            return null;
        }
        Object value = invoke(expression, "getValueAs", Class.class, Integer.class);
        return value instanceof Integer ? (Integer) value : null;
    }

    protected static Long longLiteral(Object expression) {
        return literal(expression, Long.class);
    }

    protected static <T> T literal(Object expression, Class<T> literalType) {
        if (!expression.getClass().getSimpleName().equals("RexLiteral")) {
            return null;
        }
        Object value = invoke(expression, "getValueAs", Class.class, literalType);
        return literalType.isInstance(value) ? literalType.cast(value) : null;
    }

    protected static boolean isNullLiteral(Object expression) {
        return expression.getClass().getSimpleName().equals("RexLiteral") && invoke(expression, "getValue") == null;
    }

    protected static boolean supportsNullLiteral(LogicalTypeRoot type) {
        return type == LogicalTypeRoot.TINYINT
                || type == LogicalTypeRoot.SMALLINT
                || type == LogicalTypeRoot.INTEGER
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
                || type == LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE;
    }

    protected static TimestampData timestampLiteral(Object expression) {
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

    protected static ComparisonOperator comparisonOperator(String kind) {
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
            case "IS_DISTINCT_FROM":
                return ComparisonOperator.COMPARISON_OPERATOR_IS_DISTINCT_FROM;
            case "IS_NOT_DISTINCT_FROM":
                return ComparisonOperator.COMPARISON_OPERATOR_IS_NOT_DISTINCT_FROM;
            default:
                return null;
        }
    }

    protected static int inputIndex(Object expression) {
        if (!expression.getClass().getSimpleName().equals("RexInputRef")) {
            return -1;
        }
        Object index = invoke(expression, "getIndex");
        return index instanceof Integer ? (Integer) index : -1;
    }

    protected static Object invoke(Object target, String methodName) {
        return invoke(target, methodName, null, null);
    }

    protected static Object invoke(Object target, String methodName, Class<?> parameterType, Object argument) {
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
