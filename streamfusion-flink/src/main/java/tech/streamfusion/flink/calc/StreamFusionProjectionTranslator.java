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
import java.math.BigDecimal;
import java.util.List;
import java.util.Locale;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.proto.plan.v1.AbsoluteValue;
import tech.streamfusion.proto.plan.v1.Arithmetic;
import tech.streamfusion.proto.plan.v1.ArithmeticOperator;
import tech.streamfusion.proto.plan.v1.BinaryLiteral;
import tech.streamfusion.proto.plan.v1.BooleanLiteral;
import tech.streamfusion.proto.plan.v1.ByteLiteral;
import tech.streamfusion.proto.plan.v1.Cast;
import tech.streamfusion.proto.plan.v1.CastKind;
import tech.streamfusion.proto.plan.v1.Ceiling;
import tech.streamfusion.proto.plan.v1.CharacterLength;
import tech.streamfusion.proto.plan.v1.Coalesce;
import tech.streamfusion.proto.plan.v1.ComparisonOperator;
import tech.streamfusion.proto.plan.v1.Concat;
import tech.streamfusion.proto.plan.v1.Conditional;
import tech.streamfusion.proto.plan.v1.DateLiteral;
import tech.streamfusion.proto.plan.v1.DecimalLiteral;
import tech.streamfusion.proto.plan.v1.DoubleLiteral;
import tech.streamfusion.proto.plan.v1.Expression;
import tech.streamfusion.proto.plan.v1.FloatLiteral;
import tech.streamfusion.proto.plan.v1.Floor;
import tech.streamfusion.proto.plan.v1.IntegerLiteral;
import tech.streamfusion.proto.plan.v1.LongLiteral;
import tech.streamfusion.proto.plan.v1.Lower;
import tech.streamfusion.proto.plan.v1.NullLiteral;
import tech.streamfusion.proto.plan.v1.ShortLiteral;
import tech.streamfusion.proto.plan.v1.Sign;
import tech.streamfusion.proto.plan.v1.StringLiteral;
import tech.streamfusion.proto.plan.v1.Substring;
import tech.streamfusion.proto.plan.v1.TimeLiteral;
import tech.streamfusion.proto.plan.v1.TimestampLiteral;
import tech.streamfusion.proto.plan.v1.UnaryMinus;
import tech.streamfusion.proto.plan.v1.Upper;
import tech.streamfusion.proto.plan.v1.WhenThen;

/** Reflection entry point called by the small Flink planner patch for eligible calc nodes. */
abstract class StreamFusionProjectionTranslator extends StreamFusionRexSupport {
    protected static Expression projectionExpression(
            Object expression, RowType inputType, org.apache.flink.table.types.logical.LogicalType expectedType) {
        Expression structField =
                StreamFusionComplexProjectionTranslator.structField(expression, inputType, expectedType);
        if (structField != null) {
            return structField;
        }
        Expression arrayElement =
                StreamFusionComplexProjectionTranslator.arrayElement(expression, inputType, expectedType);
        if (arrayElement != null) {
            return arrayElement;
        }
        if (isNullLiteral(expression) && supportsNullLiteral(expectedType.getTypeRoot())) {
            return Expression.newBuilder()
                    .setNullLiteral(NullLiteral.newBuilder()
                            .setType(StreamFusionIdentityCalcOperator.logicalType(expectedType)))
                    .build();
        }
        if ("COALESCE".equals(functionName(expression))) {
            List<?> operands = (List<?>) invoke(expression, "getOperands");
            if (operands.size() < 2) {
                return null;
            }
            Coalesce.Builder coalesce = Coalesce.newBuilder();
            for (Object operand : operands) {
                Expression argument = projectionExpression(operand, inputType, expectedType);
                if (argument == null) {
                    return null;
                }
                coalesce.addArguments(argument);
            }
            return Expression.newBuilder().setCoalesce(coalesce).build();
        }
        String kind = hasNoArgMethod(expression, "getKind")
                ? invoke(expression, "getKind").toString()
                : "";
        if ("CASE".equals(kind) || "IF".equals(functionName(expression))) {
            List<?> operands = (List<?>) invoke(expression, "getOperands");
            if (operands.size() < 3 || operands.size() % 2 == 0) {
                return null;
            }
            Conditional.Builder conditional = Conditional.newBuilder();
            for (int index = 0; index < operands.size() - 1; index += 2) {
                Expression when = StreamFusionExpressionTranslator.conditionExpression(operands.get(index), inputType);
                Expression then = projectionExpression(operands.get(index + 1), inputType, expectedType);
                if (when == null || then == null) {
                    return null;
                }
                conditional.addBranches(WhenThen.newBuilder().setWhen(when).setThen(then));
            }
            Expression elseValue = projectionExpression(operands.get(operands.size() - 1), inputType, expectedType);
            return elseValue == null
                    ? null
                    : Expression.newBuilder()
                            .setConditional(conditional.setElseValue(elseValue))
                            .build();
        }
        if ("ABS".equals(functionName(expression))) {
            List<?> operands = (List<?>) invoke(expression, "getOperands");
            if (operands.size() != 1 || !isNumeric(expectedType.getTypeRoot())) {
                return null;
            }
            Expression operand = projectionExpression(operands.get(0), inputType, expectedType);
            return operand == null
                    ? null
                    : Expression.newBuilder()
                            .setAbsoluteValue(AbsoluteValue.newBuilder().setOperand(operand))
                            .build();
        }
        if ("CEIL".equals(functionName(expression)) && isNonDecimalNumeric(expectedType.getTypeRoot())) {
            List<?> operands = (List<?>) invoke(expression, "getOperands");
            if (operands.size() != 1) {
                return null;
            }
            Expression operand = projectionExpression(operands.get(0), inputType, expectedType);
            return operand == null
                    ? null
                    : Expression.newBuilder()
                            .setCeiling(Ceiling.newBuilder().setOperand(operand))
                            .build();
        }
        if ("FLOOR".equals(functionName(expression)) && isNonDecimalNumeric(expectedType.getTypeRoot())) {
            List<?> operands = (List<?>) invoke(expression, "getOperands");
            if (operands.size() != 1) {
                return null;
            }
            Expression operand = projectionExpression(operands.get(0), inputType, expectedType);
            return operand == null
                    ? null
                    : Expression.newBuilder()
                            .setFloor(Floor.newBuilder().setOperand(operand))
                            .build();
        }
        if ("SIGN".equals(functionName(expression)) && isSignNumeric(expectedType.getTypeRoot())) {
            List<?> operands = (List<?>) invoke(expression, "getOperands");
            if (operands.size() != 1) {
                return null;
            }
            Expression operand = projectionExpression(operands.get(0), inputType, expectedType);
            return operand == null
                    ? null
                    : Expression.newBuilder()
                            .setSign(Sign.newBuilder().setOperand(operand))
                            .build();
        }
        if ("CHAR_LENGTH".equals(functionName(expression)) || "CHARACTER_LENGTH".equals(functionName(expression))) {
            List<?> operands = (List<?>) invoke(expression, "getOperands");
            if (expectedType.getTypeRoot() != LogicalTypeRoot.INTEGER || operands.size() != 1) {
                return null;
            }
            org.apache.flink.table.types.logical.LogicalType operandType =
                    StreamFusionExpressionTranslator.expressionLogicalType(operands.get(0));
            Expression operand = operandType == null || operandType.getTypeRoot() != LogicalTypeRoot.VARCHAR
                    ? null
                    : projectionExpression(operands.get(0), inputType, operandType);
            return operand == null
                    ? null
                    : Expression.newBuilder()
                            .setCharacterLength(CharacterLength.newBuilder().setOperand(operand))
                            .build();
        }
        if (("LOWER".equals(functionName(expression)) || "UPPER".equals(functionName(expression)))
                && expectedType.getTypeRoot() == LogicalTypeRoot.VARCHAR
                && supportsLocaleIndependentCaseMapping()) {
            List<?> operands = (List<?>) invoke(expression, "getOperands");
            if (operands.size() != 1) {
                return null;
            }
            Expression operand = projectionExpression(operands.get(0), inputType, expectedType);
            if (operand == null) {
                return null;
            }
            return "LOWER".equals(functionName(expression))
                    ? Expression.newBuilder()
                            .setLower(Lower.newBuilder().setOperand(operand))
                            .build()
                    : Expression.newBuilder()
                            .setUpper(Upper.newBuilder().setOperand(operand))
                            .build();
        }
        if ("CONCAT".equals(functionName(expression)) && expectedType.getTypeRoot() == LogicalTypeRoot.VARCHAR) {
            List<?> operands = (List<?>) invoke(expression, "getOperands");
            if (operands.size() < 2) {
                return null;
            }
            Concat.Builder concat = Concat.newBuilder();
            for (Object operand : operands) {
                Expression argument = projectionExpression(operand, inputType, expectedType);
                if (argument == null) {
                    return null;
                }
                concat.addArguments(argument);
            }
            return Expression.newBuilder().setConcat(concat).build();
        }
        if (("SUBSTRING".equals(functionName(expression)) || "SUBSTR".equals(functionName(expression)))
                && expectedType.getTypeRoot() == LogicalTypeRoot.VARCHAR) {
            List<?> operands = (List<?>) invoke(expression, "getOperands");
            if (operands.size() < 2 || operands.size() > 3) {
                return null;
            }
            Expression operand = projectionExpression(operands.get(0), inputType, expectedType);
            Integer start = integerLiteral(operands.get(1));
            Integer length = operands.size() == 3 ? integerLiteral(operands.get(2)) : null;
            if (operand == null
                    || start == null
                    || start <= 0
                    || (operands.size() == 3 && (length == null || length < 0))) {
                return null;
            }
            Substring.Builder substring =
                    Substring.newBuilder().setOperand(operand).setStart(start);
            if (length != null) {
                substring.setLength(length);
            }
            return Expression.newBuilder().setSubstring(substring).build();
        }
        return projectionExpression(expression, inputType, expectedType.getTypeRoot());
    }

    protected static boolean supportsLocaleIndependentCaseMapping() {
        String language = Locale.getDefault().getLanguage();
        return !"tr".equals(language) && !"az".equals(language) && !"lt".equals(language);
    }

    protected static boolean isSignNumeric(LogicalTypeRoot type) {
        return type == LogicalTypeRoot.INTEGER
                || type == LogicalTypeRoot.BIGINT
                || type == LogicalTypeRoot.FLOAT
                || type == LogicalTypeRoot.DOUBLE
                || type == LogicalTypeRoot.DECIMAL;
    }

    protected static boolean isNonDecimalNumeric(LogicalTypeRoot type) {
        return type == LogicalTypeRoot.TINYINT
                || type == LogicalTypeRoot.SMALLINT
                || type == LogicalTypeRoot.INTEGER
                || type == LogicalTypeRoot.BIGINT
                || type == LogicalTypeRoot.FLOAT
                || type == LogicalTypeRoot.DOUBLE;
    }

    protected static boolean isNumeric(LogicalTypeRoot type) {
        return type == LogicalTypeRoot.TINYINT
                || type == LogicalTypeRoot.SMALLINT
                || type == LogicalTypeRoot.INTEGER
                || type == LogicalTypeRoot.BIGINT
                || type == LogicalTypeRoot.FLOAT
                || type == LogicalTypeRoot.DOUBLE
                || type == LogicalTypeRoot.DECIMAL;
    }

    protected static Expression projectionExpression(
            Object expression, RowType inputType, LogicalTypeRoot expectedType) {
        if (expectedType == LogicalTypeRoot.DECIMAL) {
            return decimalProjectionExpression(expression, inputType);
        }
        if (expectedType == LogicalTypeRoot.BOOLEAN) {
            return booleanProjectionExpression(expression, inputType);
        }
        Expression cast = wideningCastExpression(expression, inputType, expectedType);
        if (cast != null) {
            return cast;
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
        } else if (expectedType == LogicalTypeRoot.TINYINT) {
            Byte literal = literal(expression, Byte.class);
            if (literal != null) {
                return Expression.newBuilder()
                        .setByteLiteral(ByteLiteral.newBuilder().setValue(literal))
                        .build();
            }
        } else if (expectedType == LogicalTypeRoot.SMALLINT) {
            Short literal = literal(expression, Short.class);
            if (literal != null) {
                return Expression.newBuilder()
                        .setShortLiteral(ShortLiteral.newBuilder().setValue(literal))
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
        if (!hasNoArgMethod(expression, "getOperands")) {
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
            if ("DIVIDE".equals(kind) && expectedType == LogicalTypeRoot.DOUBLE) {
                // IEEE-754 division is defined for zero divisors and remains vectorized.
            } else {
                boolean nonzeroLiteralDivisor = expectedType == LogicalTypeRoot.INTEGER
                        ? integerLiteral(operands.get(1)) != null && integerLiteral(operands.get(1)) != 0
                        : expectedType == LogicalTypeRoot.BIGINT
                                && longLiteral(operands.get(1)) != null
                                && longLiteral(operands.get(1)) != 0;
                if (!nonzeroLiteralDivisor) {
                    return null;
                }
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

    protected static Expression wideningCastExpression(
            Object expression, RowType inputType, LogicalTypeRoot expectedType) {
        if (!expression.getClass().getSimpleName().equals("RexCall")
                || !"CAST".equals(invoke(expression, "getKind").toString())) {
            return null;
        }
        List<?> operands = (List<?>) invoke(expression, "getOperands");
        if (operands.size() != 1) {
            return null;
        }
        int inputIndex = inputIndex(operands.get(0));
        if (inputIndex < 0 || inputIndex >= inputType.getFieldCount()) {
            return null;
        }
        LogicalTypeRoot sourceType = inputType.getTypeAt(inputIndex).getTypeRoot();
        CastKind castKind = StreamFusionCastSupport.kind(sourceType, expectedType);
        if (castKind == CastKind.CAST_KIND_UNSPECIFIED) {
            return null;
        }
        boolean nullable = (boolean) invoke(invoke(expression, "getType"), "isNullable");
        return Expression.newBuilder()
                .setCast(Cast.newBuilder()
                        .setOperand(StreamFusionIdentityCalcOperator.inputReference(
                                inputIndex, StreamFusionIdentityCalcOperator.logicalType(inputType, inputIndex)))
                        .setTargetType(StreamFusionCastSupport.targetType(expectedType, nullable))
                        .setKind(castKind))
                .build();
    }

    protected static Expression booleanProjectionExpression(Object expression, RowType inputType) {
        Boolean literal = literal(expression, Boolean.class);
        if (literal != null) {
            return Expression.newBuilder()
                    .setBooleanLiteral(BooleanLiteral.newBuilder().setValue(literal))
                    .build();
        }
        return StreamFusionExpressionTranslator.conditionExpression(expression, inputType);
    }

    protected static Expression decimalProjectionExpression(Object expression, RowType inputType) {
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

    protected static ArithmeticOperator arithmeticOperator(String kind) {
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

    protected static boolean isSupportedProjectionType(LogicalTypeRoot type) {
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
                || type == LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE
                || type == LogicalTypeRoot.ARRAY
                || type == LogicalTypeRoot.MAP
                || type == LogicalTypeRoot.ROW;
    }

    protected static StreamFusionCondition comparison(Object condition, RowType inputType) {
        if (condition == null || !hasNoArgMethod(condition, "getOperands")) {
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
        StreamFusionCondition substring =
                substringComparison(operands.get(0), operands.get(1), operator, true, inputType);
        if (substring != null) {
            return substring;
        }
        substring = substringComparison(operands.get(1), operands.get(0), operator, false, inputType);
        if (substring != null) {
            return substring;
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
                            && operator != ComparisonOperator.COMPARISON_OPERATOR_NOT_EQUAL
                            && operator != ComparisonOperator.COMPARISON_OPERATOR_IS_DISTINCT_FROM
                            && operator != ComparisonOperator.COMPARISON_OPERATOR_IS_NOT_DISTINCT_FROM)) {
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

    protected static StreamFusionCondition substringComparison(
            Object substring, Object literal, ComparisonOperator operator, boolean substringOnLeft, RowType inputType) {
        String name = functionName(substring);
        if (!"SUBSTRING".equals(name) && !"SUBSTR".equals(name)) {
            return null;
        }
        List<?> operands = (List<?>) invoke(substring, "getOperands");
        if (operands.size() < 2 || operands.size() > 3) {
            return null;
        }
        int inputIndex = inputIndex(operands.get(0));
        Integer start = integerLiteral(operands.get(1));
        Integer length = operands.size() == 3 ? integerLiteral(operands.get(2)) : Integer.MAX_VALUE;
        String literalValue = literal(literal, String.class);
        if (inputIndex < 0
                || inputIndex >= inputType.getFieldCount()
                || inputType.getTypeAt(inputIndex).getTypeRoot() != LogicalTypeRoot.VARCHAR
                || start == null
                || start <= 0
                || length == null
                || length < 0
                || literalValue == null) {
            return null;
        }
        Substring.Builder nativeSubstring = Substring.newBuilder()
                .setOperand(StreamFusionIdentityCalcOperator.inputReference(
                        inputIndex, StreamFusionIdentityCalcOperator.logicalType(inputType, inputIndex)))
                .setStart(start);
        if (operands.size() == 3) {
            nativeSubstring.setLength(length);
        }
        return new StreamFusionSubstringComparison(
                inputIndex,
                start,
                length,
                literalValue,
                operator,
                substringOnLeft,
                Expression.newBuilder().setSubstring(nativeSubstring).build());
    }
}
