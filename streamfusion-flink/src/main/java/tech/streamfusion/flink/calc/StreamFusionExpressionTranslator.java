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

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import tech.streamfusion.proto.plan.v1.BooleanBinary;
import tech.streamfusion.proto.plan.v1.BooleanLiteral;
import tech.streamfusion.proto.plan.v1.BooleanNot;
import tech.streamfusion.proto.plan.v1.BooleanOperator;
import tech.streamfusion.proto.plan.v1.ByteLiteral;
import tech.streamfusion.proto.plan.v1.Comparison;
import tech.streamfusion.proto.plan.v1.ComparisonOperator;
import tech.streamfusion.proto.plan.v1.DateLiteral;
import tech.streamfusion.proto.plan.v1.DecimalLiteral;
import tech.streamfusion.proto.plan.v1.Expression;
import tech.streamfusion.proto.plan.v1.IntegerLiteral;
import tech.streamfusion.proto.plan.v1.LongLiteral;
import tech.streamfusion.proto.plan.v1.NullCheck;
import tech.streamfusion.proto.plan.v1.ShortLiteral;
import tech.streamfusion.proto.plan.v1.StringLiteral;
import tech.streamfusion.proto.plan.v1.TimeLiteral;
import tech.streamfusion.proto.plan.v1.TimestampLiteral;
import tech.streamfusion.proto.plan.v1.TruthTest;
import tech.streamfusion.proto.plan.v1.TruthTestOperator;

/** Reflection entry point called by the small Flink planner patch for eligible calc nodes. */
abstract class StreamFusionExpressionTranslator extends StreamFusionProjectionTranslator {
    protected static String expressionFailure(
            Object expression,
            RowType inputType,
            org.apache.flink.table.types.logical.LogicalType expectedType,
            boolean booleanPosition,
            String path) {
        Expression translated;
        try {
            translated = booleanPosition
                    ? conditionExpression(expression, inputType)
                    : projectionExpression(expression, inputType, expectedType);
        } catch (RuntimeException | AssertionError ignored) {
            return path
                    + "/"
                    + expressionName(expression)
                    + ": planner representation could not be serialized safely ("
                    + expression
                    + ")";
        }
        if (translated != null) {
            return null;
        }
        if (hasNoArgMethod(expression, "getKind")
                && "SEARCH".equals(invoke(expression, "getKind").toString())) {
            return path + "/SEARCH: search argument shape or endpoint type is not parity-approved (" + expression + ")";
        }
        if (hasNoArgMethod(expression, "getOperands")) {
            List<?> operands = (List<?>) invoke(expression, "getOperands");
            for (int index = 0; index < operands.size(); index++) {
                Object operand = operands.get(index);
                org.apache.flink.table.types.logical.LogicalType operandType = expressionLogicalType(operand);
                if (operandType == null) {
                    return path
                            + "/"
                            + expressionName(expression)
                            + ".operand["
                            + index
                            + "]: Calcite type is not supported";
                }
                String childFailure = expressionFailure(
                        operand,
                        inputType,
                        operandType,
                        operandType.getTypeRoot() == LogicalTypeRoot.BOOLEAN,
                        path + "/" + expressionName(expression) + ".operand[" + index + "]");
                if (childFailure != null) {
                    return childFailure;
                }
            }
        }
        return path
                + "/"
                + expressionName(expression)
                + ": expression shape or type combination is not parity-approved ("
                + expression
                + ")";
    }

    protected static String expressionName(Object expression) {
        String function = functionName(expression);
        if (function != null && !function.isEmpty()) {
            return function;
        }
        return hasNoArgMethod(expression, "getKind")
                ? invoke(expression, "getKind").toString()
                : expression.getClass().getSimpleName();
    }

    /**
     * Recursively lowers a boolean expression into the same protobuf tree used by projections.
     * This mirrors Comet's single exprToProto path: filter position changes the expected result
     * type, not the set of child expressions that can be serialized.
     */
    protected static Expression conditionExpression(Object expression, RowType inputType) {
        if (expression == null) {
            return null;
        }
        Boolean literal = literal(expression, Boolean.class);
        if (literal != null) {
            return Expression.newBuilder()
                    .setBooleanLiteral(BooleanLiteral.newBuilder().setValue(literal))
                    .build();
        }
        int directInput = inputIndex(expression);
        if (directInput >= 0
                && directInput < inputType.getFieldCount()
                && inputType.getTypeAt(directInput).getTypeRoot() == LogicalTypeRoot.BOOLEAN) {
            return StreamFusionIdentityCalcOperator.inputReference(
                    directInput, StreamFusionIdentityCalcOperator.logicalType(inputType, directInput));
        }
        if (!hasNoArgMethod(expression, "getOperands")) {
            return null;
        }
        List<?> operands = (List<?>) invoke(expression, "getOperands");
        String kind = invoke(expression, "getKind").toString();
        ComparisonOperator comparisonOperator = comparisonOperator(kind);
        if (comparisonOperator != null && operands.size() == 2) {
            Expression comparison =
                    recursiveComparison(operands.get(0), operands.get(1), comparisonOperator, inputType);
            if (comparison != null) {
                return comparison;
            }
        }
        if ("LIKE".equals(functionName(expression)) && operands.size() == 2) {
            org.apache.flink.table.types.logical.LogicalType valueType = expressionLogicalType(operands.get(0));
            Expression value = valueType == null || valueType.getTypeRoot() != LogicalTypeRoot.VARCHAR
                    ? null
                    : projectionExpression(operands.get(0), inputType, valueType);
            String pattern = literal(operands.get(1), String.class);
            if (value != null && pattern != null && pattern.indexOf('\\') < 0) {
                return Expression.newBuilder()
                        .setLike(tech.streamfusion.proto.plan.v1.Like.newBuilder()
                                .setOperand(value)
                                .setPattern(pattern))
                        .build();
            }
        }
        if (("STARTSWITH".equals(functionName(expression)) || "STARTS_WITH".equals(functionName(expression)))
                && operands.size() == 2) {
            org.apache.flink.table.types.logical.LogicalType valueType = expressionLogicalType(operands.get(0));
            Expression value = valueType == null || valueType.getTypeRoot() != LogicalTypeRoot.VARCHAR
                    ? null
                    : projectionExpression(operands.get(0), inputType, valueType);
            String prefix = literal(operands.get(1), String.class);
            if (value != null && prefix != null) {
                return Expression.newBuilder()
                        .setStartsWith(tech.streamfusion.proto.plan.v1.StartsWith.newBuilder()
                                .setOperand(value)
                                .setPrefix(prefix))
                        .build();
            }
        }
        if (("AND".equals(kind) || "OR".equals(kind)) && operands.size() == 2) {
            Expression left = conditionExpression(operands.get(0), inputType);
            Expression right = conditionExpression(operands.get(1), inputType);
            if (left != null && right != null) {
                return Expression.newBuilder()
                        .setBooleanBinary(BooleanBinary.newBuilder()
                                .setLeft(left)
                                .setRight(right)
                                .setOperator(
                                        "AND".equals(kind)
                                                ? BooleanOperator.BOOLEAN_OPERATOR_AND
                                                : BooleanOperator.BOOLEAN_OPERATOR_OR))
                        .build();
            }
        }
        if ("NOT".equals(kind) && operands.size() == 1) {
            Expression operand = conditionExpression(operands.get(0), inputType);
            return operand == null
                    ? null
                    : Expression.newBuilder()
                            .setBooleanNot(BooleanNot.newBuilder().setOperand(operand))
                            .build();
        }
        TruthTestOperator truthTest = truthTestOperator(kind);
        if (truthTest != null && operands.size() == 1) {
            Expression operand = conditionExpression(operands.get(0), inputType);
            return operand == null
                    ? null
                    : Expression.newBuilder()
                            .setTruthTest(
                                    TruthTest.newBuilder().setOperand(operand).setOperator(truthTest))
                            .build();
        }
        if (("IS_NULL".equals(kind) || "IS_NOT_NULL".equals(kind)) && operands.size() == 1) {
            org.apache.flink.table.types.logical.LogicalType operandType = expressionLogicalType(operands.get(0));
            Expression operand =
                    operandType == null ? null : projectionExpression(operands.get(0), inputType, operandType);
            return operand == null
                    ? null
                    : Expression.newBuilder()
                            .setNullCheck(
                                    NullCheck.newBuilder().setOperand(operand).setNegated("IS_NOT_NULL".equals(kind)))
                            .build();
        }
        if ("SEARCH".equals(kind)) {
            Expression search = recursiveSearch(expression, inputType);
            if (search != null) {
                return search;
            }
        }
        StreamFusionCondition legacy = condition(expression, inputType);
        return legacy == null ? null : legacy.expression();
    }

    protected static Expression recursiveSearch(Object search, RowType inputType) {
        try {
            List<?> operands = (List<?>) invoke(search, "getOperands");
            if (operands.size() != 2) {
                return null;
            }
            org.apache.flink.table.types.logical.LogicalType valueType = expressionLogicalType(operands.get(0));
            if (valueType == null || !supportsSearch(valueType.getTypeRoot())) {
                return null;
            }
            Expression value = projectionExpression(operands.get(0), inputType, valueType);
            Object sarg = invoke(operands.get(1), "getValue");
            if (value == null
                    || sarg == null
                    || !"UNKNOWN".equals(publicField(sarg, "nullAs").toString())) {
                return null;
            }
            if ((valueType.getTypeRoot() == LogicalTypeRoot.VARBINARY
                            || valueType.getTypeRoot() == LogicalTypeRoot.BINARY)
                    && ((boolean) invoke(sarg, "isPoints") || (boolean) invoke(sarg, "isComplementedPoints"))) {
                return null;
            }
            Object ranges = invoke(publicField(sarg, "rangeSet"), "asRanges");
            Expression result = null;
            for (Object range : (Iterable<?>) ranges) {
                Expression rangeExpression = null;
                if ((boolean) invoke(range, "hasLowerBound")) {
                    boolean closed =
                            "CLOSED".equals(invoke(range, "lowerBoundType").toString());
                    rangeExpression = searchComparisonExpression(
                            value,
                            searchLiteral(invoke(range, "lowerEndpoint"), valueType),
                            closed
                                    ? ComparisonOperator.COMPARISON_OPERATOR_GREATER_THAN_OR_EQUAL
                                    : ComparisonOperator.COMPARISON_OPERATOR_GREATER_THAN);
                    if (rangeExpression == null) {
                        return null;
                    }
                }
                if ((boolean) invoke(range, "hasUpperBound")) {
                    boolean closed =
                            "CLOSED".equals(invoke(range, "upperBoundType").toString());
                    Expression upper = searchComparisonExpression(
                            value,
                            searchLiteral(invoke(range, "upperEndpoint"), valueType),
                            closed
                                    ? ComparisonOperator.COMPARISON_OPERATOR_LESS_THAN_OR_EQUAL
                                    : ComparisonOperator.COMPARISON_OPERATOR_LESS_THAN);
                    if (upper == null) {
                        return null;
                    }
                    rangeExpression = rangeExpression == null
                            ? upper
                            : Expression.newBuilder()
                                    .setBooleanBinary(BooleanBinary.newBuilder()
                                            .setLeft(rangeExpression)
                                            .setRight(upper)
                                            .setOperator(BooleanOperator.BOOLEAN_OPERATOR_AND))
                                    .build();
                }
                if (rangeExpression == null) {
                    return null;
                }
                result = result == null
                        ? rangeExpression
                        : Expression.newBuilder()
                                .setBooleanBinary(BooleanBinary.newBuilder()
                                        .setLeft(result)
                                        .setRight(rangeExpression)
                                        .setOperator(BooleanOperator.BOOLEAN_OPERATOR_OR))
                                .build();
            }
            return result;
        } catch (RuntimeException exception) {
            return null;
        }
    }

    protected static Expression searchComparisonExpression(
            Expression value, Expression literal, ComparisonOperator operator) {
        return literal == null
                ? null
                : Expression.newBuilder()
                        .setComparison(Comparison.newBuilder()
                                .setLeft(value)
                                .setRight(literal)
                                .setOperator(operator))
                        .build();
    }

    protected static Expression searchLiteral(
            Object endpoint, org.apache.flink.table.types.logical.LogicalType logicalType) {
        switch (logicalType.getTypeRoot()) {
            case TINYINT:
                return Expression.newBuilder()
                        .setByteLiteral(ByteLiteral.newBuilder().setValue(((BigDecimal) endpoint).byteValueExact()))
                        .build();
            case SMALLINT:
                return Expression.newBuilder()
                        .setShortLiteral(ShortLiteral.newBuilder().setValue(((BigDecimal) endpoint).shortValueExact()))
                        .build();
            case INTEGER:
                return Expression.newBuilder()
                        .setIntegerLiteral(
                                IntegerLiteral.newBuilder().setValue(((BigDecimal) endpoint).intValueExact()))
                        .build();
            case BIGINT:
                return Expression.newBuilder()
                        .setLongLiteral(LongLiteral.newBuilder().setValue(((BigDecimal) endpoint).longValueExact()))
                        .build();
            case DECIMAL:
                BigDecimal decimal = (BigDecimal) endpoint;
                DecimalType decimalType = (DecimalType) logicalType;
                if (decimal.scale() != decimalType.getScale() || decimal.precision() > decimalType.getPrecision()) {
                    return null;
                }
                return Expression.newBuilder()
                        .setDecimalLiteral(DecimalLiteral.newBuilder()
                                .setUnscaledValue(decimal.unscaledValue().toString())
                                .setPrecision(decimalType.getPrecision())
                                .setScale(decimalType.getScale()))
                        .build();
            case DATE:
                return Expression.newBuilder()
                        .setDateLiteral(DateLiteral.newBuilder()
                                .setEpochDay(Math.toIntExact(
                                        LocalDate.parse(endpoint.toString()).toEpochDay())))
                        .build();
            case TIME_WITHOUT_TIME_ZONE:
                return Expression.newBuilder()
                        .setTimeLiteral(TimeLiteral.newBuilder()
                                .setMillisecondOfDay(Math.toIntExact(
                                        LocalTime.parse(endpoint.toString()).toNanoOfDay() / 1_000_000))
                                .setPrecision(((TimeType) logicalType).getPrecision()))
                        .build();
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                TimestampData timestamp = TimestampData.fromLocalDateTime(
                        LocalDateTime.parse(endpoint.toString().replace(' ', 'T')));
                return Expression.newBuilder()
                        .setTimestampLiteral(TimestampLiteral.newBuilder()
                                .setEpochMillisecond(timestamp.getMillisecond())
                                .setNanoOfMillisecond(timestamp.getNanoOfMillisecond())
                                .setPrecision(((TimestampType) logicalType).getPrecision()))
                        .build();
            case VARCHAR:
                return Expression.newBuilder()
                        .setStringLiteral(StringLiteral.newBuilder()
                                .setValue(invoke(endpoint, "getValue").toString()))
                        .build();
            default:
                return null;
        }
    }

    protected static Expression recursiveComparison(
            Object leftExpression, Object rightExpression, ComparisonOperator operator, RowType inputType) {
        org.apache.flink.table.types.logical.LogicalType leftType = expressionLogicalType(leftExpression);
        org.apache.flink.table.types.logical.LogicalType rightType = expressionLogicalType(rightExpression);
        if (leftType == null
                || rightType == null
                || leftType.getTypeRoot() != rightType.getTypeRoot()
                || !supportsRecursiveComparison(leftType.getTypeRoot(), operator)) {
            return null;
        }
        Expression left = projectionExpression(leftExpression, inputType, leftType);
        Expression right = projectionExpression(rightExpression, inputType, rightType);
        if (left == null || right == null) {
            return null;
        }
        return Expression.newBuilder()
                .setComparison(
                        Comparison.newBuilder().setLeft(left).setRight(right).setOperator(operator))
                .build();
    }

    protected static boolean supportsRecursiveComparison(LogicalTypeRoot type, ComparisonOperator operator) {
        if (type == LogicalTypeRoot.BOOLEAN) {
            return operator == ComparisonOperator.COMPARISON_OPERATOR_EQUAL
                    || operator == ComparisonOperator.COMPARISON_OPERATOR_NOT_EQUAL
                    || operator == ComparisonOperator.COMPARISON_OPERATOR_IS_DISTINCT_FROM
                    || operator == ComparisonOperator.COMPARISON_OPERATOR_IS_NOT_DISTINCT_FROM;
        }
        return type == LogicalTypeRoot.TINYINT
                || type == LogicalTypeRoot.SMALLINT
                || type == LogicalTypeRoot.INTEGER
                || type == LogicalTypeRoot.BIGINT
                || type == LogicalTypeRoot.FLOAT
                || type == LogicalTypeRoot.DOUBLE
                || type == LogicalTypeRoot.DECIMAL
                || type == LogicalTypeRoot.VARCHAR
                || type == LogicalTypeRoot.DATE
                || type == LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE
                || type == LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE;
    }

    protected static LogicalTypeRoot expressionTypeRoot(Object expression) {
        if (!hasNoArgMethod(expression, "getType")) {
            return null;
        }
        String typeName =
                invoke(invoke(expression, "getType"), "getSqlTypeName").toString();
        switch (typeName) {
            case "TINYINT":
                return LogicalTypeRoot.TINYINT;
            case "SMALLINT":
                return LogicalTypeRoot.SMALLINT;
            case "INTEGER":
                return LogicalTypeRoot.INTEGER;
            case "BIGINT":
                return LogicalTypeRoot.BIGINT;
            case "FLOAT":
            case "REAL":
                return LogicalTypeRoot.FLOAT;
            case "DOUBLE":
                return LogicalTypeRoot.DOUBLE;
            case "DECIMAL":
                return LogicalTypeRoot.DECIMAL;
            case "BOOLEAN":
                return LogicalTypeRoot.BOOLEAN;
            case "CHAR":
                return LogicalTypeRoot.CHAR;
            case "VARCHAR":
                return LogicalTypeRoot.VARCHAR;
            case "BINARY":
                return LogicalTypeRoot.BINARY;
            case "VARBINARY":
                return LogicalTypeRoot.VARBINARY;
            case "DATE":
                return LogicalTypeRoot.DATE;
            case "TIME":
                return LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE;
            case "TIMESTAMP":
                return LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE;
            default:
                return null;
        }
    }

    protected static org.apache.flink.table.types.logical.LogicalType expressionLogicalType(Object expression) {
        if (!hasNoArgMethod(expression, "getType")) {
            return null;
        }
        Object type = invoke(expression, "getType");
        boolean nullable = (boolean) invoke(type, "isNullable");
        int precision = (int) invoke(type, "getPrecision");
        LogicalTypeRoot root = expressionTypeRoot(expression);
        if (root == null) {
            return null;
        }
        switch (root) {
            case TINYINT:
                return new org.apache.flink.table.types.logical.TinyIntType(nullable);
            case SMALLINT:
                return new org.apache.flink.table.types.logical.SmallIntType(nullable);
            case INTEGER:
                return new org.apache.flink.table.types.logical.IntType(nullable);
            case BIGINT:
                return new org.apache.flink.table.types.logical.BigIntType(nullable);
            case FLOAT:
                return new org.apache.flink.table.types.logical.FloatType(nullable);
            case DOUBLE:
                return new org.apache.flink.table.types.logical.DoubleType(nullable);
            case DECIMAL:
                return new DecimalType(nullable, precision, (int) invoke(type, "getScale"));
            case BOOLEAN:
                return new org.apache.flink.table.types.logical.BooleanType(nullable);
            case CHAR:
                return new CharType(nullable, precision);
            case VARCHAR:
                return new org.apache.flink.table.types.logical.VarCharType(nullable, precision);
            case BINARY:
                return new BinaryType(nullable, precision);
            case VARBINARY:
                return new org.apache.flink.table.types.logical.VarBinaryType(nullable, precision);
            case DATE:
                return new org.apache.flink.table.types.logical.DateType(nullable);
            case TIME_WITHOUT_TIME_ZONE:
                return new TimeType(nullable, precision);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return new TimestampType(nullable, precision);
            default:
                return null;
        }
    }

    protected static TruthTestOperator truthTestOperator(String kind) {
        switch (kind) {
            case "IS_TRUE":
                return TruthTestOperator.TRUTH_TEST_OPERATOR_IS_TRUE;
            case "IS_FALSE":
                return TruthTestOperator.TRUTH_TEST_OPERATOR_IS_FALSE;
            case "IS_NOT_TRUE":
                return TruthTestOperator.TRUTH_TEST_OPERATOR_IS_NOT_TRUE;
            case "IS_NOT_FALSE":
                return TruthTestOperator.TRUTH_TEST_OPERATOR_IS_NOT_FALSE;
            default:
                return null;
        }
    }

    protected static StreamFusionCondition condition(Object condition, RowType inputType) {
        if (condition == null) {
            return null;
        }
        Boolean booleanLiteral = literal(condition, Boolean.class);
        if (booleanLiteral != null) {
            return new StreamFusionBooleanLiteralCondition(booleanLiteral);
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
        if ("LIKE".equals(functionName(condition))) {
            List<?> operands = (List<?>) invoke(condition, "getOperands");
            if (operands.size() != 2) {
                return null;
            }
            int inputIndex = inputIndex(operands.get(0));
            String pattern = literal(operands.get(1), String.class);
            if (inputIndex < 0
                    || inputIndex >= inputType.getFieldCount()
                    || inputType.getTypeAt(inputIndex).getTypeRoot() != LogicalTypeRoot.VARCHAR
                    || pattern == null
                    || pattern.indexOf('\\') >= 0) {
                return null;
            }
            return new StreamFusionLikeCondition(
                    inputIndex,
                    pattern,
                    StreamFusionIdentityCalcOperator.inputReference(
                            inputIndex, StreamFusionIdentityCalcOperator.logicalType(inputType, inputIndex)));
        }
        if ("STARTSWITH".equals(functionName(condition)) || "STARTS_WITH".equals(functionName(condition))) {
            List<?> operands = (List<?>) invoke(condition, "getOperands");
            if (operands.size() != 2) {
                return null;
            }
            int inputIndex = inputIndex(operands.get(0));
            String prefix = literal(operands.get(1), String.class);
            if (inputIndex < 0
                    || inputIndex >= inputType.getFieldCount()
                    || inputType.getTypeAt(inputIndex).getTypeRoot() != LogicalTypeRoot.VARCHAR
                    || prefix == null) {
                return null;
            }
            return new StreamFusionStartsWithCondition(
                    inputIndex,
                    prefix,
                    StreamFusionIdentityCalcOperator.inputReference(
                            inputIndex, StreamFusionIdentityCalcOperator.logicalType(inputType, inputIndex)));
        }
        String kind = invoke(condition, "getKind").toString();
        if ("SEARCH".equals(kind)) {
            return search(condition, inputType);
        }
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
        if ("IS_TRUE".equals(kind)
                || "IS_FALSE".equals(kind)
                || "IS_NOT_TRUE".equals(kind)
                || "IS_NOT_FALSE".equals(kind)) {
            List<?> operands = (List<?>) invoke(condition, "getOperands");
            if (operands.size() != 1) {
                return null;
            }
            StreamFusionCondition operand = condition(operands.get(0), inputType);
            return operand == null ? null : StreamFusionTruthTestCondition.create(kind, operand);
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
}
