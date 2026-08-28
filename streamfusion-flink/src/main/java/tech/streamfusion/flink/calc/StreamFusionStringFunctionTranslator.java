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

import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.proto.plan.v1.CharacterFromCode;
import tech.streamfusion.proto.plan.v1.Expression;
import tech.streamfusion.proto.plan.v1.StringAscii;
import tech.streamfusion.proto.plan.v1.StringInitCap;
import tech.streamfusion.proto.plan.v1.StringLeft;
import tech.streamfusion.proto.plan.v1.StringPosition;
import tech.streamfusion.proto.plan.v1.StringRepeat;
import tech.streamfusion.proto.plan.v1.StringReplace;
import tech.streamfusion.proto.plan.v1.StringReverse;
import tech.streamfusion.proto.plan.v1.StringRight;

/** String scalar functions whose operands can remain ordinary native expressions. */
final class StreamFusionStringFunctionTranslator extends StreamFusionComplexTypeSupport {
    private StreamFusionStringFunctionTranslator() {}

    static String failureReason(Object expression) {
        String function = functionName(expression);
        if ("LPAD".equals(function) || "RPAD".equals(function)) {
            return function
                    + " stays on Flink because Flink measures and truncates UTF-16 code units while DataFusion measures Unicode code points; truncating a supplementary character can also create a value Arrow UTF-8 cannot represent";
        }
        if ("OVERLAY".equals(function)) {
            return "OVERLAY stays on Flink because Flink indexes UTF-16 code units and can produce an unpaired surrogate that Arrow UTF-8 cannot represent";
        }
        return null;
    }

    static Expression replace(Object expression, RowType inputType, LogicalType expectedType) {
        if (!"REPLACE".equals(functionName(expression)) || expectedType.getTypeRoot() != LogicalTypeRoot.VARCHAR) {
            return null;
        }
        java.util.List<?> operands = (java.util.List<?>) invoke(expression, "getOperands");
        if (operands.size() != 3) {
            return null;
        }
        StringReplace.Builder replace = StringReplace.newBuilder();
        for (int index = 0; index < operands.size(); index++) {
            Expression operand =
                    StreamFusionProjectionTranslator.projectionExpression(operands.get(index), inputType, expectedType);
            if (operand == null) {
                return null;
            }
            if (index == 0) {
                replace.setValue(operand);
            } else if (index == 1) {
                replace.setSearch(operand);
            } else {
                replace.setReplacement(operand);
            }
        }
        return Expression.newBuilder().setStringReplace(replace).build();
    }

    static Expression repeat(Object expression, RowType inputType, LogicalType expectedType) {
        if (!"REPEAT".equals(functionName(expression)) || expectedType.getTypeRoot() != LogicalTypeRoot.VARCHAR) {
            return null;
        }
        java.util.List<?> operands = (java.util.List<?>) invoke(expression, "getOperands");
        if (operands.size() != 2) {
            return null;
        }
        Expression value =
                StreamFusionProjectionTranslator.projectionExpression(operands.get(0), inputType, expectedType);
        Expression count =
                StreamFusionProjectionTranslator.projectionExpression(operands.get(1), inputType, new IntType());
        if (value == null || count == null) {
            return null;
        }
        return Expression.newBuilder()
                .setStringRepeat(StringRepeat.newBuilder().setValue(value).setCount(count))
                .build();
    }

    static Expression position(Object expression, RowType inputType, LogicalType expectedType) {
        if (!"POSITION".equals(functionName(expression)) || expectedType.getTypeRoot() != LogicalTypeRoot.INTEGER) {
            return null;
        }
        java.util.List<?> operands = (java.util.List<?>) invoke(expression, "getOperands");
        if (operands.size() != 2) {
            return null;
        }
        org.apache.flink.table.types.logical.VarCharType stringType =
                new org.apache.flink.table.types.logical.VarCharType(
                        org.apache.flink.table.types.logical.VarCharType.MAX_LENGTH);
        Expression needle =
                StreamFusionProjectionTranslator.projectionExpression(operands.get(0), inputType, stringType);
        Expression haystack =
                StreamFusionProjectionTranslator.projectionExpression(operands.get(1), inputType, stringType);
        if (needle == null || haystack == null) {
            return null;
        }
        return Expression.newBuilder()
                .setStringPosition(StringPosition.newBuilder().setNeedle(needle).setHaystack(haystack))
                .build();
    }

    static Expression ascii(Object expression, RowType inputType, LogicalType expectedType) {
        if (!"ASCII".equals(functionName(expression)) || expectedType.getTypeRoot() != LogicalTypeRoot.INTEGER) {
            return null;
        }
        java.util.List<?> operands = (java.util.List<?>) invoke(expression, "getOperands");
        if (operands.size() != 1) {
            return null;
        }
        org.apache.flink.table.types.logical.VarCharType stringType =
                new org.apache.flink.table.types.logical.VarCharType(
                        org.apache.flink.table.types.logical.VarCharType.MAX_LENGTH);
        Expression value =
                StreamFusionProjectionTranslator.projectionExpression(operands.get(0), inputType, stringType);
        return value == null
                ? null
                : Expression.newBuilder()
                        .setStringAscii(StringAscii.newBuilder().setValue(value))
                        .build();
    }

    static Expression chr(Object expression, RowType inputType, LogicalType expectedType) {
        if (!"CHR".equals(functionName(expression))
                || (expectedType.getTypeRoot() != LogicalTypeRoot.CHAR
                        && expectedType.getTypeRoot() != LogicalTypeRoot.VARCHAR)) {
            return null;
        }
        java.util.List<?> operands = (java.util.List<?>) invoke(expression, "getOperands");
        if (operands.size() != 1) {
            return null;
        }
        LogicalType operandType = logicalType(operands.get(0), inputType);
        if (operandType == null || !supportsChr(operandType.getTypeRoot())) {
            return null;
        }
        Expression operand =
                StreamFusionProjectionTranslator.projectionExpression(operands.get(0), inputType, operandType);
        return operand == null
                ? null
                : Expression.newBuilder()
                        .setChr(CharacterFromCode.newBuilder().setOperand(operand))
                        .build();
    }

    static Expression reverse(Object expression, RowType inputType, LogicalType expectedType) {
        if (!"REVERSE".equals(functionName(expression)) || expectedType.getTypeRoot() != LogicalTypeRoot.VARCHAR) {
            return null;
        }
        java.util.List<?> operands = (java.util.List<?>) invoke(expression, "getOperands");
        if (operands.size() != 1) {
            return null;
        }
        LogicalType operandType = logicalType(operands.get(0), inputType);
        if (operandType == null || operandType.getTypeRoot() != LogicalTypeRoot.VARCHAR) {
            return null;
        }
        Expression operand =
                StreamFusionProjectionTranslator.projectionExpression(operands.get(0), inputType, operandType);
        return operand == null
                ? null
                : Expression.newBuilder()
                        .setStringReverse(StringReverse.newBuilder().setOperand(operand))
                        .build();
    }

    static Expression initCap(Object expression, RowType inputType, LogicalType expectedType) {
        if (!"INITCAP".equals(functionName(expression)) || expectedType.getTypeRoot() != LogicalTypeRoot.VARCHAR) {
            return null;
        }
        java.util.List<?> operands = (java.util.List<?>) invoke(expression, "getOperands");
        if (operands.size() != 1) {
            return null;
        }
        LogicalType operandType = logicalType(operands.get(0), inputType);
        if (operandType == null || operandType.getTypeRoot() != LogicalTypeRoot.VARCHAR) {
            return null;
        }
        Expression operand =
                StreamFusionProjectionTranslator.projectionExpression(operands.get(0), inputType, operandType);
        return operand == null
                ? null
                : Expression.newBuilder()
                        .setStringInitCap(StringInitCap.newBuilder().setOperand(operand))
                        .build();
    }

    static Expression edge(Object expression, RowType inputType, LogicalType expectedType) {
        String function = functionName(expression);
        if (!("LEFT".equals(function) || "RIGHT".equals(function))
                || expectedType.getTypeRoot() != LogicalTypeRoot.VARCHAR) {
            return null;
        }
        java.util.List<?> operands = (java.util.List<?>) invoke(expression, "getOperands");
        if (operands.size() != 2) {
            return null;
        }
        LogicalType valueType = logicalType(operands.get(0), inputType);
        LogicalType countType = logicalType(operands.get(1), inputType);
        if (valueType == null
                || valueType.getTypeRoot() != LogicalTypeRoot.VARCHAR
                || countType == null
                || countType.getTypeRoot() != LogicalTypeRoot.INTEGER) {
            return null;
        }
        Expression value = StreamFusionProjectionTranslator.projectionExpression(operands.get(0), inputType, valueType);
        Expression count = StreamFusionProjectionTranslator.projectionExpression(operands.get(1), inputType, countType);
        if (value == null || count == null) {
            return null;
        }
        return "LEFT".equals(function)
                ? Expression.newBuilder()
                        .setStringLeft(StringLeft.newBuilder().setValue(value).setCount(count))
                        .build()
                : Expression.newBuilder()
                        .setStringRight(StringRight.newBuilder().setValue(value).setCount(count))
                        .build();
    }

    private static boolean supportsChr(LogicalTypeRoot type) {
        return type == LogicalTypeRoot.TINYINT
                || type == LogicalTypeRoot.SMALLINT
                || type == LogicalTypeRoot.INTEGER
                || type == LogicalTypeRoot.BIGINT;
    }
}
