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
import tech.streamfusion.proto.plan.v1.StringConcatWs;
import tech.streamfusion.proto.plan.v1.StringInitCap;
import tech.streamfusion.proto.plan.v1.StringLeft;
import tech.streamfusion.proto.plan.v1.StringPosition;
import tech.streamfusion.proto.plan.v1.StringRepeat;
import tech.streamfusion.proto.plan.v1.StringReplace;
import tech.streamfusion.proto.plan.v1.StringReverse;
import tech.streamfusion.proto.plan.v1.StringRight;
import tech.streamfusion.proto.plan.v1.StringTranslate;
import tech.streamfusion.proto.plan.v1.StringTrim;
import tech.streamfusion.proto.plan.v1.StringTrimDirection;

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
        String function = functionName(expression);
        if (!("POSITION".equals(function) || "INSTR".equals(function) || "LOCATE".equals(function))
                || expectedType.getTypeRoot() != LogicalTypeRoot.INTEGER) {
            return null;
        }
        java.util.List<?> operands = (java.util.List<?>) invoke(expression, "getOperands");
        if (operands.size() != 2) {
            return null;
        }
        org.apache.flink.table.types.logical.VarCharType stringType =
                new org.apache.flink.table.types.logical.VarCharType(
                        org.apache.flink.table.types.logical.VarCharType.MAX_LENGTH);
        int needleIndex = "INSTR".equals(function) ? 1 : 0;
        int haystackIndex = "INSTR".equals(function) ? 0 : 1;
        Expression needle =
                StreamFusionProjectionTranslator.projectionExpression(operands.get(needleIndex), inputType, stringType);
        Expression haystack = StreamFusionProjectionTranslator.projectionExpression(
                operands.get(haystackIndex), inputType, stringType);
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

    static Expression trim(Object expression, RowType inputType, LogicalType expectedType) {
        if (expectedType.getTypeRoot() != LogicalTypeRoot.VARCHAR) {
            return null;
        }
        String function = functionName(expression);
        if (!("TRIM".equals(function) || "LTRIM".equals(function) || "RTRIM".equals(function))) {
            return null;
        }
        java.util.List<?> operands = (java.util.List<?>) invoke(expression, "getOperands");
        Object valueOperand;
        Object charactersOperand = null;
        StringTrimDirection direction;
        if ("TRIM".equals(function) && operands.size() == 1) {
            direction = StringTrimDirection.STRING_TRIM_DIRECTION_BOTH;
            valueOperand = operands.get(0);
        } else if ("TRIM".equals(function) && operands.size() == 3) {
            String flag = invoke(operands.get(0), "getValue").toString();
            direction = trimDirection(flag);
            charactersOperand = operands.get(1);
            valueOperand = operands.get(2);
        } else if (("LTRIM".equals(function) || "RTRIM".equals(function))
                && (operands.size() == 1 || operands.size() == 2)) {
            direction = "LTRIM".equals(function)
                    ? StringTrimDirection.STRING_TRIM_DIRECTION_LEADING
                    : StringTrimDirection.STRING_TRIM_DIRECTION_TRAILING;
            valueOperand = operands.get(0);
            if (operands.size() == 2) {
                charactersOperand = operands.get(1);
            }
        } else {
            return null;
        }
        if (direction == StringTrimDirection.STRING_TRIM_DIRECTION_UNSPECIFIED) {
            return null;
        }
        LogicalType valueType = logicalType(valueOperand, inputType);
        if (valueType == null || valueType.getTypeRoot() != LogicalTypeRoot.VARCHAR) {
            return null;
        }
        Expression value = StreamFusionProjectionTranslator.projectionExpression(valueOperand, inputType, valueType);
        if (value == null) {
            return null;
        }
        StringTrim.Builder trim = StringTrim.newBuilder().setValue(value).setDirection(direction);
        if (charactersOperand != null) {
            LogicalType charactersType = logicalType(charactersOperand, inputType);
            boolean characterLiteral = charactersType != null
                    && charactersType.getTypeRoot() == LogicalTypeRoot.CHAR
                    && literal(charactersOperand, String.class) != null;
            if (charactersType == null
                    || (charactersType.getTypeRoot() != LogicalTypeRoot.VARCHAR && !characterLiteral)) {
                return null;
            }
            LogicalType nativeCharactersType = characterLiteral
                    ? new org.apache.flink.table.types.logical.VarCharType(
                            org.apache.flink.table.types.logical.VarCharType.MAX_LENGTH)
                    : charactersType;
            Expression characters = StreamFusionProjectionTranslator.projectionExpression(
                    charactersOperand, inputType, nativeCharactersType);
            if (characters == null) {
                return null;
            }
            trim.setCharacters(characters);
        }
        return Expression.newBuilder().setStringTrim(trim).build();
    }

    static Expression concatWs(Object expression, RowType inputType, LogicalType expectedType) {
        if (!"CONCAT_WS".equals(functionName(expression)) || expectedType.getTypeRoot() != LogicalTypeRoot.VARCHAR) {
            return null;
        }
        java.util.List<?> operands = (java.util.List<?>) invoke(expression, "getOperands");
        if (operands.size() < 2) {
            return null;
        }
        org.apache.flink.table.types.logical.VarCharType stringType =
                new org.apache.flink.table.types.logical.VarCharType(
                        org.apache.flink.table.types.logical.VarCharType.MAX_LENGTH);
        StringConcatWs.Builder concatWs = StringConcatWs.newBuilder();
        for (int index = 0; index < operands.size(); index++) {
            Object operand = operands.get(index);
            LogicalType operandType = logicalType(operand, inputType);
            boolean characterLiteral = operandType != null
                    && operandType.getTypeRoot() == LogicalTypeRoot.CHAR
                    && literal(operand, String.class) != null;
            if (operandType == null || (operandType.getTypeRoot() != LogicalTypeRoot.VARCHAR && !characterLiteral)) {
                return null;
            }
            Expression argument = StreamFusionProjectionTranslator.projectionExpression(
                    operand, inputType, characterLiteral ? stringType : operandType);
            if (argument == null) {
                return null;
            }
            if (index == 0) {
                concatWs.setSeparator(argument);
            } else {
                concatWs.addValues(argument);
            }
        }
        return Expression.newBuilder().setStringConcatWs(concatWs).build();
    }

    static Expression translate(Object expression, RowType inputType, LogicalType expectedType) {
        String function = functionName(expression);
        if (!("TRANSLATE".equals(function) || "TRANSLATE3".equals(function))
                || expectedType.getTypeRoot() != LogicalTypeRoot.VARCHAR) {
            return null;
        }
        java.util.List<?> operands = (java.util.List<?>) invoke(expression, "getOperands");
        if (operands.size() != 3) {
            return null;
        }
        org.apache.flink.table.types.logical.VarCharType stringType =
                new org.apache.flink.table.types.logical.VarCharType(
                        org.apache.flink.table.types.logical.VarCharType.MAX_LENGTH);
        StringTranslate.Builder translate = StringTranslate.newBuilder();
        for (int index = 0; index < operands.size(); index++) {
            Object operand = operands.get(index);
            LogicalType operandType = logicalType(operand, inputType);
            boolean characterLiteral = operandType != null
                    && operandType.getTypeRoot() == LogicalTypeRoot.CHAR
                    && literal(operand, String.class) != null;
            if (operandType == null || (operandType.getTypeRoot() != LogicalTypeRoot.VARCHAR && !characterLiteral)) {
                return null;
            }
            Expression argument = StreamFusionProjectionTranslator.projectionExpression(
                    operand, inputType, characterLiteral ? stringType : operandType);
            if (argument == null) {
                return null;
            }
            if (index == 0) {
                translate.setValue(argument);
            } else if (index == 1) {
                translate.setSourceCharacters(argument);
            } else {
                translate.setTargetCharacters(argument);
            }
        }
        return Expression.newBuilder().setStringTranslate(translate).build();
    }

    private static StringTrimDirection trimDirection(String flag) {
        String normalized = flag.toUpperCase(java.util.Locale.ROOT);
        if (normalized.contains("BOTH")) {
            return StringTrimDirection.STRING_TRIM_DIRECTION_BOTH;
        }
        if (normalized.contains("LEADING")) {
            return StringTrimDirection.STRING_TRIM_DIRECTION_LEADING;
        }
        if (normalized.contains("TRAILING")) {
            return StringTrimDirection.STRING_TRIM_DIRECTION_TRAILING;
        }
        return StringTrimDirection.STRING_TRIM_DIRECTION_UNSPECIFIED;
    }

    private static boolean supportsChr(LogicalTypeRoot type) {
        return type == LogicalTypeRoot.TINYINT
                || type == LogicalTypeRoot.SMALLINT
                || type == LogicalTypeRoot.INTEGER
                || type == LogicalTypeRoot.BIGINT;
    }
}
