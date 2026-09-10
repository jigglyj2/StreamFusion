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

import java.util.List;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.proto.plan.v1.Expression;
import tech.streamfusion.proto.plan.v1.RegexExtract;

/** Explicit parity decisions for expressions backed by Java's regular-expression engine. */
final class StreamFusionRegexFunctionTranslator extends StreamFusionComplexTypeSupport {
    private StreamFusionRegexFunctionTranslator() {}

    static String failureReason(Object expression) {
        String function = functionName(expression);
        if ("REGEXP_EXTRACT".equals(function)) {
            List<?> operands = (List<?>) invoke(expression, "getOperands");
            if (operands.size() != 2 && operands.size() != 3)
                return "REGEXP_EXTRACT requires a value, literal pattern and optional literal INTEGER capture index";
            LogicalType valueType = StreamFusionExpressionTranslator.expressionLogicalType(operands.get(0));
            if (valueType == null || valueType.getTypeRoot() != LogicalTypeRoot.VARCHAR)
                return "REGEXP_EXTRACT requires VARCHAR input; fixed-width CHAR stays on Flink";
            if (capturePattern(operands) == null)
                return "REGEXP_EXTRACT stays on Flink outside the restricted Java Pattern syntax: require a non-null ASCII literal pattern of at most 256 characters, nesting at most 32, and an in-range literal INTEGER capture index; flags, escapes, end anchors, dot, repeated groups, set operations, look-around and backreferences stay on Flink";
            return null;
        }
        if (function != null && (function.startsWith("REGEXP") || function.contains("SIMILAR"))) {
            return function
                    + " stays on Flink because Flink uses Java Pattern syntax, matching, capture, and replacement semantics; Rust/DataFusion regex deliberately excludes constructs such as look-around and backreferences and is not byte-parity compatible";
        }
        return null;
    }

    static Expression translate(Object expression, RowType inputType, LogicalType expectedType) {
        if (!"REGEXP_EXTRACT".equals(functionName(expression))
                || expectedType.getTypeRoot() != LogicalTypeRoot.VARCHAR
                || failureReason(expression) != null) return null;
        List<?> operands = (List<?>) invoke(expression, "getOperands");
        Expression value = StreamFusionProjectionTranslator.projectionExpression(
                operands.get(0), inputType, logicalType(operands.get(0), inputType));
        if (value == null) return null;
        return Expression.newBuilder()
                .setRegexExtract(
                        RegexExtract.newBuilder().setOperand(value).setCapturePattern(capturePattern(operands)))
                .build();
    }

    private static String capturePattern(List<?> operands) {
        int group = 0;
        if (operands.size() == 3) {
            LogicalType type = StreamFusionExpressionTranslator.expressionLogicalType(operands.get(2));
            if (type == null || type.getTypeRoot() != LogicalTypeRoot.INTEGER) return null;
            Integer literalGroup = literal(operands.get(2), Integer.class);
            if (literalGroup == null) return null;
            group = literalGroup;
        }
        return StreamFusionRegexCapturePattern.project(literal(operands.get(1), String.class), group);
    }
}
