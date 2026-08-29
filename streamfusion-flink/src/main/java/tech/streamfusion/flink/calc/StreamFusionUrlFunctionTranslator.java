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
import tech.streamfusion.proto.plan.v1.UrlDecode;
import tech.streamfusion.proto.plan.v1.UrlEncode;

/** Translates application/x-www-form-urlencoded string functions. */
final class StreamFusionUrlFunctionTranslator extends StreamFusionComplexTypeSupport {
    private StreamFusionUrlFunctionTranslator() {}

    static String failureReason(Object expression) {
        if ("PARSE_URL".equals(functionName(expression))) {
            return "PARSE_URL stays on Flink because URL acceptance, normalization, component extraction, and malformed-input handling are defined by java.net.URL and differ from native URL parsers";
        }
        return null;
    }

    static Expression translate(Object expression, RowType inputType, LogicalType expectedType) {
        String function = functionName(expression);
        if (!("URL_ENCODE".equals(function) || "URL_DECODE".equals(function))
                || expectedType.getTypeRoot() != LogicalTypeRoot.VARCHAR) {
            return null;
        }
        List<?> operands = (List<?>) invoke(expression, "getOperands");
        if (operands.size() != 1) {
            return null;
        }
        LogicalType operandType = logicalType(operands.get(0), inputType);
        if (operandType == null || operandType.getTypeRoot() != LogicalTypeRoot.VARCHAR) {
            return null;
        }
        Expression operand =
                StreamFusionProjectionTranslator.projectionExpression(operands.get(0), inputType, operandType);
        if (operand == null) {
            return null;
        }
        return "URL_ENCODE".equals(function)
                ? Expression.newBuilder()
                        .setUrlEncode(UrlEncode.newBuilder().setOperand(operand))
                        .build()
                : Expression.newBuilder()
                        .setUrlDecode(UrlDecode.newBuilder().setOperand(operand))
                        .build();
    }
}
