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
import tech.streamfusion.proto.plan.v1.JsonQuote;

/** Translates JSON string transformations with Flink-specific escaping semantics. */
final class StreamFusionJsonFunctionTranslator extends StreamFusionComplexTypeSupport {
    private StreamFusionJsonFunctionTranslator() {}

    static String failureReason(Object expression) {
        return "JSON_UNQUOTE".equals(functionName(expression))
                ? "JSON_UNQUOTE stays on Flink because its pass-through behavior depends on Flink's shaded Jackson JSON validator and has not been proven identical to a native validator"
                : null;
    }

    static Expression translate(Object expression, RowType inputType, LogicalType expectedType) {
        if (!"JSON_QUOTE".equals(functionName(expression)) || expectedType.getTypeRoot() != LogicalTypeRoot.VARCHAR) {
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
        return operand == null
                ? null
                : Expression.newBuilder()
                        .setJsonQuote(JsonQuote.newBuilder().setOperand(operand))
                        .build();
    }
}
