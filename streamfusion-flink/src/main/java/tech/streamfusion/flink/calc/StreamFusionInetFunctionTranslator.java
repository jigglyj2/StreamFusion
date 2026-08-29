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
import tech.streamfusion.proto.plan.v1.InetNtoa;

/** Translates IPv4 numeric/string conversion functions. */
final class StreamFusionInetFunctionTranslator extends StreamFusionComplexTypeSupport {
    private StreamFusionInetFunctionTranslator() {}

    static Expression ntoa(Object expression, RowType inputType, LogicalType expectedType) {
        if (!"INET_NTOA".equals(functionName(expression))
                || expectedType.getTypeRoot() != LogicalTypeRoot.VARCHAR
                || !hasNoArgMethod(expression, "getOperands")) {
            return null;
        }
        List<?> operands = (List<?>) invoke(expression, "getOperands");
        if (operands.size() != 1) {
            return null;
        }
        LogicalType operandType = logicalType(operands.get(0), inputType);
        if (operandType == null || !isInteger(operandType.getTypeRoot())) {
            return null;
        }
        Expression operand =
                StreamFusionProjectionTranslator.projectionExpression(operands.get(0), inputType, operandType);
        return operand == null
                ? null
                : Expression.newBuilder()
                        .setInetNtoa(InetNtoa.newBuilder().setOperand(operand))
                        .build();
    }

    private static boolean isInteger(LogicalTypeRoot root) {
        return root == LogicalTypeRoot.TINYINT
                || root == LogicalTypeRoot.SMALLINT
                || root == LogicalTypeRoot.INTEGER
                || root == LogicalTypeRoot.BIGINT;
    }
}
