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
import tech.streamfusion.proto.plan.v1.TemporalExtract;
import tech.streamfusion.proto.plan.v1.TemporalExtractField;

/** Translates timezone-free calendar expressions into native temporal kernels. */
final class StreamFusionTemporalFunctionTranslator extends StreamFusionComplexTypeSupport {
    private StreamFusionTemporalFunctionTranslator() {}

    static Expression extract(Object expression, RowType inputType, LogicalType expectedType) {
        if (!"EXTRACT".equals(functionName(expression)) || !hasNoArgMethod(expression, "getOperands")) {
            return null;
        }
        List<?> operands = (List<?>) invoke(expression, "getOperands");
        if (operands.size() != 2
                || (expectedType.getTypeRoot() != LogicalTypeRoot.INTEGER
                        && expectedType.getTypeRoot() != LogicalTypeRoot.BIGINT)) {
            return null;
        }
        TemporalExtractField field = field(operands.get(0));
        LogicalType operandType = logicalType(operands.get(1), inputType);
        if (field == TemporalExtractField.TEMPORAL_EXTRACT_FIELD_UNSPECIFIED
                || operandType == null
                || operandType.getTypeRoot() != LogicalTypeRoot.DATE) {
            return null;
        }
        Expression operand =
                StreamFusionProjectionTranslator.projectionExpression(operands.get(1), inputType, operandType);
        return operand == null
                ? null
                : Expression.newBuilder()
                        .setTemporalExtract(TemporalExtract.newBuilder()
                                .setOperand(operand)
                                .setField(field)
                                .setResultIsBigint(expectedType.getTypeRoot() == LogicalTypeRoot.BIGINT))
                        .build();
    }

    private static TemporalExtractField field(Object expression) {
        if (!hasNoArgMethod(expression, "getValue")) {
            return TemporalExtractField.TEMPORAL_EXTRACT_FIELD_UNSPECIFIED;
        }
        Object value = invoke(expression, "getValue");
        if (value == null) {
            return TemporalExtractField.TEMPORAL_EXTRACT_FIELD_UNSPECIFIED;
        }
        switch (value.toString()) {
            case "YEAR":
                return TemporalExtractField.TEMPORAL_EXTRACT_FIELD_YEAR;
            case "QUARTER":
                return TemporalExtractField.TEMPORAL_EXTRACT_FIELD_QUARTER;
            case "MONTH":
                return TemporalExtractField.TEMPORAL_EXTRACT_FIELD_MONTH;
            case "WEEK":
                return TemporalExtractField.TEMPORAL_EXTRACT_FIELD_WEEK;
            default:
                return TemporalExtractField.TEMPORAL_EXTRACT_FIELD_UNSPECIFIED;
        }
    }
}
