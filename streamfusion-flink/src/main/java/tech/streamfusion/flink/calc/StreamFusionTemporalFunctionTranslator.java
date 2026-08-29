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
                || !supports(field, operandType.getTypeRoot())) {
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

    static String failureReason(Object expression, RowType inputType) {
        if (!"EXTRACT".equals(functionName(expression)) || !hasNoArgMethod(expression, "getOperands")) {
            return null;
        }
        List<?> operands = (List<?>) invoke(expression, "getOperands");
        if (operands.size() != 2) {
            return "EXTRACT planner shape is not parity-approved";
        }
        LogicalType operandType = logicalType(operands.get(1), inputType);
        if (operandType == null) {
            return "EXTRACT operand type could not be resolved safely";
        }
        LogicalTypeRoot root = operandType.getTypeRoot();
        if (root == LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE
                || root == LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
            return "timestamp EXTRACT stays on Flink until session-zone and subsecond precision semantics are parity-proven";
        }
        if (root == LogicalTypeRoot.INTERVAL_DAY_TIME || root == LogicalTypeRoot.INTERVAL_YEAR_MONTH) {
            return "interval EXTRACT stays on Flink until signed interval field decomposition is parity-proven";
        }
        if (root == LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE) {
            return "TIME EXTRACT field "
                    + fieldName(operands.get(0))
                    + " is outside Flink 2.3's accelerated hour/minute/second/millisecond contract";
        }
        if (root == LogicalTypeRoot.DATE) {
            return "DATE EXTRACT field "
                    + fieldName(operands.get(0))
                    + " stays on Flink until BCE and year-zero calendar conventions are parity-proven";
        }
        return "EXTRACT operand type " + root + " is not parity-approved";
    }

    private static String fieldName(Object expression) {
        return hasNoArgMethod(expression, "getValue") && invoke(expression, "getValue") != null
                ? invoke(expression, "getValue").toString()
                : "UNKNOWN";
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
            case "DAY":
                return TemporalExtractField.TEMPORAL_EXTRACT_FIELD_DAY;
            case "DOY":
                return TemporalExtractField.TEMPORAL_EXTRACT_FIELD_DAY_OF_YEAR;
            case "DOW":
                return TemporalExtractField.TEMPORAL_EXTRACT_FIELD_DAY_OF_WEEK;
            case "ISODOW":
                return TemporalExtractField.TEMPORAL_EXTRACT_FIELD_ISO_DAY_OF_WEEK;
            case "ISOYEAR":
                return TemporalExtractField.TEMPORAL_EXTRACT_FIELD_ISO_YEAR;
            case "HOUR":
                return TemporalExtractField.TEMPORAL_EXTRACT_FIELD_HOUR;
            case "MINUTE":
                return TemporalExtractField.TEMPORAL_EXTRACT_FIELD_MINUTE;
            case "SECOND":
                return TemporalExtractField.TEMPORAL_EXTRACT_FIELD_SECOND;
            case "EPOCH":
                return TemporalExtractField.TEMPORAL_EXTRACT_FIELD_EPOCH;
            case "MILLISECOND":
                return TemporalExtractField.TEMPORAL_EXTRACT_FIELD_MILLISECOND;
            default:
                return TemporalExtractField.TEMPORAL_EXTRACT_FIELD_UNSPECIFIED;
        }
    }

    private static boolean supports(TemporalExtractField field, LogicalTypeRoot operandType) {
        switch (field) {
            case TEMPORAL_EXTRACT_FIELD_HOUR:
            case TEMPORAL_EXTRACT_FIELD_MINUTE:
            case TEMPORAL_EXTRACT_FIELD_SECOND:
            case TEMPORAL_EXTRACT_FIELD_MILLISECOND:
                return operandType == LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE;
            default:
                return operandType == LogicalTypeRoot.DATE;
        }
    }
}
