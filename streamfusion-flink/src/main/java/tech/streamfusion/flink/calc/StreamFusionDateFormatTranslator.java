/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.calc;

import java.util.List;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import tech.streamfusion.proto.plan.v1.DateFormat;
import tech.streamfusion.proto.plan.v1.Expression;

/** The numeric, timezone-free Java DateTimeFormatter contract, including BCE and large years. */
final class StreamFusionDateFormatTranslator extends StreamFusionComplexTypeSupport {
    private StreamFusionDateFormatTranslator() {}

    static Expression translate(Object expression, RowType inputType, LogicalType expectedType) {
        if (!"DATE_FORMAT".equals(functionName(expression))
                || expectedType.getTypeRoot() != LogicalTypeRoot.VARCHAR
                || failureReason(expression, inputType) != null) return null;
        List<?> operands = (List<?>) invoke(expression, "getOperands");
        Expression operand = StreamFusionProjectionTranslator.projectionExpression(
                operands.get(0), inputType, logicalType(operands.get(0), inputType));
        if (operand == null) return null;
        return Expression.newBuilder()
                .setDateFormat(
                        DateFormat.newBuilder().setOperand(operand).setPattern(literal(operands.get(1), String.class)))
                .build();
    }

    static String failureReason(Object expression, RowType inputType) {
        if (!"DATE_FORMAT".equals(functionName(expression))) return null;
        List<?> operands = (List<?>) invoke(expression, "getOperands");
        if (operands.size() != 2) return "DATE_FORMAT requires one timestamp and one literal pattern";
        LogicalType type = logicalType(operands.get(0), inputType);
        if (!(type instanceof TimestampType)
                || ((TimestampType) type).getPrecision() != 3
                || ((TimestampType) type).getKind() == org.apache.flink.table.types.logical.TimestampKind.PROCTIME)
            return "DATE_FORMAT requires timezone-free TIMESTAMP(3); string parsing, other precisions and session-zone semantics stay on Flink";
        String pattern = literal(operands.get(1), String.class);
        if (pattern == null || !supportedPattern(pattern))
            return "DATE_FORMAT requires numeric literal patterns using yyyy, MM, dd, HH, mm, ss, SSS and quoted literals; dynamic patterns, locale names, zones and other Java pattern fields stay on Flink";
        return null;
    }

    static boolean supportedPattern(String pattern) {
        boolean quoted = false;
        for (int i = 0; i < pattern.length(); ) {
            char c = pattern.charAt(i++);
            if (c == '\'') {
                if (i < pattern.length() && pattern.charAt(i) == '\'') i++;
                else quoted = !quoted;
            } else if (!quoted && ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z'))) {
                int start = i - 1;
                while (i < pattern.length() && pattern.charAt(i) == c) i++;
                String token = pattern.substring(start, i);
                if (!List.of("yyyy", "MM", "dd", "HH", "mm", "ss", "SSS").contains(token)) return false;
            } else if (!quoted && (c == '[' || c == ']' || c == '{' || c == '}' || c == '#')) return false;
        }
        return !quoted;
    }
}
