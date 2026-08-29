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
import tech.streamfusion.proto.plan.v1.ScalarExtremum;

/** Translates parity-approved scalar GREATEST and LEAST expressions. */
final class StreamFusionScalarExtremumTranslator extends StreamFusionComplexTypeSupport {
    private StreamFusionScalarExtremumTranslator() {}

    static Expression translate(Object expression, RowType inputType, LogicalType expectedType) {
        String function = functionName(expression);
        if ((!("GREATEST".equals(function) || "LEAST".equals(function)))
                || !isSupported(expectedType.getTypeRoot())
                || !hasNoArgMethod(expression, "getOperands")) {
            return null;
        }
        List<?> operands = (List<?>) invoke(expression, "getOperands");
        if (operands.isEmpty()) {
            return null;
        }
        ScalarExtremum.Builder extremum = ScalarExtremum.newBuilder().setGreatest("GREATEST".equals(function));
        for (Object operand : operands) {
            Expression argument =
                    StreamFusionProjectionTranslator.projectionExpression(operand, inputType, expectedType);
            if (argument == null) {
                return null;
            }
            extremum.addArguments(argument);
        }
        return Expression.newBuilder().setScalarExtremum(extremum).build();
    }

    static String failureReason(Object expression) {
        String function = functionName(expression);
        if ("GREATEST".equals(function) || "LEAST".equals(function)) {
            return function
                    + " currently accelerates only signed integer and DATE common types; floating NaN/signed-zero, decimal coercion, string ordering, and time/timestamp precision require separate parity audits";
        }
        return null;
    }

    private static boolean isSupported(LogicalTypeRoot root) {
        return root == LogicalTypeRoot.TINYINT
                || root == LogicalTypeRoot.SMALLINT
                || root == LogicalTypeRoot.INTEGER
                || root == LogicalTypeRoot.BIGINT
                || root == LogicalTypeRoot.DATE;
    }
}
