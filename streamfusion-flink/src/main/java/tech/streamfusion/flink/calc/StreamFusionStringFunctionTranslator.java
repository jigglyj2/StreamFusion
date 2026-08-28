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
import tech.streamfusion.proto.plan.v1.Expression;
import tech.streamfusion.proto.plan.v1.StringRepeat;
import tech.streamfusion.proto.plan.v1.StringReplace;

/** String scalar functions whose operands can remain ordinary native expressions. */
final class StreamFusionStringFunctionTranslator extends StreamFusionComplexTypeSupport {
    private StreamFusionStringFunctionTranslator() {}

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
}
