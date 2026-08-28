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

import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.proto.plan.v1.ArrayDistinct;
import tech.streamfusion.proto.plan.v1.Expression;

/** Array set functions kept separate from collection transformation and search functions. */
final class StreamFusionCollectionSetTranslator extends StreamFusionComplexTypeSupport {
    private StreamFusionCollectionSetTranslator() {}

    static Expression arrayDistinct(Object expression, RowType inputType, LogicalType expectedType) {
        if (!"ARRAY_DISTINCT".equals(functionName(expression)) || !(expectedType instanceof ArrayType)) {
            return null;
        }
        java.util.List<?> operands = (java.util.List<?>) invoke(expression, "getOperands");
        if (operands.size() != 1) {
            return null;
        }
        LogicalType operandType = logicalType(operands.get(0), inputType);
        if (!(operandType instanceof ArrayType)) {
            return null;
        }
        Expression array =
                StreamFusionProjectionTranslator.projectionExpression(operands.get(0), inputType, operandType);
        return array == null
                ? null
                : Expression.newBuilder()
                        .setArrayDistinct(ArrayDistinct.newBuilder().setArray(array))
                        .build();
    }
}
