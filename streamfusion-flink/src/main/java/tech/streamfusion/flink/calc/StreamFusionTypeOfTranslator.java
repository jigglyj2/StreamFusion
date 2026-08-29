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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.proto.plan.v1.Expression;
import tech.streamfusion.proto.plan.v1.StringLiteral;

/** Specializes Flink's type-inspection function into a native constant expression. */
final class StreamFusionTypeOfTranslator extends StreamFusionComplexTypeSupport {
    private StreamFusionTypeOfTranslator() {}

    static Expression translate(Object expression, RowType inputType, LogicalType expectedType) {
        if (!"TYPEOF".equals(functionName(expression))
                || expectedType.getTypeRoot() != LogicalTypeRoot.VARCHAR
                || !hasNoArgMethod(expression, "getOperands")) {
            return null;
        }
        List<?> operands = (List<?>) invoke(expression, "getOperands");
        if (operands.size() < 1 || operands.size() > 2) {
            return null;
        }
        boolean forceSerializable = false;
        if (operands.size() == 2) {
            Boolean force = literal(operands.get(1), Boolean.class);
            if (force == null) {
                return null;
            }
            forceSerializable = force;
        }
        LogicalType inspectedType = inspectedType(operands.get(0), inputType);
        if (inspectedType == null) {
            return null;
        }
        final String typeString;
        try {
            typeString = forceSerializable ? inspectedType.asSerializableString() : inspectedType.asSummaryString();
        } catch (RuntimeException ignored) {
            return null;
        }
        return Expression.newBuilder()
                .setStringLiteral(StringLiteral.newBuilder().setValue(typeString))
                .build();
    }

    private static LogicalType inspectedType(Object operand, RowType inputType) {
        LogicalType resolved = logicalType(operand, inputType);
        if (resolved != null || !hasNoArgMethod(operand, "getType")) {
            return resolved;
        }
        Object calciteType = invoke(operand, "getType");
        return calciteType instanceof RelDataType ? FlinkTypeFactory.toLogicalType((RelDataType) calciteType) : null;
    }
}
