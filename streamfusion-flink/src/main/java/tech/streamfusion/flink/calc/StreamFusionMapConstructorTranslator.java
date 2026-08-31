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

import java.util.HashSet;
import java.util.Set;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.proto.plan.v1.Expression;
import tech.streamfusion.proto.plan.v1.MapConstructor;

/** Translates the duplicate-free literal-key subset of Flink's {@code MAP[...]} constructor. */
final class StreamFusionMapConstructorTranslator extends StreamFusionRexSupport {
    private StreamFusionMapConstructorTranslator() {}

    static Expression translate(Object expression, RowType inputType, LogicalType expectedType) {
        if (!isMapConstructor(expression) || !(expectedType instanceof MapType)) {
            return null;
        }
        java.util.List<?> operands = (java.util.List<?>) invoke(expression, "getOperands");
        if (operands.isEmpty() || operands.size() % 2 != 0 || !hasUniqueLiteralKeys(operands)) {
            return null;
        }
        MapType mapType = (MapType) expectedType;
        MapConstructor.Builder constructor = MapConstructor.newBuilder();
        for (int index = 0; index < operands.size(); index += 2) {
            Expression key = StreamFusionProjectionTranslator.projectionExpression(
                    operands.get(index), inputType, mapType.getKeyType());
            Expression value = StreamFusionProjectionTranslator.projectionExpression(
                    operands.get(index + 1), inputType, mapType.getValueType());
            if (key == null || value == null) {
                return null;
            }
            constructor.addKeys(key).addValues(value);
        }
        return Expression.newBuilder().setMapConstructor(constructor).build();
    }

    static String failureReason(Object expression) {
        if (!isMapConstructor(expression)) {
            return null;
        }
        java.util.List<?> operands = (java.util.List<?>) invoke(expression, "getOperands");
        if (operands.isEmpty()) {
            return "empty MAP constructors are rejected by Flink 2.3 SQL validation before physical planning";
        }
        return hasUniqueLiteralKeys(operands)
                ? null
                : "MAP constructors stay on Flink unless every key is a unique non-null literal because Flink keeps the last duplicate key while DataFusion rejects duplicates";
    }

    private static boolean hasUniqueLiteralKeys(java.util.List<?> operands) {
        Set<Object> keys = new HashSet<>();
        for (int index = 0; index < operands.size(); index += 2) {
            Object key = operands.get(index);
            if (!"RexLiteral".equals(key.getClass().getSimpleName())) {
                return false;
            }
            Object value = invoke(key, "getValue");
            if (value == null || !keys.add(value)) {
                return false;
            }
        }
        return true;
    }

    private static boolean isMapConstructor(Object expression) {
        return hasNoArgMethod(expression, "getKind")
                && "MAP_VALUE_CONSTRUCTOR".equals(invoke(expression, "getKind").toString());
    }
}
