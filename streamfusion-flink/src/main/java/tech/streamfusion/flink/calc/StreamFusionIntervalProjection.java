/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.calc;

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.proto.plan.v1.Expression;
import tech.streamfusion.proto.plan.v1.IntegerLiteral;
import tech.streamfusion.proto.plan.v1.LongLiteral;

/** Flink stores interval output columns as months or milliseconds, not Arrow temporal intervals. */
final class StreamFusionIntervalProjection extends StreamFusionProjectionTranslator {
    private StreamFusionIntervalProjection() {}

    static Expression materialize(Object expression, RowType inputType, LogicalType expectedType) {
        if (unsupportedReason(expression, expectedType) != null) return null;
        if (expectedType.getTypeRoot() == LogicalTypeRoot.INTERVAL_YEAR_MONTH) {
            Integer months = integerLiteral(expression);
            if (months != null)
                return Expression.newBuilder()
                        .setIntegerLiteral(IntegerLiteral.newBuilder().setValue(months))
                        .build();
        }
        if (expectedType.getTypeRoot() == LogicalTypeRoot.INTERVAL_DAY_TIME) {
            Long millis = longLiteral(expression);
            if (millis != null)
                return Expression.newBuilder()
                        .setLongLiteral(LongLiteral.newBuilder().setValue(millis))
                        .build();
        }
        // Temporal arithmetic still lowers its internal interval operands through the ordinary
        // expression translator. This adaptation applies only to materialized operator columns.
        return projectionExpression(expression, inputType, expectedType);
    }

    static String unsupportedReason(Object expression, LogicalType type) {
        if (!containsInterval(type) || inputIndex(expression) >= 0 || isNullLiteral(expression)) return null;
        if (type.getTypeRoot() == LogicalTypeRoot.INTERVAL_YEAR_MONTH && integerLiteral(expression) != null)
            return null;
        if (type.getTypeRoot() == LogicalTypeRoot.INTERVAL_DAY_TIME && longLiteral(expression) != null) return null;
        return "computed interval output requires Flink's months/milliseconds storage; "
                + "native materialization currently supports only input references, literals and nulls";
    }

    private static boolean containsInterval(LogicalType type) {
        return type.getTypeRoot() == LogicalTypeRoot.INTERVAL_YEAR_MONTH
                || type.getTypeRoot() == LogicalTypeRoot.INTERVAL_DAY_TIME
                || type.getChildren().stream().anyMatch(StreamFusionIntervalProjection::containsInterval);
    }
}
