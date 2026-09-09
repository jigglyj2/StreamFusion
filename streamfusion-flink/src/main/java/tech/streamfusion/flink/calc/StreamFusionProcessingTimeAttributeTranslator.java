/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.calc;

import java.util.List;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TimestampKind;
import tech.streamfusion.flink.proto.FlinkLogicalTypeProto;
import tech.streamfusion.proto.plan.v1.Expression;
import tech.streamfusion.proto.plan.v1.NullLiteral;

/** Flink ExprCodeGenerator's logical PROCTIME slot; this expression never reads a clock. */
final class StreamFusionProcessingTimeAttributeTranslator extends StreamFusionRexSupport {
    private StreamFusionProcessingTimeAttributeTranslator() {}

    static Expression translate(Object expression, LogicalType expectedType) {
        if (!"PROCTIME".equals(functionName(expression))
                || !((List<?>) invoke(expression, "getOperands")).isEmpty()
                || !(expectedType instanceof LocalZonedTimestampType)) return null;
        var time = (LocalZonedTimestampType) expectedType;
        if (time.getKind() != TimestampKind.PROCTIME || time.getPrecision() != 3) return null;
        return Expression.newBuilder()
                .setNullLiteral(NullLiteral.newBuilder().setType(FlinkLogicalTypeProto.serialize(time.copy(true))))
                .build();
    }
}
