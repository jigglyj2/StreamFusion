/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.window;

import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.proto.FlinkLogicalTypeProto;
import tech.streamfusion.proto.plan.v1.Field;
import tech.streamfusion.proto.plan.v1.Input;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;
import tech.streamfusion.proto.plan.v1.Schema;
import tech.streamfusion.proto.plan.v1.WindowTableFunction;

/** Builds an Arrow-native aligned Window TVF protobuf plan. */
final class StreamFusionWindowTableFunctionPlan {
    private StreamFusionWindowTableFunctionPlan() {}

    static byte[] create(
            RowType inputType,
            int timeAttributeIndex,
            int[] partitionKeys,
            boolean processingTime,
            String shiftTimeZone,
            StreamFusionWindowTableFunctionTranslator.WindowParameters parameters) {
        return create(
                Operator.newBuilder().setInput(Input.newBuilder()).build(),
                inputType,
                timeAttributeIndex,
                partitionKeys,
                processingTime,
                shiftTimeZone,
                parameters);
    }

    static byte[] create(
            Operator input,
            RowType inputType,
            int timeAttributeIndex,
            int[] partitionKeys,
            boolean processingTime,
            String shiftTimeZone,
            StreamFusionWindowTableFunctionTranslator.WindowParameters parameters) {
        WindowTableFunction.Builder window = WindowTableFunction.newBuilder()
                .setInput(input)
                .setTimeAttributeIndex(timeAttributeIndex)
                .setKind(parameters.kind)
                .setSizeMillis(parameters.sizeMillis)
                .setSlideOrStepMillis(parameters.slideOrStepMillis)
                .setOffsetMillis(parameters.offsetMillis)
                .setProcessingTime(processingTime)
                .setInputSchema(schema(inputType))
                .setShiftTimeZone(shiftTimeZone);
        for (int key : partitionKeys) {
            window.addPartitionKeyIndices(key);
        }
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setWindowTableFunction(window))
                .build()
                .toByteArray();
    }

    private static Schema schema(RowType type) {
        Schema.Builder schema = Schema.newBuilder();
        for (RowType.RowField field : type.getFields()) {
            schema.addFields(Field.newBuilder()
                    .setName(field.getName())
                    .setType(FlinkLogicalTypeProto.serialize(field.getType())));
        }
        return schema.build();
    }
}
