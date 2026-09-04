/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.window;

import java.util.List;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.runtime.groupwindow.NamedWindowProperty;
import org.apache.flink.table.runtime.groupwindow.ProctimeAttribute;
import org.apache.flink.table.runtime.groupwindow.RowtimeAttribute;
import org.apache.flink.table.runtime.groupwindow.WindowEnd;
import org.apache.flink.table.runtime.groupwindow.WindowStart;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.calc.StreamFusionCalcTranslator;
import tech.streamfusion.flink.proto.FlinkLogicalTypeProto;
import tech.streamfusion.proto.plan.v1.AggregateFunction;
import tech.streamfusion.proto.plan.v1.Field;
import tech.streamfusion.proto.plan.v1.Input;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;
import tech.streamfusion.proto.plan.v1.Schema;
import tech.streamfusion.proto.plan.v1.WindowAggregate;
import tech.streamfusion.proto.plan.v1.WindowProperty;

/** Builds the versioned protobuf contract for native SQL window aggregation. */
final class StreamFusionWindowAggregatePlan {
    private StreamFusionWindowAggregatePlan() {}

    static byte[] create(
            RowType inputType,
            RowType outputType,
            int[] grouping,
            AggregateCall[] calls,
            boolean inputChangelog,
            boolean needRetraction,
            StreamFusionWindowTableFunctionTranslator.WindowParameters window,
            int timeAttributeIndex,
            boolean processingTime,
            String shiftTimeZone,
            NamedWindowProperty[] properties) {
        WindowAggregate.Builder aggregate = WindowAggregate.newBuilder()
                .setInput(Operator.newBuilder().setInput(Input.newBuilder()))
                .setInputChangelog(inputChangelog)
                .setTimeAttributeIndex(timeAttributeIndex)
                .setKind(window.kind)
                .setSizeMillis(window.sizeMillis)
                .setSlideOrStepMillis(window.slideOrStepMillis)
                .setOffsetMillis(window.offsetMillis)
                .setProcessingTime(processingTime)
                .setShiftTimeZone(shiftTimeZone)
                .setInputSchema(schema(inputType))
                .setOutputSchema(schema(outputType));
        for (int index : grouping) {
            aggregate.addGroupingIndices(index);
        }
        for (AggregateCall call : calls) {
            tech.streamfusion.proto.plan.v1.AggregateCall.Builder nativeCall =
                    tech.streamfusion.proto.plan.v1.AggregateCall.newBuilder()
                            .setFunction(function(call))
                            .setOutputType(StreamFusionCalcTranslator.operatorLogicalType(
                                    FlinkTypeFactory.toLogicalType(call.getType())))
                            .setRetractable(needRetraction)
                            .setDistinct(call.isDistinct());
            List<Integer> arguments = call.getArgList();
            if (!arguments.isEmpty()) {
                int inputIndex = arguments.get(0);
                nativeCall
                        .setInputIndex(inputIndex)
                        .setInputType(StreamFusionCalcTranslator.operatorLogicalType(inputType.getTypeAt(inputIndex)));
            }
            if (call.filterArg >= 0) {
                nativeCall.setFilterIndex(call.filterArg);
            }
            aggregate.addAggregateCalls(nativeCall);
        }
        for (NamedWindowProperty property : properties) {
            aggregate.addWindowProperties(property(property));
        }
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setWindowAggregate(aggregate))
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

    private static WindowProperty property(NamedWindowProperty property) {
        if (property.getProperty() instanceof WindowStart) {
            return WindowProperty.WINDOW_PROPERTY_START;
        }
        if (property.getProperty() instanceof WindowEnd) {
            return WindowProperty.WINDOW_PROPERTY_END;
        }
        if (property.getProperty() instanceof RowtimeAttribute || property.getProperty() instanceof ProctimeAttribute) {
            return WindowProperty.WINDOW_PROPERTY_TIME;
        }
        throw new IllegalArgumentException("Unsupported native window property " + property.getProperty());
    }

    private static AggregateFunction function(AggregateCall call) {
        SqlKind kind = call.getAggregation().getKind();
        if (kind == SqlKind.COUNT) {
            return call.getArgList().isEmpty()
                    ? AggregateFunction.AGGREGATE_FUNCTION_COUNT_STAR
                    : AggregateFunction.AGGREGATE_FUNCTION_COUNT;
        }
        if (kind == SqlKind.SUM || kind == SqlKind.SUM0) {
            return AggregateFunction.AGGREGATE_FUNCTION_SUM;
        }
        if (kind == SqlKind.MIN) {
            return AggregateFunction.AGGREGATE_FUNCTION_MIN;
        }
        if (kind == SqlKind.MAX) {
            return AggregateFunction.AGGREGATE_FUNCTION_MAX;
        }
        throw new IllegalArgumentException("Unsupported native window aggregate " + call);
    }
}
