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
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeMerging;
import tech.streamfusion.flink.calc.StreamFusionCalcTranslator;
import tech.streamfusion.flink.proto.FlinkLogicalTypeProto;
import tech.streamfusion.proto.plan.v1.AggregateFunction;
import tech.streamfusion.proto.plan.v1.Field;
import tech.streamfusion.proto.plan.v1.Input;
import tech.streamfusion.proto.plan.v1.LocalWindowAggregate;
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
            int attachedWindowStartIndex,
            int attachedWindowEndIndex,
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
        if (attachedWindowStartIndex >= 0) {
            aggregate.setAttachedWindowStartIndex(attachedWindowStartIndex);
            aggregate.setAttachedWindowEndIndex(attachedWindowEndIndex);
        }
        for (int index : grouping) {
            aggregate.addGroupingIndices(index);
        }
        addCalls(aggregate, inputType, calls, needRetraction);
        for (NamedWindowProperty property : properties) {
            aggregate.addWindowProperties(property(property));
        }
        return nativePlan(Operator.newBuilder().setWindowAggregate(aggregate));
    }

    static byte[] createLocal(
            RowType inputType,
            RowType outputType,
            int[] grouping,
            AggregateCall[] calls,
            boolean inputChangelog,
            boolean needRetraction,
            StreamFusionWindowTableFunctionTranslator.WindowParameters window,
            int timeAttributeIndex,
            int attachedWindowStartIndex,
            int attachedWindowEndIndex,
            String shiftTimeZone) {
        LocalWindowAggregate.Builder aggregate = LocalWindowAggregate.newBuilder()
                .setInput(Operator.newBuilder().setInput(Input.newBuilder()))
                .setInputChangelog(inputChangelog)
                .setTimeAttributeIndex(timeAttributeIndex)
                .setKind(window.kind)
                .setSizeMillis(window.sizeMillis)
                .setSlideOrStepMillis(window.slideOrStepMillis)
                .setOffsetMillis(window.offsetMillis)
                .setShiftTimeZone(shiftTimeZone)
                .setInputSchema(schema(inputType))
                .setOutputSchema(schema(outputType));
        if (attachedWindowStartIndex >= 0) {
            aggregate.setAttachedWindowStartIndex(attachedWindowStartIndex);
            aggregate.setAttachedWindowEndIndex(attachedWindowEndIndex);
        }
        for (int index : grouping) {
            aggregate.addGroupingIndices(index);
        }
        for (tech.streamfusion.proto.plan.v1.AggregateCall call : aggregateCalls(inputType, calls, needRetraction)) {
            aggregate.addAggregateCalls(call);
        }
        return nativePlan(Operator.newBuilder().setLocalWindowAggregate(aggregate));
    }

    static byte[] createGlobal(
            RowType originalInputType,
            RowType internalInputType,
            RowType outputType,
            int groupingCount,
            AggregateCall[] calls,
            boolean needRetraction,
            StreamFusionWindowTableFunctionTranslator.WindowParameters window,
            boolean partialWindowsAreSlices,
            String shiftTimeZone,
            NamedWindowProperty[] properties) {
        WindowAggregate.Builder aggregate = WindowAggregate.newBuilder()
                .setInput(Operator.newBuilder().setInput(Input.newBuilder()))
                .setKind(window.kind)
                .setSizeMillis(window.sizeMillis)
                .setSlideOrStepMillis(window.slideOrStepMillis)
                .setOffsetMillis(window.offsetMillis)
                .setShiftTimeZone(shiftTimeZone)
                .setInputSchema(schema(internalInputType))
                .setOutputSchema(schema(outputType))
                .setPartialAccumulatorIndex(groupingCount)
                .setPartialWindowStartIndex(groupingCount + 1)
                .setPartialSliceEndIndex(groupingCount + 2)
                .setPartialWindowsAreSlices(partialWindowsAreSlices);
        for (int index = 0; index < groupingCount; index++) {
            aggregate.addGroupingIndices(index);
        }
        addCalls(aggregate, originalInputType, calls, needRetraction);
        for (NamedWindowProperty property : properties) {
            aggregate.addWindowProperties(property(property));
        }
        return nativePlan(Operator.newBuilder().setWindowAggregate(aggregate));
    }

    private static void addCalls(
            WindowAggregate.Builder aggregate, RowType inputType, AggregateCall[] calls, boolean needRetraction) {
        aggregate.addAllAggregateCalls(aggregateCalls(inputType, calls, needRetraction));
    }

    private static List<tech.streamfusion.proto.plan.v1.AggregateCall> aggregateCalls(
            RowType inputType, AggregateCall[] calls, boolean needRetraction) {
        java.util.ArrayList<tech.streamfusion.proto.plan.v1.AggregateCall> result =
                new java.util.ArrayList<>(calls.length);
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
            if (call.getAggregation().getKind() == SqlKind.AVG) {
                nativeCall.setAccumulatorType(StreamFusionCalcTranslator.operatorLogicalType(
                        averageAccumulatorType(inputType.getTypeAt(arguments.get(0)))));
            }
            result.add(nativeCall.build());
        }
        return result;
    }

    private static byte[] nativePlan(Operator.Builder root) {
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(root)
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
        if (kind == SqlKind.SUM) {
            return AggregateFunction.AGGREGATE_FUNCTION_SUM;
        }
        if (kind == SqlKind.SUM0) {
            return AggregateFunction.AGGREGATE_FUNCTION_SUM0;
        }
        if (kind == SqlKind.MIN) {
            return AggregateFunction.AGGREGATE_FUNCTION_MIN;
        }
        if (kind == SqlKind.MAX) {
            return AggregateFunction.AGGREGATE_FUNCTION_MAX;
        }
        if (kind == SqlKind.AVG) {
            return AggregateFunction.AGGREGATE_FUNCTION_AVG;
        }
        throw new IllegalArgumentException("Unsupported native window aggregate " + call);
    }

    private static LogicalType averageAccumulatorType(LogicalType inputType) {
        LogicalTypeRoot root = inputType.getTypeRoot();
        if (root == LogicalTypeRoot.DECIMAL) {
            return LogicalTypeMerging.findSumAggType(inputType);
        }
        if (root == LogicalTypeRoot.FLOAT || root == LogicalTypeRoot.DOUBLE) {
            return new DoubleType(inputType.isNullable());
        }
        return new BigIntType(inputType.isNullable());
    }
}
