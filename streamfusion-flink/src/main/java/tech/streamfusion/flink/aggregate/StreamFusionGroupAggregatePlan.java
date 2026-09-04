/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.aggregate;

import java.util.List;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
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
import tech.streamfusion.proto.plan.v1.GlobalGroupAggregate;
import tech.streamfusion.proto.plan.v1.GroupAggregate;
import tech.streamfusion.proto.plan.v1.Input;
import tech.streamfusion.proto.plan.v1.LocalGroupAggregate;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;
import tech.streamfusion.proto.plan.v1.Schema;

/** Builds the versioned protobuf contract for native keyed group aggregation. */
final class StreamFusionGroupAggregatePlan {
    private StreamFusionGroupAggregatePlan() {}

    static byte[] create(
            RowType inputType,
            int[] grouping,
            AggregateCall[] calls,
            boolean[] retractable,
            boolean generateUpdateBefore,
            boolean inputChangelog,
            long miniBatchSize,
            RowType outputType) {
        GroupAggregate.Builder aggregate = GroupAggregate.newBuilder()
                .setInput(Operator.newBuilder().setInput(Input.newBuilder()))
                .setGenerateUpdateBefore(generateUpdateBefore)
                .setInputChangelog(inputChangelog)
                .setMiniBatchSize(miniBatchSize)
                .setInputSchema(schema(inputType))
                .setOutputSchema(schema(outputType));
        for (int index : grouping) {
            aggregate.addGroupingIndices(index);
        }
        aggregate.addAllAggregateCalls(aggregateCalls(inputType, calls, retractable));
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setGroupAggregate(aggregate))
                .build()
                .toByteArray();
    }

    static byte[] createLocal(
            RowType inputType,
            RowType internalOutputType,
            int[] grouping,
            AggregateCall[] calls,
            boolean[] retractable,
            boolean inputChangelog,
            long miniBatchSize) {
        LocalGroupAggregate.Builder aggregate = LocalGroupAggregate.newBuilder()
                .setInput(Operator.newBuilder().setInput(Input.newBuilder()))
                .setInputChangelog(inputChangelog)
                .setMiniBatchSize(miniBatchSize)
                .setInputSchema(schema(inputType))
                .setOutputSchema(schema(internalOutputType))
                .addAllAggregateCalls(aggregateCalls(inputType, calls, retractable));
        for (int index : grouping) {
            aggregate.addGroupingIndices(index);
        }
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setLocalGroupAggregate(aggregate))
                .build()
                .toByteArray();
    }

    static byte[] createGlobal(
            RowType originalInputType,
            RowType internalInputType,
            RowType outputType,
            int groupingCount,
            AggregateCall[] calls,
            boolean[] retractable,
            boolean generateUpdateBefore,
            long miniBatchSize) {
        GlobalGroupAggregate.Builder aggregate = GlobalGroupAggregate.newBuilder()
                .setInput(Operator.newBuilder().setInput(Input.newBuilder()))
                .setGenerateUpdateBefore(generateUpdateBefore)
                .setMiniBatchSize(miniBatchSize)
                .setInputSchema(schema(internalInputType))
                .setOutputSchema(schema(outputType))
                .addAllAggregateCalls(aggregateCalls(originalInputType, calls, retractable));
        for (int index = 0; index < groupingCount; index++) {
            aggregate.addGroupingIndices(index);
        }
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setGlobalGroupAggregate(aggregate))
                .build()
                .toByteArray();
    }

    private static List<tech.streamfusion.proto.plan.v1.AggregateCall> aggregateCalls(
            RowType inputType, AggregateCall[] calls, boolean[] retractable) {
        java.util.ArrayList<tech.streamfusion.proto.plan.v1.AggregateCall> result =
                new java.util.ArrayList<>(calls.length);
        for (int index = 0; index < calls.length; index++) {
            AggregateCall call = calls[index];
            tech.streamfusion.proto.plan.v1.AggregateCall.Builder nativeCall =
                    tech.streamfusion.proto.plan.v1.AggregateCall.newBuilder()
                            .setFunction(function(call))
                            .setOutputType(StreamFusionCalcTranslator.operatorLogicalType(
                                    FlinkTypeFactory.toLogicalType(call.getType())))
                            .setRetractable(retractable[index])
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

    private static Schema schema(RowType type) {
        Schema.Builder schema = Schema.newBuilder();
        for (RowType.RowField field : type.getFields()) {
            schema.addFields(Field.newBuilder()
                    .setName(field.getName())
                    .setType(FlinkLogicalTypeProto.serialize(field.getType())));
        }
        return schema.build();
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
        if (kind == SqlKind.AVG) {
            return AggregateFunction.AGGREGATE_FUNCTION_AVG;
        }
        throw new IllegalArgumentException("Unsupported native aggregate " + call);
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
