/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.over;

import java.util.List;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.nodes.exec.spec.OverSpec;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.calc.StreamFusionCalcTranslator;
import tech.streamfusion.flink.proto.FlinkLogicalTypeProto;
import tech.streamfusion.proto.plan.v1.AggregateFunction;
import tech.streamfusion.proto.plan.v1.Field;
import tech.streamfusion.proto.plan.v1.Input;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;
import tech.streamfusion.proto.plan.v1.OverAggregate;
import tech.streamfusion.proto.plan.v1.OverTimeAttribute;
import tech.streamfusion.proto.plan.v1.Schema;

/** Builds the versioned protobuf contract for native streaming OVER aggregation. */
final class StreamFusionOverAggregatePlan {
    private StreamFusionOverAggregatePlan() {}

    static byte[] create(RowType inputType, RowType outputType, OverSpec spec, long stateTtl) {
        OverSpec.GroupSpec group = spec.getGroups().get(0);
        OverAggregate.Builder aggregate = OverAggregate.newBuilder()
                .setInput(Operator.newBuilder().setInput(Input.newBuilder()))
                .setOrderKeyIndex(group.getSort().getFieldIndices()[0])
                .setRowsFrame(group.isRows())
                .setTimeAttribute(OverTimeAttribute.OVER_TIME_ATTRIBUTE_NON_TIME)
                .setInputSchema(schema(inputType))
                .setOutputSchema(schema(outputType))
                .setInputChangelog(true)
                .setStateTtlMillis(stateTtl)
                .setSortAscending(group.getSort().getAscendingOrders()[0])
                .setSortNullsLast(group.getSort().getNullsIsLast()[0]);
        for (int key : spec.getPartition().getFieldIndices()) {
            aggregate.addPartitionKeyIndices(key);
        }
        for (AggregateCall call : group.getAggCalls()) {
            aggregate.addAggregateCalls(call(call, inputType));
        }
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setOverAggregate(aggregate))
                .build()
                .toByteArray();
    }

    private static tech.streamfusion.proto.plan.v1.AggregateCall call(AggregateCall call, RowType inputType) {
        tech.streamfusion.proto.plan.v1.AggregateCall.Builder nativeCall =
                tech.streamfusion.proto.plan.v1.AggregateCall.newBuilder()
                        .setFunction(function(call))
                        .setOutputType(StreamFusionCalcTranslator.operatorLogicalType(
                                FlinkTypeFactory.toLogicalType(call.getType())))
                        .setRetractable(true);
        List<Integer> arguments = call.getArgList();
        if (!arguments.isEmpty()) {
            int inputIndex = arguments.get(0);
            nativeCall
                    .setInputIndex(inputIndex)
                    .setInputType(StreamFusionCalcTranslator.operatorLogicalType(inputType.getTypeAt(inputIndex)));
        }
        return nativeCall.build();
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
        throw new IllegalArgumentException("Unsupported native OVER aggregate " + call);
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
