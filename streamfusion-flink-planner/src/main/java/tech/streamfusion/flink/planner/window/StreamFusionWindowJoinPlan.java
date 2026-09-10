/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner.window;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.table.planner.plan.nodes.exec.spec.JoinSpec;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.calc.StreamFusionCalcTranslator;
import tech.streamfusion.flink.proto.FlinkLogicalTypeProto;
import tech.streamfusion.proto.plan.v1.Field;
import tech.streamfusion.proto.plan.v1.Input;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;
import tech.streamfusion.proto.plan.v1.RegularJoinType;
import tech.streamfusion.proto.plan.v1.Schema;
import tech.streamfusion.proto.plan.v1.WindowJoin;

/** Builds legacy state-only and explicit native-compute Window Join contracts. */
public final class StreamFusionWindowJoinPlan {
    private StreamFusionWindowJoinPlan() {}

    public static byte[] createNativeInner(
            RowType leftType,
            RowType rightType,
            JoinSpec spec,
            int leftWindowEnd,
            int rightWindowEnd,
            String shiftTimeZone) {
        if (spec.getJoinType() != FlinkJoinType.INNER) {
            throw new IllegalArgumentException("Native Window Join currently requires INNER semantics");
        }
        if (spec.getLeftKeys().length != spec.getRightKeys().length
                || spec.getLeftKeys().length != spec.getFilterNulls().length) {
            throw new IllegalArgumentException("Window Join key and null-filter counts differ");
        }
        WindowJoin.Builder join = WindowJoin.newBuilder()
                .setLeftInput(Operator.newBuilder().setInput(Input.newBuilder().setInputIndex(0)))
                .setRightInput(Operator.newBuilder().setInput(Input.newBuilder().setInputIndex(1)))
                .setLeftSchema(schema(leftType))
                .setRightSchema(schema(rightType))
                .setLeftWindowEndIndex(leftWindowEnd)
                .setRightWindowEndIndex(rightWindowEnd)
                .setShiftTimeZone(shiftTimeZone)
                .setJoinType(RegularJoinType.REGULAR_JOIN_TYPE_INNER);
        for (int key : spec.getLeftKeys()) join.addLeftKeyIndices(key);
        for (int key : spec.getRightKeys()) join.addRightKeyIndices(key);
        for (boolean filter : spec.getFilterNulls()) join.addFilterNulls(filter);
        spec.getNonEquiCondition()
                .ifPresent(condition -> join.setResidualCondition(StreamFusionCalcTranslator.operatorCondition(
                        condition, conditionInputType(leftType, rightType))));
        return NativePlan.newBuilder()
                .setProtocolVersion(3)
                .setRoot(Operator.newBuilder().setWindowJoin(join).setClearRecordTimestamps(true))
                .build()
                .toByteArray();
    }

    static RowType conditionInputType(RowType left, RowType right) {
        List<LogicalType> types = new ArrayList<>(left.getChildren());
        types.addAll(right.getChildren());
        String[] names = java.util.stream.IntStream.range(0, types.size())
                .mapToObj(i -> "window_join_field_" + i)
                .toArray(String[]::new);
        return RowType.of(types.toArray(new LogicalType[0]), names);
    }

    public static byte[] create(
            RowType leftType,
            RowType rightType,
            int[] leftKeys,
            int[] rightKeys,
            int leftWindowEnd,
            int rightWindowEnd,
            String shiftTimeZone) {
        WindowJoin.Builder join = WindowJoin.newBuilder()
                .setLeftWindowEndIndex(leftWindowEnd)
                .setRightWindowEndIndex(rightWindowEnd)
                .setLeftSchema(schema(leftType))
                .setRightSchema(schema(rightType))
                .setShiftTimeZone(shiftTimeZone);
        for (int key : leftKeys) {
            join.addLeftKeyIndices(key);
        }
        for (int key : rightKeys) {
            join.addRightKeyIndices(key);
        }
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setWindowJoin(join))
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
