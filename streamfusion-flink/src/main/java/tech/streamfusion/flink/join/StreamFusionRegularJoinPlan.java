/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.join;

import java.util.List;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.calc.StreamFusionCalcTranslator;
import tech.streamfusion.flink.proto.FlinkLogicalTypeProto;
import tech.streamfusion.proto.plan.v1.Field;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;
import tech.streamfusion.proto.plan.v1.RegularJoin;
import tech.streamfusion.proto.plan.v1.RegularJoinType;
import tech.streamfusion.proto.plan.v1.Schema;

/** Builds the versioned native regular streaming join state contract. */
final class StreamFusionRegularJoinPlan {
    private StreamFusionRegularJoinPlan() {}

    static byte[] create(
            RowType leftType,
            RowType rightType,
            int[] leftKeys,
            int[] rightKeys,
            boolean[] filterNulls,
            FlinkJoinType joinType,
            Object residualCondition) {
        return create(leftType, rightType, leftKeys, rightKeys, filterNulls, joinType, residualCondition, false);
    }

    static byte[] createBounded(
            RowType leftType,
            RowType rightType,
            int[] leftKeys,
            int[] rightKeys,
            boolean[] filterNulls,
            FlinkJoinType joinType,
            Object residualCondition) {
        return create(leftType, rightType, leftKeys, rightKeys, filterNulls, joinType, residualCondition, true);
    }

    static byte[] createBoundedWithOutputCalcs(
            RowType leftType,
            RowType rightType,
            int[] leftKeys,
            int[] rightKeys,
            boolean[] filterNulls,
            FlinkJoinType joinType,
            Object residualCondition,
            List<RowType> calcInputTypes,
            List<RowType> calcOutputTypes,
            List<List<?>> calcProjections,
            List<?> calcConditions) {
        Operator join = createOperator(
                leftType, rightType, leftKeys, rightKeys, filterNulls, joinType, residualCondition, true);
        Operator root = StreamFusionCalcTranslator.appendChangelogCalcOperators(
                join, calcInputTypes, calcOutputTypes, calcProjections, calcConditions);
        if (root == null) {
            return null;
        }
        return nativePlan(root);
    }

    static byte[] createWithOutputCalcs(
            RowType leftType,
            RowType rightType,
            int[] leftKeys,
            int[] rightKeys,
            boolean[] filterNulls,
            FlinkJoinType joinType,
            Object residualCondition,
            List<RowType> calcInputTypes,
            List<RowType> calcOutputTypes,
            List<List<?>> calcProjections,
            List<?> calcConditions) {
        Operator join = createOperator(
                leftType, rightType, leftKeys, rightKeys, filterNulls, joinType, residualCondition, false);
        Operator root = StreamFusionCalcTranslator.appendChangelogCalcOperators(
                join, calcInputTypes, calcOutputTypes, calcProjections, calcConditions);
        return root == null ? null : nativePlan(root);
    }

    private static byte[] create(
            RowType leftType,
            RowType rightType,
            int[] leftKeys,
            int[] rightKeys,
            boolean[] filterNulls,
            FlinkJoinType joinType,
            Object residualCondition,
            boolean boundedFinalOutput) {
        return nativePlan(createOperator(
                leftType,
                rightType,
                leftKeys,
                rightKeys,
                filterNulls,
                joinType,
                residualCondition,
                boundedFinalOutput));
    }

    private static Operator createOperator(
            RowType leftType,
            RowType rightType,
            int[] leftKeys,
            int[] rightKeys,
            boolean[] filterNulls,
            FlinkJoinType joinType,
            Object residualCondition,
            boolean boundedFinalOutput) {
        RegularJoin.Builder join = RegularJoin.newBuilder()
                .setLeftSchema(schema(leftType))
                .setRightSchema(schema(rightType))
                .setJoinType(joinType(joinType))
                .setBoundedFinalOutput(boundedFinalOutput);
        if (residualCondition != null) {
            join.setResidualCondition(StreamFusionCalcTranslator.operatorCondition(
                    residualCondition, conditionInputType(leftType, rightType)));
        }
        for (int key : leftKeys) {
            join.addLeftKeyIndices(key);
        }
        for (int key : rightKeys) {
            join.addRightKeyIndices(key);
        }
        for (boolean filter : filterNulls) {
            join.addFilterNulls(filter);
        }
        return Operator.newBuilder().setRegularJoin(join).build();
    }

    private static byte[] nativePlan(Operator root) {
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(root)
                .build()
                .toByteArray();
    }

    static RowType conditionInputType(RowType leftType, RowType rightType) {
        java.util.List<org.apache.flink.table.types.logical.LogicalType> types =
                new java.util.ArrayList<>(leftType.getFieldCount() + rightType.getFieldCount());
        types.addAll(leftType.getChildren());
        types.addAll(rightType.getChildren());
        String[] names = java.util.stream.IntStream.range(0, types.size())
                .mapToObj(index -> "join_field_" + index)
                .toArray(String[]::new);
        return RowType.of(types.toArray(new org.apache.flink.table.types.logical.LogicalType[0]), names);
    }

    private static RegularJoinType joinType(FlinkJoinType type) {
        switch (type) {
            case INNER:
                return RegularJoinType.REGULAR_JOIN_TYPE_INNER;
            case LEFT:
                return RegularJoinType.REGULAR_JOIN_TYPE_LEFT;
            case RIGHT:
                return RegularJoinType.REGULAR_JOIN_TYPE_RIGHT;
            case FULL:
                return RegularJoinType.REGULAR_JOIN_TYPE_FULL;
            case SEMI:
                return RegularJoinType.REGULAR_JOIN_TYPE_SEMI;
            case ANTI:
                return RegularJoinType.REGULAR_JOIN_TYPE_ANTI;
            default:
                throw new IllegalArgumentException("Unsupported regular join type " + type);
        }
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
