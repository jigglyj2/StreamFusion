/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.join;

import java.util.List;
import java.util.Map;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.stream.keyselector.AttributeBasedJoinKeyExtractor.ConditionAttributeRef;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.proto.FlinkLogicalTypeProto;
import tech.streamfusion.proto.plan.v1.Field;
import tech.streamfusion.proto.plan.v1.MultiJoin;
import tech.streamfusion.proto.plan.v1.MultiJoinEquiCondition;
import tech.streamfusion.proto.plan.v1.MultiJoinInput;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;
import tech.streamfusion.proto.plan.v1.RegularJoinType;
import tech.streamfusion.proto.plan.v1.Schema;

/** Builds the versioned native N-input streaming join state contract. */
final class StreamFusionMultiJoinPlan {
    private StreamFusionMultiJoinPlan() {}

    static byte[] create(
            List<RowType> inputTypes,
            List<int[]> commonKeyIndices,
            List<FlinkJoinType> joinTypes,
            Map<Integer, List<ConditionAttributeRef>> joinAttributeMap,
            long[] stateRetentionMillis) {
        if (inputTypes.size() != commonKeyIndices.size()
                || inputTypes.size() != joinTypes.size()
                || inputTypes.size() != stateRetentionMillis.length) {
            throw new IllegalArgumentException("Multi-join plan inputs have incompatible sizes");
        }
        MultiJoin.Builder join = MultiJoin.newBuilder();
        for (int input = 0; input < inputTypes.size(); input++) {
            MultiJoinInput.Builder inputPlan = MultiJoinInput.newBuilder()
                    .setSchema(schema(inputTypes.get(input)))
                    .setStateRetentionMillis(stateRetentionMillis[input]);
            for (int key : commonKeyIndices.get(input)) {
                inputPlan.addCommonKeyIndices(key);
            }
            join.addInputs(inputPlan).addJoinTypes(joinType(joinTypes.get(input)));
        }
        joinAttributeMap.forEach((depth, conditions) -> {
            // Flink retains a bookkeeping entry for input zero. No native predicate can be
            // evaluated there because there is no preceding input.
            if (depth > 0) {
                conditions.forEach(condition -> join.addEquiConditions(MultiJoinEquiCondition.newBuilder()
                        .setDepth(depth)
                        .setLeftInputIndex(condition.leftInputId)
                        .setLeftFieldIndex(condition.leftFieldIndex)
                        .setRightFieldIndex(condition.rightFieldIndex)));
            }
        });
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setMultiJoin(join))
                .build()
                .toByteArray();
    }

    private static RegularJoinType joinType(FlinkJoinType type) {
        switch (type) {
            case INNER:
                return RegularJoinType.REGULAR_JOIN_TYPE_INNER;
            case LEFT:
                return RegularJoinType.REGULAR_JOIN_TYPE_LEFT;
            default:
                throw new IllegalArgumentException("Unsupported multi-join type " + type);
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
