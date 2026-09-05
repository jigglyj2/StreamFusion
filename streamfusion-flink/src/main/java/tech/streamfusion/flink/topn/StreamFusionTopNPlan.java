/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.topn;

import org.apache.flink.table.planner.plan.nodes.exec.spec.SortSpec;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.proto.FlinkLogicalTypeProto;
import tech.streamfusion.proto.plan.v1.Field;
import tech.streamfusion.proto.plan.v1.Input;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;
import tech.streamfusion.proto.plan.v1.Schema;
import tech.streamfusion.proto.plan.v1.TopN;
import tech.streamfusion.proto.plan.v1.TopNRankType;
import tech.streamfusion.proto.plan.v1.TopNStrategy;

/** Builds the versioned protobuf control contract for native non-window Top-N. */
final class StreamFusionTopNPlan {
    private StreamFusionTopNPlan() {}

    static byte[] create(
            RowType inputType,
            RowType outputType,
            int[] partitionKeys,
            SortSpec sortSpec,
            int[] primaryKeys,
            long rankStart,
            Long rankEnd,
            Integer variableRankEndIndex,
            boolean outputRankNumber,
            boolean generateUpdateBefore,
            StreamFusionTopNStrategy strategy,
            long stateTtlMillis) {
        TopN.Builder topN = TopN.newBuilder()
                .setInput(Operator.newBuilder().setInput(Input.newBuilder()))
                .setRankStart(rankStart)
                .setOutputRankNumber(outputRankNumber)
                .setGenerateUpdateBefore(generateUpdateBefore)
                .setStrategy(toProto(strategy))
                .setRankType(TopNRankType.TOP_N_RANK_TYPE_ROW_NUMBER)
                .setInputSchema(schema(inputType))
                .setOutputSchema(schema(outputType))
                .setStateTtlMillis(stateTtlMillis);
        if (rankEnd != null) {
            topN.setRankEnd(rankEnd);
        } else {
            topN.setVariableRankEndIndex(variableRankEndIndex);
        }
        for (int key : partitionKeys) {
            topN.addPartitionKeyIndices(key);
        }
        for (int index = 0; index < sortSpec.getFieldSize(); index++) {
            topN.addSortKeyIndices(sortSpec.getFieldSpec(index).getFieldIndex());
            topN.addSortAscending(sortSpec.getFieldSpec(index).getIsAscendingOrder());
            topN.addSortNullsLast(sortSpec.getFieldSpec(index).getNullIsLast());
        }
        for (int key : primaryKeys) {
            topN.addPrimaryKeyIndices(key);
        }
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setTopN(topN))
                .build()
                .toByteArray();
    }

    private static TopNStrategy toProto(StreamFusionTopNStrategy strategy) {
        switch (strategy) {
            case APPEND_FAST:
                return TopNStrategy.TOP_N_STRATEGY_APPEND_FAST;
            case UPDATE_FAST:
                return TopNStrategy.TOP_N_STRATEGY_UPDATE_FAST;
            case RETRACT:
                return TopNStrategy.TOP_N_STRATEGY_RETRACT;
            default:
                throw new IllegalArgumentException("Unknown Top-N strategy " + strategy);
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
