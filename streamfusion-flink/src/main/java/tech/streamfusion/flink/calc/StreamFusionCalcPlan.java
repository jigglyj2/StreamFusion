/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.calc;

import java.util.List;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.proto.plan.v1.ArrayUnnest;
import tech.streamfusion.proto.plan.v1.Calc;
import tech.streamfusion.proto.plan.v1.Expression;
import tech.streamfusion.proto.plan.v1.Input;
import tech.streamfusion.proto.plan.v1.InputReference;
import tech.streamfusion.proto.plan.v1.LogicalType;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;
import tech.streamfusion.proto.plan.v1.ReplicateRows;
import tech.streamfusion.proto.plan.v1.UnnestCollection;

/** Builds protobuf plans for Arrow-native Calc and fused UNNEST/Calc execution. */
final class StreamFusionCalcPlan {
    private StreamFusionCalcPlan() {}

    static byte[] create(RowType inputType, List<List<Expression>> projectionStages, List<Expression> conditions) {
        Operator input = Operator.newBuilder().setInput(Input.newBuilder()).build();
        return appendCalcs(input, inputType.getFieldCount(), projectionStages, conditions);
    }

    static byte[] createFusedUnnest(
            List<Integer> indexes,
            List<Boolean> withOrdinalities,
            List<Boolean> preserveEmpty,
            List<UnnestCollection> collections,
            List<Expression> collectionExpressions,
            List<Integer> outputFieldCounts,
            List<List<Expression>> projectionStages,
            List<Expression> conditions) {
        int stages = indexes.size();
        if (stages == 0
                || withOrdinalities.size() != stages
                || preserveEmpty.size() != stages
                || collections.size() != stages
                || collectionExpressions.size() != stages
                || outputFieldCounts.size() != stages) {
            throw new IllegalArgumentException("A fused UNNEST chain must contain equally sized, non-empty stages");
        }
        Operator operator = Operator.newBuilder().setInput(Input.newBuilder()).build();
        for (int stage = 0; stage < stages; stage++) {
            ArrayUnnest.Builder unnest = ArrayUnnest.newBuilder()
                    .setInput(operator)
                    .setArrayIndex(indexes.get(stage))
                    .setWithOrdinality(withOrdinalities.get(stage))
                    .setPreserveEmpty(preserveEmpty.get(stage))
                    .setCollection(collections.get(stage));
            Expression expression = collectionExpressions.get(stage);
            if (expression != null) {
                unnest.setCollectionExpression(expression);
            }
            operator = Operator.newBuilder().setArrayUnnest(unnest).build();
        }
        return appendCalcs(operator, outputFieldCounts.get(stages - 1), projectionStages, conditions);
    }

    static byte[] createFusedReplicateRows(
            Expression repetition,
            List<Expression> values,
            int replicateOutputFieldCount,
            List<List<Expression>> projectionStages,
            List<Expression> conditions) {
        Operator input = Operator.newBuilder().setInput(Input.newBuilder()).build();
        Operator replicate = Operator.newBuilder()
                .setReplicateRows(ReplicateRows.newBuilder()
                        .setInput(input)
                        .setRepetition(repetition)
                        .addAllValues(values))
                .build();
        return appendCalcs(replicate, replicateOutputFieldCount, projectionStages, conditions);
    }

    private static byte[] appendCalcs(
            Operator input, int inputFieldCount, List<List<Expression>> projectionStages, List<Expression> conditions) {
        if (projectionStages.size() != conditions.size()) {
            throw new IllegalArgumentException("Calc projections and conditions must have the same stage count");
        }
        Operator operator = input;
        int stageInputFieldCount = inputFieldCount;
        for (int stage = 0; stage < projectionStages.size(); stage++) {
            List<Expression> projections = projectionStages.get(stage);
            Calc.Builder calc = Calc.newBuilder().setInput(operator).addAllProjections(projections);
            calc.addProjections(inputReference(
                    stageInputFieldCount, logicalType(new org.apache.flink.table.types.logical.IntType(false))));
            Expression condition = conditions.get(stage);
            if (condition != null) {
                calc.setCondition(condition);
            }
            operator = Operator.newBuilder().setCalc(calc).build();
            stageInputFieldCount = projections.size();
        }
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(operator)
                .build()
                .toByteArray();
    }

    static LogicalType logicalType(RowType inputType, int inputIndex) {
        return tech.streamfusion.flink.proto.FlinkLogicalTypeProto.serialize(inputType.getTypeAt(inputIndex));
    }

    static LogicalType logicalType(org.apache.flink.table.types.logical.LogicalType flinkType) {
        return tech.streamfusion.flink.proto.FlinkLogicalTypeProto.serialize(flinkType);
    }

    static Expression inputReference(int inputIndex, LogicalType type) {
        return Expression.newBuilder()
                .setInputReference(
                        InputReference.newBuilder().setIndex(inputIndex).setType(type))
                .build();
    }
}
