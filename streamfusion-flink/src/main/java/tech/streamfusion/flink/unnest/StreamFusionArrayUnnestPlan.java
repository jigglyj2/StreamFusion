/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.unnest;

import java.util.List;
import tech.streamfusion.proto.plan.v1.ArrayUnnest;
import tech.streamfusion.proto.plan.v1.Expression;
import tech.streamfusion.proto.plan.v1.Input;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;
import tech.streamfusion.proto.plan.v1.UnnestCollection;

/** Builds nested Arrow-native UNNEST protobuf plans. */
final class StreamFusionArrayUnnestPlan {
    private StreamFusionArrayUnnestPlan() {}

    static byte[] create(
            List<Integer> indexes,
            List<Boolean> withOrdinalities,
            List<Boolean> preserveEmpty,
            List<UnnestCollection> collections,
            List<Expression> collectionExpressions) {
        int stages = indexes.size();
        if (stages == 0
                || withOrdinalities.size() != stages
                || preserveEmpty.size() != stages
                || collections.size() != stages
                || collectionExpressions.size() != stages) {
            throw new IllegalArgumentException("A native UNNEST chain must contain equally sized, non-empty stages");
        }
        Operator root = Operator.newBuilder().setInput(Input.newBuilder()).build();
        for (int stage = 0; stage < stages; stage++) {
            ArrayUnnest.Builder unnest = ArrayUnnest.newBuilder()
                    .setInput(root)
                    .setArrayIndex(indexes.get(stage))
                    .setWithOrdinality(withOrdinalities.get(stage))
                    .setPreserveEmpty(preserveEmpty.get(stage))
                    .setCollection(collections.get(stage));
            Expression expression = collectionExpressions.get(stage);
            if (expression != null) {
                unnest.setCollectionExpression(expression);
            }
            root = Operator.newBuilder().setArrayUnnest(unnest).build();
        }
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(root)
                .build()
                .toByteArray();
    }
}
