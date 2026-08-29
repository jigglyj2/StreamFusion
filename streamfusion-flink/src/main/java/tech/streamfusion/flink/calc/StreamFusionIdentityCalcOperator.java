/*
 * Copyright 2026 StreamFusion Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package tech.streamfusion.flink.calc;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import tech.streamfusion.flink.arrow.ArrowCDataBridge;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.NativeCalcResult;
import tech.streamfusion.proto.plan.v1.ArrayUnnest;
import tech.streamfusion.proto.plan.v1.Calc;
import tech.streamfusion.proto.plan.v1.Expression;
import tech.streamfusion.proto.plan.v1.Input;
import tech.streamfusion.proto.plan.v1.InputReference;
import tech.streamfusion.proto.plan.v1.LogicalType;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;
import tech.streamfusion.proto.plan.v1.UnnestCollection;

/** Initial bounded, vectorized identity calc for one non-null INT column. */
final class StreamFusionIdentityCalcOperator extends AbstractStreamOperator<RowData>
        implements OneInputStreamOperator<RowData, RowData>, BoundedOneInput {
    private static final int BATCH_SIZE = 1024;
    private final RowType inputType;
    private final RowType outputType;
    private final RowDataSerializer serializer;
    private final byte[] serializedPlan;
    private final List<RowData> rows = new ArrayList<>(BATCH_SIZE);
    private final List<RowKind> rowKinds = new ArrayList<>(BATCH_SIZE);

    StreamFusionIdentityCalcOperator(
            RowType inputType,
            RowType outputType,
            List<List<Expression>> projectionStages,
            List<Expression> conditions) {
        this.inputType = inputType;
        this.outputType = outputType;
        this.serializer = new RowDataSerializer(inputType);
        this.serializedPlan = createPlan(inputType, projectionStages, conditions);
    }

    StreamFusionIdentityCalcOperator(
            RowType boundaryInputType,
            RowType outputType,
            int arrayUnnestIndex,
            boolean withOrdinality,
            boolean preserveEmpty,
            UnnestCollection collection,
            Expression collectionExpression,
            int unnestOutputFieldCount,
            List<List<Expression>> projectionStages,
            List<Expression> conditions) {
        this(
                boundaryInputType,
                outputType,
                java.util.Collections.singletonList(arrayUnnestIndex),
                java.util.Collections.singletonList(withOrdinality),
                java.util.Collections.singletonList(preserveEmpty),
                java.util.Collections.singletonList(collection),
                java.util.Collections.singletonList(collectionExpression),
                java.util.Collections.singletonList(unnestOutputFieldCount),
                projectionStages,
                conditions);
    }

    StreamFusionIdentityCalcOperator(
            RowType boundaryInputType,
            RowType outputType,
            List<Integer> arrayUnnestIndexes,
            List<Boolean> withOrdinalities,
            List<Boolean> preserveEmpty,
            List<UnnestCollection> collections,
            List<Expression> collectionExpressions,
            List<Integer> unnestOutputFieldCounts,
            List<List<Expression>> projectionStages,
            List<Expression> conditions) {
        this.inputType = boundaryInputType;
        this.outputType = outputType;
        this.serializer = new RowDataSerializer(boundaryInputType);
        this.serializedPlan = createPlan(
                arrayUnnestIndexes,
                withOrdinalities,
                preserveEmpty,
                collections,
                collectionExpressions,
                unnestOutputFieldCounts,
                projectionStages,
                conditions);
    }

    @Override
    public void processElement(StreamRecord<RowData> element) {
        RowData row = element.getValue();
        rows.add(serializer.copy(row));
        rowKinds.add(row.getRowKind());
        if (rows.size() == BATCH_SIZE) {
            flushBatch();
        }
    }

    @Override
    public void endInput() {
        flushBatch();
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) {
        flushBatch();
    }

    private void flushBatch() {
        if (rows.isEmpty()) {
            return;
        }
        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                ArrowRowDataBatch inputBatch = ArrowRowDataBatch.transpose(rows, inputType, allocator);
                NativeCalcResult nativeResult =
                        ArrowCDataBridge.executeWithSelection(serializedPlan, inputBatch, outputType, allocator)) {
            ArrowRowDataBatch outputBatch = nativeResult.batch();
            for (int index = 0; index < outputBatch.size(); index++) {
                RowData row = outputBatch.rowView(index);
                int inputRow = nativeResult.inputRow(index);
                if (inputRow < 0 || inputRow >= rowKinds.size()) {
                    throw new IllegalStateException("Native calc returned invalid input-row ordinal " + inputRow);
                }
                row.setRowKind(rowKinds.get(inputRow));
                output.collect(new StreamRecord<>(row));
            }
        }
        rows.clear();
        rowKinds.clear();
    }

    static byte[] createPlan(RowType inputType, List<List<Expression>> projectionStages, List<Expression> conditions) {
        Operator operator = Operator.newBuilder().setInput(Input.newBuilder()).build();
        int stageInputFieldCount = inputType.getFieldCount();
        return createPlan(operator, stageInputFieldCount, projectionStages, conditions);
    }

    private static byte[] createPlan(
            List<Integer> arrayUnnestIndexes,
            List<Boolean> withOrdinalities,
            List<Boolean> preserveEmpty,
            List<UnnestCollection> collections,
            List<Expression> collectionExpressions,
            List<Integer> unnestOutputFieldCounts,
            List<List<Expression>> projectionStages,
            List<Expression> conditions) {
        int stageCount = arrayUnnestIndexes.size();
        if (stageCount == 0
                || withOrdinalities.size() != stageCount
                || preserveEmpty.size() != stageCount
                || collections.size() != stageCount
                || collectionExpressions.size() != stageCount
                || unnestOutputFieldCounts.size() != stageCount) {
            throw new IllegalArgumentException("A fused UNNEST chain must contain equally sized, non-empty stages");
        }
        Operator operator = Operator.newBuilder().setInput(Input.newBuilder()).build();
        for (int stage = 0; stage < stageCount; stage++) {
            ArrayUnnest.Builder unnest = ArrayUnnest.newBuilder()
                    .setInput(operator)
                    .setArrayIndex(arrayUnnestIndexes.get(stage))
                    .setWithOrdinality(withOrdinalities.get(stage))
                    .setPreserveEmpty(preserveEmpty.get(stage))
                    .setCollection(collections.get(stage));
            Expression expression = collectionExpressions.get(stage);
            if (expression != null) {
                unnest.setCollectionExpression(expression);
            }
            operator = Operator.newBuilder().setArrayUnnest(unnest).build();
        }
        return createPlan(operator, unnestOutputFieldCounts.get(stageCount - 1), projectionStages, conditions);
    }

    private static byte[] createPlan(
            Operator initialOperator,
            int initialFieldCount,
            List<List<Expression>> projectionStages,
            List<Expression> conditions) {
        Operator operator = initialOperator;
        int stageInputFieldCount = initialFieldCount;
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
