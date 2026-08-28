/*
 * Copyright 2026 StreamFusion Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package tech.streamfusion.flink.unnest;

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
import tech.streamfusion.proto.plan.v1.Expression;
import tech.streamfusion.proto.plan.v1.Input;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;
import tech.streamfusion.proto.plan.v1.UnnestCollection;

/** Bounded vectorized execution of an array UNNEST. */
final class StreamFusionArrayUnnestOperator extends AbstractStreamOperator<RowData>
        implements OneInputStreamOperator<RowData, RowData>, BoundedOneInput {
    private static final int BATCH_SIZE = 1024;

    private final RowType inputType;
    private final RowType outputType;
    private final RowDataSerializer serializer;
    private final byte[] serializedPlan;
    private final List<RowData> rows = new ArrayList<>(BATCH_SIZE);
    private final List<RowKind> rowKinds = new ArrayList<>(BATCH_SIZE);

    StreamFusionArrayUnnestOperator(
            RowType inputType,
            RowType outputType,
            int arrayIndex,
            boolean withOrdinality,
            boolean preserveEmpty,
            UnnestCollection collection,
            Expression collectionExpression) {
        this(
                inputType,
                outputType,
                java.util.Collections.singletonList(arrayIndex),
                java.util.Collections.singletonList(withOrdinality),
                java.util.Collections.singletonList(preserveEmpty),
                java.util.Collections.singletonList(collection),
                java.util.Collections.singletonList(collectionExpression));
    }

    StreamFusionArrayUnnestOperator(
            RowType inputType,
            RowType outputType,
            List<Integer> arrayIndexes,
            List<Boolean> withOrdinalities,
            List<Boolean> preserveEmpty,
            List<UnnestCollection> collections,
            List<Expression> collectionExpressions) {
        this.inputType = inputType;
        this.outputType = outputType;
        this.serializer = new RowDataSerializer(inputType);
        int stageCount = arrayIndexes.size();
        if (stageCount == 0
                || withOrdinalities.size() != stageCount
                || preserveEmpty.size() != stageCount
                || collections.size() != stageCount
                || collectionExpressions.size() != stageCount) {
            throw new IllegalArgumentException("A native UNNEST chain must contain equally sized, non-empty stages");
        }
        Operator root = Operator.newBuilder().setInput(Input.newBuilder()).build();
        for (int stage = 0; stage < stageCount; stage++) {
            ArrayUnnest.Builder unnest = ArrayUnnest.newBuilder()
                    .setInput(root)
                    .setArrayIndex(arrayIndexes.get(stage))
                    .setWithOrdinality(withOrdinalities.get(stage))
                    .setPreserveEmpty(preserveEmpty.get(stage))
                    .setCollection(collections.get(stage));
            Expression expression = collectionExpressions.get(stage);
            if (expression != null) {
                unnest.setCollectionExpression(expression);
            }
            root = Operator.newBuilder().setArrayUnnest(unnest).build();
        }
        this.serializedPlan = NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(root)
                .build()
                .toByteArray();
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
                    throw new IllegalStateException("Native UNNEST returned invalid input-row ordinal " + inputRow);
                }
                row.setRowKind(rowKinds.get(inputRow));
                output.collect(new StreamRecord<>(row));
            }
        }
        rows.clear();
        rowKinds.clear();
    }
}
