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
import tech.streamfusion.proto.plan.v1.Calc;
import tech.streamfusion.proto.plan.v1.EmptyType;
import tech.streamfusion.proto.plan.v1.Expression;
import tech.streamfusion.proto.plan.v1.GreaterThanOrEqual;
import tech.streamfusion.proto.plan.v1.Input;
import tech.streamfusion.proto.plan.v1.InputReference;
import tech.streamfusion.proto.plan.v1.IntegerLiteral;
import tech.streamfusion.proto.plan.v1.LogicalType;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;

/** Initial bounded, vectorized identity calc for one non-null INT column. */
final class StreamFusionIdentityCalcOperator extends AbstractStreamOperator<RowData>
        implements OneInputStreamOperator<RowData, RowData>, BoundedOneInput {
    private static final int BATCH_SIZE = 1024;
    private final int inputIndex;
    private final Integer minimum;
    private final RowType inputType;
    private final RowType outputType;
    private final RowDataSerializer serializer;
    private final byte[] serializedPlan;
    private final List<RowData> rows = new ArrayList<>(BATCH_SIZE);
    private final List<RowKind> rowKinds = new ArrayList<>(BATCH_SIZE);

    StreamFusionIdentityCalcOperator(RowType inputType, RowType outputType, int inputIndex, Integer minimum) {
        this.inputType = inputType;
        this.outputType = outputType;
        this.inputIndex = inputIndex;
        this.minimum = minimum;
        this.serializer = new RowDataSerializer(inputType);
        this.serializedPlan = createPlan(inputIndex, minimum);
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
        List<RowKind> outputRowKinds = new ArrayList<>(rows.size());
        for (int index = 0; index < rows.size(); index++) {
            if (minimum == null || rows.get(index).getInt(inputIndex) >= minimum) {
                outputRowKinds.add(rowKinds.get(index));
            }
        }
        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                ArrowRowDataBatch inputBatch = ArrowRowDataBatch.transpose(rows, inputType, allocator);
                ArrowRowDataBatch outputBatch =
                        ArrowCDataBridge.execute(serializedPlan, inputBatch, outputType, allocator)) {
            if (outputBatch.size() != outputRowKinds.size()) {
                throw new IllegalStateException(
                        "Native calc returned " + outputBatch.size() + " rows, expected " + outputRowKinds.size());
            }
            for (int index = 0; index < outputBatch.size(); index++) {
                RowData row = outputBatch.rowView(index);
                row.setRowKind(outputRowKinds.get(index));
                output.collect(new StreamRecord<>(row));
            }
        }
        rows.clear();
        rowKinds.clear();
    }

    private static byte[] createPlan(int inputIndex, Integer minimum) {
        LogicalType integer = LogicalType.newBuilder()
                .setNullable(false)
                .setInteger(EmptyType.getDefaultInstance())
                .build();
        Calc.Builder calc = Calc.newBuilder()
                .setInput(Operator.newBuilder().setInput(Input.newBuilder()))
                .addProjections(inputReference(inputIndex, integer));
        if (minimum != null) {
            calc.setCondition(Expression.newBuilder()
                    .setGreaterThanOrEqual(GreaterThanOrEqual.newBuilder()
                            .setLeft(inputReference(inputIndex, integer))
                            .setRight(Expression.newBuilder()
                                    .setIntegerLiteral(
                                            IntegerLiteral.newBuilder().setValue(minimum)))));
        }
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setCalc(calc))
                .build()
                .toByteArray();
    }

    private static Expression inputReference(int inputIndex, LogicalType type) {
        return Expression.newBuilder()
                .setInputReference(
                        InputReference.newBuilder().setIndex(inputIndex).setType(type))
                .build();
    }
}
