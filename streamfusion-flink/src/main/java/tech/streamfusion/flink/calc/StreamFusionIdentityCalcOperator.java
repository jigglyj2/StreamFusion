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
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import tech.streamfusion.flink.arrow.ArrowIntBatch;
import tech.streamfusion.nativebridge.NativeCalcBridge;
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
    private final byte[] serializedPlan;
    private final List<Integer> values = new ArrayList<>(BATCH_SIZE);
    private final List<RowKind> rowKinds = new ArrayList<>(BATCH_SIZE);

    StreamFusionIdentityCalcOperator(int inputIndex, Integer minimum) {
        this.inputIndex = inputIndex;
        this.minimum = minimum;
        this.serializedPlan = createPlan(inputIndex, minimum);
    }

    @Override
    public void processElement(StreamRecord<RowData> element) {
        RowData row = element.getValue();
        values.add(row.getInt(inputIndex));
        rowKinds.add(row.getRowKind());
        if (values.size() == BATCH_SIZE) {
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
        if (values.isEmpty()) {
            return;
        }
        int[] input = values.stream().mapToInt(Integer::intValue).toArray();
        int[] result;
        try (ArrowIntBatch inputBatch = ArrowIntBatch.fromRebasedArray(input)) {
            result = NativeCalcBridge.executeIdentity(serializedPlan, inputBatch.toRebasedArray());
        }
        List<RowKind> outputRowKinds = new ArrayList<>(result.length);
        for (int index = 0; index < values.size(); index++) {
            if (minimum == null || values.get(index) >= minimum) {
                outputRowKinds.add(rowKinds.get(index));
            }
        }
        if (result.length != outputRowKinds.size()) {
            throw new IllegalStateException(
                    "Native calc returned " + result.length + " rows, expected " + outputRowKinds.size());
        }
        try (ArrowIntBatch outputBatch = ArrowIntBatch.fromRebasedArray(result)) {
            for (int index = 0; index < result.length; index++) {
                RowData row = outputBatch.rowView(index);
                row.setRowKind(outputRowKinds.get(index));
                output.collect(new StreamRecord<>(row));
            }
        }
        values.clear();
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
