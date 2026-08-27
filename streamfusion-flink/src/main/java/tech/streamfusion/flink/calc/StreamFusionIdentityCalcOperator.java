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
import tech.streamfusion.proto.plan.v1.Comparison;
import tech.streamfusion.proto.plan.v1.DecimalType;
import tech.streamfusion.proto.plan.v1.EmptyType;
import tech.streamfusion.proto.plan.v1.Expression;
import tech.streamfusion.proto.plan.v1.Input;
import tech.streamfusion.proto.plan.v1.InputReference;
import tech.streamfusion.proto.plan.v1.IntegerLiteral;
import tech.streamfusion.proto.plan.v1.LogicalType;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;
import tech.streamfusion.proto.plan.v1.PrecisionType;

/** Initial bounded, vectorized identity calc for one non-null INT column. */
final class StreamFusionIdentityCalcOperator extends AbstractStreamOperator<RowData>
        implements OneInputStreamOperator<RowData, RowData>, BoundedOneInput {
    private static final int BATCH_SIZE = 1024;
    private final StreamFusionIntComparison condition;
    private final RowType inputType;
    private final RowType outputType;
    private final RowDataSerializer serializer;
    private final byte[] serializedPlan;
    private final List<RowData> rows = new ArrayList<>(BATCH_SIZE);
    private final List<RowKind> rowKinds = new ArrayList<>(BATCH_SIZE);

    StreamFusionIdentityCalcOperator(
            RowType inputType, RowType outputType, List<Expression> projections, StreamFusionIntComparison condition) {
        this.inputType = inputType;
        this.outputType = outputType;
        this.condition = condition;
        this.serializer = new RowDataSerializer(inputType);
        this.serializedPlan = createPlan(inputType, projections, condition);
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
            if (condition == null || condition.test(rows.get(index))) {
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

    private static byte[] createPlan(
            RowType inputType, List<Expression> projections, StreamFusionIntComparison condition) {
        Calc.Builder calc = Calc.newBuilder().setInput(Operator.newBuilder().setInput(Input.newBuilder()));
        calc.addAllProjections(projections);
        if (condition != null) {
            LogicalType integer = logicalType(inputType, condition.inputIndex());
            Expression input = inputReference(condition.inputIndex(), integer);
            Expression literal = Expression.newBuilder()
                    .setIntegerLiteral(IntegerLiteral.newBuilder().setValue(condition.literal()))
                    .build();
            calc.setCondition(Expression.newBuilder()
                    .setComparison(Comparison.newBuilder()
                            .setLeft(condition.inputOnLeft() ? input : literal)
                            .setRight(condition.inputOnLeft() ? literal : input)
                            .setOperator(condition.operator())));
        }
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setCalc(calc))
                .build()
                .toByteArray();
    }

    static LogicalType logicalType(RowType inputType, int inputIndex) {
        org.apache.flink.table.types.logical.LogicalType flinkType = inputType.getTypeAt(inputIndex);
        LogicalType.Builder type = LogicalType.newBuilder().setNullable(flinkType.isNullable());
        switch (flinkType.getTypeRoot()) {
            case TINYINT:
                return type.setTinyint(EmptyType.getDefaultInstance()).build();
            case SMALLINT:
                return type.setSmallint(EmptyType.getDefaultInstance()).build();
            case INTEGER:
                return type.setInteger(EmptyType.getDefaultInstance()).build();
            case BIGINT:
                return type.setBigint(EmptyType.getDefaultInstance()).build();
            case FLOAT:
                return type.setFloat(EmptyType.getDefaultInstance()).build();
            case DOUBLE:
                return type.setDouble(EmptyType.getDefaultInstance()).build();
            case BOOLEAN:
                return type.setBoolean(EmptyType.getDefaultInstance()).build();
            case CHAR:
            case VARCHAR:
                return type.setVarchar(EmptyType.getDefaultInstance()).build();
            case BINARY:
            case VARBINARY:
                return type.setBinary(EmptyType.getDefaultInstance()).build();
            case DATE:
                return type.setDate(EmptyType.getDefaultInstance()).build();
            case TIME_WITHOUT_TIME_ZONE:
                return type.setTime(PrecisionType.newBuilder()
                                .setPrecision(
                                        ((org.apache.flink.table.types.logical.TimeType) flinkType).getPrecision()))
                        .build();
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return type.setTimestamp(PrecisionType.newBuilder()
                                .setPrecision(((org.apache.flink.table.types.logical.TimestampType) flinkType)
                                        .getPrecision()))
                        .build();
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return type.setTimestampLtz(PrecisionType.newBuilder()
                                .setPrecision(((org.apache.flink.table.types.logical.LocalZonedTimestampType) flinkType)
                                        .getPrecision()))
                        .build();
            case DECIMAL:
                org.apache.flink.table.types.logical.DecimalType decimal =
                        (org.apache.flink.table.types.logical.DecimalType) flinkType;
                return type.setDecimal(DecimalType.newBuilder()
                                .setPrecision(decimal.getPrecision())
                                .setScale(decimal.getScale()))
                        .build();
            default:
                throw new IllegalArgumentException("Unsupported projection type " + flinkType);
        }
    }

    static Expression inputReference(int inputIndex, LogicalType type) {
        return Expression.newBuilder()
                .setInputReference(
                        InputReference.newBuilder().setIndex(inputIndex).setType(type))
                .build();
    }
}
