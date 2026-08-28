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
import tech.streamfusion.proto.plan.v1.CollectionType;
import tech.streamfusion.proto.plan.v1.DecimalType;
import tech.streamfusion.proto.plan.v1.EmptyType;
import tech.streamfusion.proto.plan.v1.Expression;
import tech.streamfusion.proto.plan.v1.Input;
import tech.streamfusion.proto.plan.v1.InputReference;
import tech.streamfusion.proto.plan.v1.LengthType;
import tech.streamfusion.proto.plan.v1.LogicalType;
import tech.streamfusion.proto.plan.v1.MapType;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;
import tech.streamfusion.proto.plan.v1.PrecisionType;
import tech.streamfusion.proto.plan.v1.RowField;
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
        this.inputType = boundaryInputType;
        this.outputType = outputType;
        this.serializer = new RowDataSerializer(boundaryInputType);
        this.serializedPlan = createPlan(
                arrayUnnestIndex,
                withOrdinality,
                preserveEmpty,
                collection,
                collectionExpression,
                unnestOutputFieldCount,
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
            int arrayUnnestIndex,
            boolean withOrdinality,
            boolean preserveEmpty,
            UnnestCollection collection,
            Expression collectionExpression,
            int unnestOutputFieldCount,
            List<List<Expression>> projectionStages,
            List<Expression> conditions) {
        Operator input = Operator.newBuilder().setInput(Input.newBuilder()).build();
        ArrayUnnest.Builder unnest = ArrayUnnest.newBuilder()
                .setInput(input)
                .setArrayIndex(arrayUnnestIndex)
                .setWithOrdinality(withOrdinality)
                .setPreserveEmpty(preserveEmpty)
                .setCollection(collection);
        if (collectionExpression != null) {
            unnest.setCollectionExpression(collectionExpression);
        }
        Operator operator = Operator.newBuilder().setArrayUnnest(unnest).build();
        return createPlan(operator, unnestOutputFieldCount, projectionStages, conditions);
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
        return logicalType(inputType.getTypeAt(inputIndex));
    }

    static LogicalType logicalType(org.apache.flink.table.types.logical.LogicalType flinkType) {
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
                return type.setFixedChar(LengthType.newBuilder()
                                .setLength(((org.apache.flink.table.types.logical.CharType) flinkType).getLength()))
                        .build();
            case VARCHAR:
                return type.setVarchar(EmptyType.getDefaultInstance()).build();
            case BINARY:
                return type.setFixedBinary(LengthType.newBuilder()
                                .setLength(((org.apache.flink.table.types.logical.BinaryType) flinkType).getLength()))
                        .build();
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
            case ARRAY:
                org.apache.flink.table.types.logical.ArrayType array =
                        (org.apache.flink.table.types.logical.ArrayType) flinkType;
                return type.setArray(CollectionType.newBuilder().setElementType(logicalType(array.getElementType())))
                        .build();
            case MAP:
                org.apache.flink.table.types.logical.MapType map =
                        (org.apache.flink.table.types.logical.MapType) flinkType;
                return type.setMap(MapType.newBuilder()
                                .setKeyType(logicalType(map.getKeyType()))
                                .setValueType(logicalType(map.getValueType())))
                        .build();
            case MULTISET:
                org.apache.flink.table.types.logical.MultisetType multiset =
                        (org.apache.flink.table.types.logical.MultisetType) flinkType;
                return type.setMap(MapType.newBuilder()
                                .setKeyType(logicalType(multiset.getElementType()))
                                .setValueType(logicalType(new org.apache.flink.table.types.logical.IntType(false))))
                        .build();
            case ROW:
                org.apache.flink.table.types.logical.RowType row =
                        (org.apache.flink.table.types.logical.RowType) flinkType;
                tech.streamfusion.proto.plan.v1.RowType.Builder rowType =
                        tech.streamfusion.proto.plan.v1.RowType.newBuilder();
                for (org.apache.flink.table.types.logical.RowType.RowField field : row.getFields()) {
                    rowType.addFields(
                            RowField.newBuilder().setName(field.getName()).setType(logicalType(field.getType())));
                }
                return type.setRow(rowType).build();
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
