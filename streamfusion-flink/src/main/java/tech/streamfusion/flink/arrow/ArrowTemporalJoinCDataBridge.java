/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.arrow;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.table.runtime.generated.JoinCondition;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import tech.streamfusion.flink.exchange.ArrowExchangeInputBatch;
import tech.streamfusion.nativebridge.NativeMemoryManager;
import tech.streamfusion.nativebridge.NativeTemporalJoinBridge;

/** One Arrow C Data call for a temporal-join input batch or timer firing. */
public final class ArrowTemporalJoinCDataBridge {
    private ArrowTemporalJoinCDataBridge() {}

    public static Result process(
            long handle,
            int side,
            long processingTime,
            ArrowExchangeInputBatch input,
            List<byte[]> preencodedKeys,
            RowType leftType,
            RowType rightType,
            RowType outputType,
            FlinkJoinType joinType,
            JoinCondition condition,
            BufferAllocator allocator,
            NativeMemoryManager memoryManager) {
        try (ArrowArray inputArray = ArrowArray.allocateNew(allocator);
                ArrowSchema inputSchema = ArrowSchema.allocateNew(allocator);
                ArrowArray outputArray = ArrowArray.allocateNew(allocator);
                ArrowSchema outputSchema = ArrowSchema.allocateNew(allocator);
                CDataDictionaryProvider dictionaries = new CDataDictionaryProvider()) {
            VarBinaryVector keys =
                    preencodedKeys == null ? null : preencodedKeys(allocator, input.size(), preencodedKeys);
            try {
                VectorSchemaRoot exported = input.transportRoot();
                if (keys != null) {
                    List<FieldVector> vectors = new ArrayList<>(exported.getFieldVectors());
                    vectors.add(keys);
                    exported = new VectorSchemaRoot(vectors);
                    exported.setRowCount(input.size());
                }
                Data.exportVectorSchemaRoot(allocator, exported, null, inputArray, inputSchema);
                long count = NativeTemporalJoinBridge.process(
                        handle,
                        side,
                        processingTime,
                        inputArray.memoryAddress(),
                        inputSchema.memoryAddress(),
                        outputArray.memoryAddress(),
                        outputSchema.memoryAddress());
                return importOutput(
                        count,
                        outputArray,
                        outputSchema,
                        leftType,
                        rightType,
                        outputType,
                        joinType,
                        condition,
                        allocator,
                        dictionaries);
            } finally {
                memoryManager.finishArrowTransfer();
                if (keys != null) {
                    keys.close();
                }
            }
        }
    }

    public static Result advance(
            long handle,
            boolean processingTime,
            long timestamp,
            RowType leftType,
            RowType rightType,
            RowType outputType,
            FlinkJoinType joinType,
            JoinCondition condition,
            BufferAllocator allocator,
            NativeMemoryManager memoryManager) {
        try (ArrowArray outputArray = ArrowArray.allocateNew(allocator);
                ArrowSchema outputSchema = ArrowSchema.allocateNew(allocator);
                CDataDictionaryProvider dictionaries = new CDataDictionaryProvider()) {
            try {
                long count = NativeTemporalJoinBridge.advance(
                        handle, processingTime, timestamp, outputArray.memoryAddress(), outputSchema.memoryAddress());
                return importOutput(
                        count,
                        outputArray,
                        outputSchema,
                        leftType,
                        rightType,
                        outputType,
                        joinType,
                        condition,
                        allocator,
                        dictionaries);
            } finally {
                memoryManager.finishArrowTransfer();
            }
        }
    }

    private static Result importOutput(
            long count,
            ArrowArray outputArray,
            ArrowSchema outputSchema,
            RowType leftType,
            RowType rightType,
            RowType outputType,
            FlinkJoinType joinType,
            JoinCondition condition,
            BufferAllocator allocator,
            CDataDictionaryProvider dictionaries) {
        if (count < 0 || count > Integer.MAX_VALUE) {
            throw new IllegalStateException("Native temporal join returned invalid row count " + count);
        }
        VectorSchemaRoot output = Data.importVectorSchemaRoot(allocator, outputArray, outputSchema, dictionaries);
        output.setRowCount((int) count);
        int ordinalIndex = output.getFieldVectors().size() - 1;
        int rowKindIndex = ordinalIndex - 1;
        int matchedIndex = rowKindIndex - 1;
        if (rowKindIndex < 0
                || matchedIndex != outputType.getFieldCount()
                || !(output.getVector(ordinalIndex) instanceof IntVector)
                || !(output.getVector(rowKindIndex) instanceof TinyIntVector)
                || !(output.getVector(matchedIndex) instanceof TinyIntVector)) {
            output.close();
            throw new IllegalStateException("Native temporal join returned invalid changelog metadata");
        }
        RowKind[] kinds = new RowKind[output.getRowCount()];
        TinyIntVector kindVector = (TinyIntVector) output.getVector(rowKindIndex);
        for (int row = 0; row < output.getRowCount(); row++) {
            kinds[row] = RowKind.fromByteValue(kindVector.get(row));
        }
        if (condition != null) {
            return materializeCondition(
                    output, leftType, rightType, outputType, joinType, condition, matchedIndex, kinds, allocator);
        }
        return visibleOutput(output, outputType, matchedIndex, kinds, allocator, 0);
    }

    private static Result visibleOutput(
            VectorSchemaRoot output,
            RowType outputType,
            int matchedIndex,
            RowKind[] kinds,
            BufferAllocator allocator,
            long evaluations) {
        int rowKindIndex = matchedIndex + 1;
        int ordinalIndex = rowKindIndex + 1;
        FieldVector ordinal = output.getVector(ordinalIndex);
        VectorSchemaRoot withoutOrdinal = output.removeVector(ordinalIndex);
        ordinal.close();
        FieldVector kind = withoutOrdinal.getVector(rowKindIndex);
        VectorSchemaRoot withoutKind = withoutOrdinal.removeVector(rowKindIndex);
        kind.close();
        FieldVector matched = withoutKind.getVector(matchedIndex);
        VectorSchemaRoot visible = withoutKind.removeVector(matchedIndex);
        matched.close();
        VectorSchemaRoot normalized = new VectorSchemaRoot(
                ArrowUtils.toArrowSchema(outputType).getFields(), visible.getFieldVectors(), visible.getRowCount());
        return new Result(
                ArrowRowDataBatch.wrap(normalized, outputType, allocator)
                        .withRowKinds(kinds)
                        .withoutTimestamps(),
                evaluations);
    }

    private static Result materializeCondition(
            VectorSchemaRoot candidates,
            RowType leftType,
            RowType rightType,
            RowType outputType,
            FlinkJoinType joinType,
            JoinCondition condition,
            int matchedIndex,
            RowKind[] kinds,
            BufferAllocator allocator) {
        try {
            int leftArity = leftType.getFieldCount();
            int rightArity = rightType.getFieldCount();
            VectorSchemaRoot leftRoot = new VectorSchemaRoot(
                    candidates.getSchema().getFields().subList(0, leftArity),
                    candidates.getFieldVectors().subList(0, leftArity),
                    candidates.getRowCount());
            VectorSchemaRoot rightRoot = new VectorSchemaRoot(
                    candidates.getSchema().getFields().subList(leftArity, leftArity + rightArity),
                    candidates.getFieldVectors().subList(leftArity, leftArity + rightArity),
                    candidates.getRowCount());
            ArrowReader left = ArrowUtils.createArrowReader(leftRoot, leftType);
            ArrowReader right = ArrowUtils.createArrowReader(rightRoot, rightType);
            TinyIntVector matched = (TinyIntVector) candidates.getVector(matchedIndex);
            List<Integer> selected = new ArrayList<>();
            List<Boolean> nullRight = new ArrayList<>();
            long evaluations = 0;
            boolean requiresMaterialization = false;
            for (int row = 0; row < candidates.getRowCount(); row++) {
                if (matched.get(row) == 0) {
                    selected.add(row);
                    nullRight.add(true);
                    continue;
                }
                evaluations++;
                boolean passes = condition.apply(left.read(row), right.read(row));
                if (passes || joinType == FlinkJoinType.LEFT) {
                    selected.add(row);
                    nullRight.add(!passes);
                }
                requiresMaterialization |= !passes;
            }
            if (!requiresMaterialization && selected.size() == candidates.getRowCount()) {
                return visibleOutput(candidates, outputType, matchedIndex, kinds, allocator, evaluations);
            }
            VectorSchemaRoot output = VectorSchemaRoot.create(ArrowUtils.toArrowSchema(outputType), allocator);
            try {
                for (FieldVector target : output.getFieldVectors()) {
                    target.setInitialCapacity(selected.size());
                    target.allocateNew();
                }
                RowKind[] selectedKinds = new RowKind[selected.size()];
                for (int outputRow = 0; outputRow < selected.size(); outputRow++) {
                    int inputRow = selected.get(outputRow);
                    selectedKinds[outputRow] = kinds[inputRow];
                    for (int field = 0; field < outputType.getFieldCount(); field++) {
                        FieldVector target = output.getVector(field);
                        if (field >= leftArity && nullRight.get(outputRow)) {
                            target.setNull(outputRow);
                        } else {
                            target.copyFromSafe(inputRow, outputRow, candidates.getVector(field));
                        }
                    }
                }
                output.setRowCount(selected.size());
                candidates.close();
                return new Result(
                        ArrowRowDataBatch.wrap(output, outputType, allocator)
                                .withRowKinds(selectedKinds)
                                .withoutTimestamps(),
                        evaluations);
            } catch (RuntimeException | Error failure) {
                output.close();
                throw failure;
            }
        } catch (RuntimeException | Error failure) {
            candidates.close();
            throw failure;
        }
    }

    private static VarBinaryVector preencodedKeys(BufferAllocator allocator, int rowCount, List<byte[]> values) {
        if (values.size() != rowCount) {
            throw new IllegalArgumentException("Temporal join key count does not match its Arrow batch");
        }
        VarBinaryVector vector = new VarBinaryVector("__streamfusion_key", allocator);
        vector.allocateNew();
        for (int row = 0; row < values.size(); row++) {
            vector.setSafe(row, values.get(row));
        }
        vector.setValueCount(values.size());
        return vector;
    }

    public static final class Result implements AutoCloseable {
        private final ArrowRowDataBatch output;
        private final long conditionEvaluations;

        private Result(ArrowRowDataBatch output, long conditionEvaluations) {
            this.output = output;
            this.conditionEvaluations = conditionEvaluations;
        }

        public ArrowRowDataBatch output() {
            return output;
        }

        public long conditionEvaluations() {
            return conditionEvaluations;
        }

        @Override
        public void close() {
            output.close();
        }
    }
}
