/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.arrow;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.generated.JoinCondition;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import tech.streamfusion.nativebridge.NativeMemoryManager;
import tech.streamfusion.nativebridge.NativeWindowJoinBridge;

/** Batched Arrow boundary and exact Flink result materialization for native Window Join. */
public final class ArrowWindowJoinCDataBridge {
    private ArrowWindowJoinCDataBridge() {}

    public static Result process(
            long handle,
            int side,
            ArrowRowDataBatch input,
            List<byte[]> preencodedKeys,
            List<byte[]> storedRows,
            RowType leftType,
            RowType rightType,
            RowType outputType,
            FlinkJoinType joinType,
            JoinCondition condition,
            int[] nullFilterKeys,
            BufferAllocator allocator,
            NativeMemoryManager memoryManager) {
        try (ArrowArray inputArray = ArrowArray.allocateNew(input.allocator());
                ArrowSchema inputSchema = ArrowSchema.allocateNew(input.allocator());
                ArrowArray outputArray = ArrowArray.allocateNew(allocator);
                ArrowSchema outputSchema = ArrowSchema.allocateNew(allocator);
                CDataDictionaryProvider dictionaries = new CDataDictionaryProvider()) {
            VarBinaryVector rows =
                    binaryMetadata("__streamfusion_stored_row", storedRows, input.size(), input.allocator());
            TinyIntVector kinds = inputKinds(input, input.allocator());
            VarBinaryVector keys = preencodedKeys == null
                    ? null
                    : binaryMetadata("__streamfusion_key", preencodedKeys, input.size(), input.allocator());
            try {
                VectorSchemaRoot exported = input.root();
                exported = exported.addVector(exported.getFieldVectors().size(), rows);
                exported = exported.addVector(exported.getFieldVectors().size(), kinds);
                if (keys != null) {
                    exported = exported.addVector(exported.getFieldVectors().size(), keys);
                }
                exported.setRowCount(input.size());
                Data.exportVectorSchemaRoot(input.allocator(), exported, null, inputArray, inputSchema);
                long count = NativeWindowJoinBridge.process(
                        handle,
                        side,
                        inputArray.memoryAddress(),
                        inputSchema.memoryAddress(),
                        outputArray.memoryAddress(),
                        outputSchema.memoryAddress());
                return importAndJoin(
                        count,
                        outputArray,
                        outputSchema,
                        leftType,
                        rightType,
                        outputType,
                        joinType,
                        condition,
                        nullFilterKeys,
                        allocator,
                        dictionaries);
            } finally {
                memoryManager.finishArrowTransfer();
                if (keys != null) {
                    keys.close();
                }
                kinds.close();
                rows.close();
            }
        }
    }

    public static Result advance(
            long handle,
            long watermark,
            RowType leftType,
            RowType rightType,
            RowType outputType,
            FlinkJoinType joinType,
            JoinCondition condition,
            int[] nullFilterKeys,
            BufferAllocator allocator,
            NativeMemoryManager memoryManager) {
        try (ArrowArray outputArray = ArrowArray.allocateNew(allocator);
                ArrowSchema outputSchema = ArrowSchema.allocateNew(allocator);
                CDataDictionaryProvider dictionaries = new CDataDictionaryProvider()) {
            try {
                long count = NativeWindowJoinBridge.advance(
                        handle, watermark, outputArray.memoryAddress(), outputSchema.memoryAddress());
                return importAndJoin(
                        count,
                        outputArray,
                        outputSchema,
                        leftType,
                        rightType,
                        outputType,
                        joinType,
                        condition,
                        nullFilterKeys,
                        allocator,
                        dictionaries);
            } finally {
                memoryManager.finishArrowTransfer();
            }
        }
    }

    private static Result importAndJoin(
            long count,
            ArrowArray outputArray,
            ArrowSchema outputSchema,
            RowType leftType,
            RowType rightType,
            RowType outputType,
            FlinkJoinType joinType,
            JoinCondition condition,
            int[] nullFilterKeys,
            BufferAllocator allocator,
            CDataDictionaryProvider dictionaries) {
        if (count < 0 || count > Integer.MAX_VALUE) {
            throw new IllegalStateException("Native window join returned invalid row count " + count);
        }
        try (VectorSchemaRoot output =
                Data.importVectorSchemaRoot(allocator, outputArray, outputSchema, dictionaries)) {
            output.setRowCount((int) count);
            if (output.getFieldVectors().size() != 3
                    || !(output.getVector(0) instanceof VarBinaryVector)
                    || !(output.getVector(1) instanceof TinyIntVector)
                    || !(output.getVector(2) instanceof IntVector)) {
                throw new IllegalStateException("Native window join returned invalid row metadata");
            }
            VarBinaryVector rows = (VarBinaryVector) output.getVector(0);
            TinyIntVector sides = (TinyIntVector) output.getVector(1);
            IntVector groups = (IntVector) output.getVector(2);
            Map<Integer, WindowGroup> windows = new LinkedHashMap<>();
            for (int index = 0; index < count; index++) {
                int side = sides.get(index);
                RowType type = side == 0 ? leftType : rightType;
                byte[] bytes = rows.get(index);
                BinaryRowData row = new BinaryRowData(type.getFieldCount());
                row.pointTo(MemorySegmentFactory.wrap(bytes), 0, bytes.length);
                windows.computeIfAbsent(groups.get(index), ignored -> new WindowGroup())
                        .rows(side)
                        .add(row);
            }
            List<RowData> joined = new ArrayList<>();
            long evaluations = 0;
            for (WindowGroup window : windows.values()) {
                JoinOutput result = join(
                        window.left, window.right, leftType, rightType, joinType, condition, nullFilterKeys, joined);
                evaluations += result.evaluations;
            }
            RowKind[] kinds = new RowKind[joined.size()];
            java.util.Arrays.fill(kinds, RowKind.INSERT);
            return new Result(
                    ArrowRowDataBatch.transpose(joined, outputType, allocator)
                            .withRowKinds(kinds)
                            .withoutTimestamps(),
                    evaluations);
        }
    }

    private static JoinOutput join(
            List<RowData> leftRows,
            List<RowData> rightRows,
            RowType leftType,
            RowType rightType,
            FlinkJoinType joinType,
            JoinCondition condition,
            int[] nullFilterKeys,
            List<RowData> output) {
        long evaluations = 0;
        boolean[] matchedRight = new boolean[rightRows.size()];
        GenericRowData leftNull = new GenericRowData(leftType.getFieldCount());
        GenericRowData rightNull = new GenericRowData(rightType.getFieldCount());
        for (RowData left : leftRows) {
            boolean matched = false;
            if (!hasFilteredNull(left, nullFilterKeys)) {
                for (int rightIndex = 0; rightIndex < rightRows.size(); rightIndex++) {
                    RowData right = rightRows.get(rightIndex);
                    evaluations++;
                    if (condition.apply(left, right)) {
                        matched = true;
                        matchedRight[rightIndex] = true;
                        if (joinType == FlinkJoinType.INNER
                                || joinType == FlinkJoinType.LEFT
                                || joinType == FlinkJoinType.RIGHT
                                || joinType == FlinkJoinType.FULL) {
                            output.add(insert(new JoinedRowData(left, right)));
                        }
                        if (joinType == FlinkJoinType.SEMI) {
                            break;
                        }
                    }
                }
            }
            if (joinType == FlinkJoinType.SEMI && matched) {
                output.add(insert(left));
            } else if (joinType == FlinkJoinType.ANTI && !matched) {
                output.add(insert(left));
            } else if ((joinType == FlinkJoinType.LEFT || joinType == FlinkJoinType.FULL) && !matched) {
                output.add(insert(new JoinedRowData(left, rightNull)));
            }
        }
        if (joinType == FlinkJoinType.RIGHT || joinType == FlinkJoinType.FULL) {
            for (int index = 0; index < rightRows.size(); index++) {
                if (!matchedRight[index]) {
                    output.add(insert(new JoinedRowData(leftNull, rightRows.get(index))));
                }
            }
        }
        return new JoinOutput(evaluations);
    }

    private static boolean hasFilteredNull(RowData row, int[] nullFilterKeys) {
        for (int key : nullFilterKeys) {
            if (row.isNullAt(key)) {
                return true;
            }
        }
        return false;
    }

    private static RowData insert(RowData row) {
        row.setRowKind(RowKind.INSERT);
        return row;
    }

    private static VarBinaryVector binaryMetadata(
            String name, List<byte[]> values, int count, BufferAllocator allocator) {
        if (values.size() != count) {
            throw new IllegalArgumentException(name + " count does not match the Arrow batch");
        }
        VarBinaryVector vector = new VarBinaryVector(name, allocator);
        vector.allocateNew();
        for (int index = 0; index < count; index++) {
            vector.setSafe(index, values.get(index));
        }
        vector.setValueCount(count);
        return vector;
    }

    private static TinyIntVector inputKinds(ArrowRowDataBatch input, BufferAllocator allocator) {
        TinyIntVector vector = new TinyIntVector("__streamfusion_input_row_kind", allocator);
        vector.allocateNew(input.size());
        for (int index = 0; index < input.size(); index++) {
            vector.setSafe(index, input.rowKind(index).toByteValue());
        }
        vector.setValueCount(input.size());
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

    private static final class WindowGroup {
        private final List<RowData> left = new ArrayList<>();
        private final List<RowData> right = new ArrayList<>();

        private List<RowData> rows(int side) {
            if (side == 0) {
                return left;
            }
            if (side == 1) {
                return right;
            }
            throw new IllegalStateException("Native window join returned invalid side " + side);
        }
    }

    private static final class JoinOutput {
        private final long evaluations;

        private JoinOutput(long evaluations) {
            this.evaluations = evaluations;
        }
    }
}
