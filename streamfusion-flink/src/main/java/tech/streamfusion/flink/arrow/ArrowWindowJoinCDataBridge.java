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
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.JoinCondition;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import tech.streamfusion.nativebridge.NativeMemoryManager;
import tech.streamfusion.nativebridge.NativeWindowJoinBridge;

/** Batched Arrow boundary and Arrow-native Flink result materialization for Window Join. */
public final class ArrowWindowJoinCDataBridge {
    private ArrowWindowJoinCDataBridge() {}

    public static Result process(
            long handle,
            int side,
            ArrowRowDataBatch input,
            List<byte[]> preencodedKeys,
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
            TinyIntVector kinds = inputKinds(input, input.allocator());
            VarBinaryVector keys = preencodedKeys == null
                    ? null
                    : binaryMetadata("__streamfusion_key", preencodedKeys, input.size(), input.allocator());
            try {
                VectorSchemaRoot exported = input.root();
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
            int leftArity = leftType.getFieldCount();
            int rightArity = rightType.getFieldCount();
            int sideIndex = leftArity + rightArity;
            int groupIndex = sideIndex + 1;
            if (output.getFieldVectors().size() != groupIndex + 1
                    || !(output.getVector(sideIndex) instanceof TinyIntVector)
                    || !(output.getVector(groupIndex) instanceof IntVector)) {
                throw new IllegalStateException("Native window join returned invalid Arrow row metadata");
            }
            TinyIntVector sides = (TinyIntVector) output.getVector(sideIndex);
            IntVector groups = (IntVector) output.getVector(groupIndex);
            VectorSchemaRoot leftRoot = new VectorSchemaRoot(
                    output.getSchema().getFields().subList(0, leftArity),
                    output.getFieldVectors().subList(0, leftArity),
                    (int) count);
            VectorSchemaRoot rightRoot = new VectorSchemaRoot(
                    output.getSchema().getFields().subList(leftArity, sideIndex),
                    output.getFieldVectors().subList(leftArity, sideIndex),
                    (int) count);
            ArrowReader leftReader = ArrowUtils.createArrowReader(leftRoot, leftType);
            ArrowReader rightReader = ArrowUtils.createArrowReader(rightRoot, rightType);
            Map<Integer, WindowGroup> windows = new LinkedHashMap<>();
            for (int index = 0; index < count; index++) {
                int side = sides.get(index);
                windows.computeIfAbsent(groups.get(index), ignored -> new WindowGroup())
                        .rows(side)
                        .add(index);
            }
            List<JoinedIndex> joined = new ArrayList<>();
            long evaluations = 0;
            for (WindowGroup window : windows.values()) {
                JoinOutput result = join(
                        window.left,
                        window.right,
                        leftReader,
                        rightReader,
                        joinType,
                        condition,
                        nullFilterKeys,
                        joined);
                evaluations += result.evaluations;
            }
            return new Result(
                    materialize(output, leftArity, rightArity, outputType, joinType, joined, allocator), evaluations);
        }
    }

    private static JoinOutput join(
            List<Integer> leftRows,
            List<Integer> rightRows,
            ArrowReader leftReader,
            ArrowReader rightReader,
            FlinkJoinType joinType,
            JoinCondition condition,
            int[] nullFilterKeys,
            List<JoinedIndex> output) {
        long evaluations = 0;
        boolean[] matchedRight = new boolean[rightRows.size()];
        for (int leftIndex : leftRows) {
            RowData left = leftReader.read(leftIndex);
            boolean matched = false;
            if (!hasFilteredNull(left, nullFilterKeys)) {
                for (int rightIndex = 0; rightIndex < rightRows.size(); rightIndex++) {
                    int rightRow = rightRows.get(rightIndex);
                    RowData right = rightReader.read(rightRow);
                    evaluations++;
                    if (condition.apply(left, right)) {
                        matched = true;
                        matchedRight[rightIndex] = true;
                        if (joinType == FlinkJoinType.INNER
                                || joinType == FlinkJoinType.LEFT
                                || joinType == FlinkJoinType.RIGHT
                                || joinType == FlinkJoinType.FULL) {
                            output.add(new JoinedIndex(leftIndex, rightRow));
                        }
                        if (joinType == FlinkJoinType.SEMI) {
                            break;
                        }
                    }
                }
            }
            if (joinType == FlinkJoinType.SEMI && matched) {
                output.add(new JoinedIndex(leftIndex, -1));
            } else if (joinType == FlinkJoinType.ANTI && !matched) {
                output.add(new JoinedIndex(leftIndex, -1));
            } else if ((joinType == FlinkJoinType.LEFT || joinType == FlinkJoinType.FULL) && !matched) {
                output.add(new JoinedIndex(leftIndex, -1));
            }
        }
        if (joinType == FlinkJoinType.RIGHT || joinType == FlinkJoinType.FULL) {
            for (int index = 0; index < rightRows.size(); index++) {
                if (!matchedRight[index]) {
                    output.add(new JoinedIndex(-1, rightRows.get(index)));
                }
            }
        }
        return new JoinOutput(evaluations);
    }

    private static ArrowRowDataBatch materialize(
            VectorSchemaRoot candidates,
            int leftArity,
            int rightArity,
            RowType outputType,
            FlinkJoinType joinType,
            List<JoinedIndex> joined,
            BufferAllocator allocator) {
        boolean leftOnly = joinType == FlinkJoinType.SEMI || joinType == FlinkJoinType.ANTI;
        int expectedArity = leftOnly ? leftArity : leftArity + rightArity;
        if (outputType.getFieldCount() != expectedArity) {
            throw new IllegalStateException("Window join output arity does not match its join type");
        }
        VectorSchemaRoot result = VectorSchemaRoot.create(ArrowUtils.toArrowSchema(outputType), allocator);
        try {
            for (FieldVector target : result.getFieldVectors()) {
                target.setInitialCapacity(joined.size());
                target.allocateNew();
            }
            for (int outputRow = 0; outputRow < joined.size(); outputRow++) {
                JoinedIndex pair = joined.get(outputRow);
                for (int field = 0; field < expectedArity; field++) {
                    boolean left = field < leftArity;
                    int sourceRow = left ? pair.left : pair.right;
                    FieldVector target = result.getVector(field);
                    if (sourceRow < 0) {
                        target.setNull(outputRow);
                    } else {
                        int sourceField = field;
                        target.copyFromSafe(sourceRow, outputRow, candidates.getVector(sourceField));
                    }
                }
            }
            result.setRowCount(joined.size());
            RowKind[] kinds = new RowKind[joined.size()];
            java.util.Arrays.fill(kinds, RowKind.INSERT);
            return ArrowRowDataBatch.wrap(result, outputType, allocator)
                    .withRowKinds(kinds)
                    .withoutTimestamps();
        } catch (RuntimeException | Error failure) {
            result.close();
            throw failure;
        }
    }

    private static boolean hasFilteredNull(RowData row, int[] nullFilterKeys) {
        for (int key : nullFilterKeys) {
            if (row.isNullAt(key)) {
                return true;
            }
        }
        return false;
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
        private final List<Integer> left = new ArrayList<>();
        private final List<Integer> right = new ArrayList<>();

        private List<Integer> rows(int side) {
            if (side == 0) {
                return left;
            }
            if (side == 1) {
                return right;
            }
            throw new IllegalStateException("Native window join returned invalid side " + side);
        }
    }

    private static final class JoinedIndex {
        private final int left;
        private final int right;

        private JoinedIndex(int left, int right) {
            this.left = left;
            this.right = right;
        }
    }

    private static final class JoinOutput {
        private final long evaluations;

        private JoinOutput(long evaluations) {
            this.evaluations = evaluations;
        }
    }
}
