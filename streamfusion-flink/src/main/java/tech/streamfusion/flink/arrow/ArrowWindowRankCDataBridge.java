/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.arrow;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
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
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import tech.streamfusion.nativebridge.NativeMemoryManager;
import tech.streamfusion.nativebridge.NativeWindowRankBridge;

/** Arrow C Data transport and exact Flink comparator materialization for native Window Top-N. */
public final class ArrowWindowRankCDataBridge {
    private ArrowWindowRankCDataBridge() {}

    public static ArrowRowDataBatch process(
            long handle,
            ArrowRowDataBatch input,
            List<byte[]> preencodedKeys,
            List<byte[]> storedRows,
            List<byte[]> sortKeys,
            RowType outputType,
            BufferAllocator allocator,
            NativeMemoryManager memoryManager) {
        try (ArrowArray inputArray = ArrowArray.allocateNew(input.allocator());
                ArrowSchema inputSchema = ArrowSchema.allocateNew(input.allocator());
                ArrowArray outputArray = ArrowArray.allocateNew(allocator);
                ArrowSchema outputSchema = ArrowSchema.allocateNew(allocator);
                CDataDictionaryProvider dictionaries = new CDataDictionaryProvider()) {
            VarBinaryVector rows =
                    binaryMetadata("__streamfusion_stored_row", storedRows, input.size(), input.allocator());
            VarBinaryVector sorts =
                    binaryMetadata("__streamfusion_sort_key", sortKeys, input.size(), input.allocator());
            TinyIntVector kinds = inputKinds(input, input.allocator());
            VarBinaryVector keys = preencodedKeys == null
                    ? null
                    : binaryMetadata("__streamfusion_key", preencodedKeys, input.size(), input.allocator());
            try {
                VectorSchemaRoot exported = input.root();
                exported = exported.addVector(exported.getFieldVectors().size(), rows);
                exported = exported.addVector(exported.getFieldVectors().size(), sorts);
                exported = exported.addVector(exported.getFieldVectors().size(), kinds);
                if (keys != null) {
                    exported = exported.addVector(exported.getFieldVectors().size(), keys);
                }
                exported.setRowCount(input.size());
                Data.exportVectorSchemaRoot(input.allocator(), exported, null, inputArray, inputSchema);
                long count = NativeWindowRankBridge.process(
                        handle,
                        inputArray.memoryAddress(),
                        inputSchema.memoryAddress(),
                        outputArray.memoryAddress(),
                        outputSchema.memoryAddress());
                return importEmpty(count, outputArray, outputSchema, outputType, allocator, dictionaries);
            } finally {
                memoryManager.finishArrowTransfer();
                if (keys != null) {
                    keys.close();
                }
                kinds.close();
                sorts.close();
                rows.close();
            }
        }
    }

    public static ArrowRowDataBatch advance(
            long handle,
            long watermark,
            RowType inputType,
            RowType outputType,
            int sortKeyArity,
            long rankStart,
            long rankEnd,
            boolean outputRankNumber,
            RecordComparator comparator,
            BufferAllocator allocator,
            NativeMemoryManager memoryManager) {
        try (ArrowArray outputArray = ArrowArray.allocateNew(allocator);
                ArrowSchema outputSchema = ArrowSchema.allocateNew(allocator);
                CDataDictionaryProvider dictionaries = new CDataDictionaryProvider()) {
            try {
                long count = NativeWindowRankBridge.advance(
                        handle, watermark, outputArray.memoryAddress(), outputSchema.memoryAddress());
                if (count < 0 || count > Integer.MAX_VALUE) {
                    throw new IllegalStateException("Native window rank returned invalid row count " + count);
                }
                try (VectorSchemaRoot output =
                        Data.importVectorSchemaRoot(allocator, outputArray, outputSchema, dictionaries)) {
                    output.setRowCount((int) count);
                    return materialize(
                            output,
                            inputType,
                            outputType,
                            sortKeyArity,
                            rankStart,
                            rankEnd,
                            outputRankNumber,
                            comparator,
                            allocator);
                }
            } finally {
                memoryManager.finishArrowTransfer();
            }
        }
    }

    private static ArrowRowDataBatch importEmpty(
            long count,
            ArrowArray outputArray,
            ArrowSchema outputSchema,
            RowType outputType,
            BufferAllocator allocator,
            CDataDictionaryProvider dictionaries) {
        if (count != 0) {
            throw new IllegalStateException("Window rank emitted records before a timer fired");
        }
        try (VectorSchemaRoot ignored =
                Data.importVectorSchemaRoot(allocator, outputArray, outputSchema, dictionaries)) {
            return ArrowRowDataBatch.transpose(List.of(), outputType, allocator);
        }
    }

    private static ArrowRowDataBatch materialize(
            VectorSchemaRoot output,
            RowType inputType,
            RowType outputType,
            int sortKeyArity,
            long rankStart,
            long rankEnd,
            boolean outputRankNumber,
            RecordComparator comparator,
            BufferAllocator allocator) {
        if (output.getFieldVectors().size() != 5
                || !(output.getVector(0) instanceof VarBinaryVector)
                || !(output.getVector(1) instanceof VarBinaryVector)
                || !(output.getVector(2) instanceof IntVector)
                || !(output.getVector(3) instanceof org.apache.arrow.vector.BigIntVector)
                || !(output.getVector(4) instanceof TinyIntVector)) {
            throw new IllegalStateException("Native window rank returned invalid candidate metadata");
        }
        VarBinaryVector rows = (VarBinaryVector) output.getVector(0);
        VarBinaryVector sorts = (VarBinaryVector) output.getVector(1);
        IntVector groups = (IntVector) output.getVector(2);
        org.apache.arrow.vector.BigIntVector sequences = (org.apache.arrow.vector.BigIntVector) output.getVector(3);
        List<Candidate> candidates = new ArrayList<>(output.getRowCount());
        for (int index = 0; index < output.getRowCount(); index++) {
            candidates.add(new Candidate(
                    groups.get(index),
                    sequences.get(index),
                    binaryRow(rows.get(index), inputType.getFieldCount()),
                    binaryRow(sorts.get(index), sortKeyArity)));
        }
        Comparator<Candidate> ordering = (left, right) -> {
            int result = comparator.compare(left.sortKey, right.sortKey);
            return result != 0 ? result : Long.compare(left.sequence, right.sequence);
        };
        List<RowData> selected = new ArrayList<>();
        int start = 0;
        while (start < candidates.size()) {
            int group = candidates.get(start).group;
            int end = start + 1;
            while (end < candidates.size() && candidates.get(end).group == group) {
                end++;
            }
            candidates.subList(start, end).sort(ordering);
            long first = Math.max(0, rankStart - 1);
            long exclusiveEnd = Math.min((long) (end - start), rankEnd);
            for (long rank = first; rank < exclusiveEnd; rank++) {
                BinaryRowData row = candidates.get(start + Math.toIntExact(rank)).row;
                if (outputRankNumber) {
                    selected.add(new JoinedRowData(row, GenericRowData.of(rank + 1)));
                } else {
                    selected.add(row);
                }
            }
            start = end;
        }
        RowKind[] kinds = new RowKind[selected.size()];
        java.util.Arrays.fill(kinds, RowKind.INSERT);
        return ArrowRowDataBatch.transpose(selected, outputType, allocator)
                .withRowKinds(kinds)
                .withoutTimestamps();
    }

    private static BinaryRowData binaryRow(byte[] bytes, int arity) {
        BinaryRowData row = new BinaryRowData(arity);
        row.pointTo(MemorySegmentFactory.wrap(bytes), 0, bytes.length);
        return row;
    }

    private static VarBinaryVector binaryMetadata(
            String name, List<byte[]> values, int count, BufferAllocator allocator) {
        if (values.size() != count) {
            throw new IllegalArgumentException(name + " count does not match the Arrow batch");
        }
        VarBinaryVector vector = new VarBinaryVector(name, allocator);
        try {
            vector.allocateNew();
            for (int index = 0; index < count; index++) {
                vector.setSafe(index, values.get(index));
            }
            vector.setValueCount(count);
            return vector;
        } catch (RuntimeException | Error failure) {
            vector.close();
            throw failure;
        }
    }

    private static TinyIntVector inputKinds(ArrowRowDataBatch input, BufferAllocator allocator) {
        TinyIntVector vector = new TinyIntVector("__streamfusion_input_row_kind", allocator);
        try {
            vector.allocateNew(input.size());
            for (int index = 0; index < input.size(); index++) {
                vector.setSafe(index, input.rowKind(index).toByteValue());
            }
            vector.setValueCount(input.size());
            return vector;
        } catch (RuntimeException | Error failure) {
            vector.close();
            throw failure;
        }
    }

    private static final class Candidate {
        private final int group;
        private final long sequence;
        private final BinaryRowData row;
        private final BinaryRowData sortKey;

        private Candidate(int group, long sequence, BinaryRowData row, BinaryRowData sortKey) {
            this.group = group;
            this.sequence = sequence;
            this.row = row;
            this.sortKey = sortKey;
        }
    }
}
