/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.arrow;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import tech.streamfusion.nativebridge.NativeBoundedSortBridge;
import tech.streamfusion.nativebridge.NativeMemoryManager;

/** Arrow C Data transport for native bounded-sort input and terminal output. */
public final class ArrowBoundedSortCDataBridge {
    private ArrowBoundedSortCDataBridge() {}

    public static void process(long handle, ArrowRowDataBatch input, NativeMemoryManager memoryManager) {
        try (ArrowArray inputArray = ArrowArray.allocateNew(input.allocator());
                ArrowSchema inputSchema = ArrowSchema.allocateNew(input.allocator());
                TinyIntVector kinds = inputKinds(input, input.allocator())) {
            try {
                VectorSchemaRoot exported =
                        input.root().addVector(input.root().getFieldVectors().size(), kinds);
                exported.setRowCount(input.size());
                Data.exportVectorSchemaRoot(input.allocator(), exported, null, inputArray, inputSchema);
                NativeBoundedSortBridge.process(handle, inputArray.memoryAddress(), inputSchema.memoryAddress());
            } finally {
                memoryManager.finishArrowTransfer();
            }
        }
    }

    public static ArrowRowDataBatch finish(
            long handle, RowType outputType, BufferAllocator allocator, NativeMemoryManager memoryManager) {
        try (ArrowArray outputArray = ArrowArray.allocateNew(allocator);
                ArrowSchema outputSchema = ArrowSchema.allocateNew(allocator);
                CDataDictionaryProvider dictionaries = new CDataDictionaryProvider()) {
            try {
                long count = NativeBoundedSortBridge.finish(
                        handle, outputArray.memoryAddress(), outputSchema.memoryAddress());
                if (count < 0 || count > Integer.MAX_VALUE) {
                    throw new IllegalStateException("Native bounded sort returned invalid row count " + count);
                }
                VectorSchemaRoot output =
                        Data.importVectorSchemaRoot(allocator, outputArray, outputSchema, dictionaries);
                output.setRowCount((int) count);
                int kindIndex = output.getFieldVectors().size() - 1;
                if (kindIndex < 0 || !(output.getVector(kindIndex) instanceof TinyIntVector)) {
                    output.close();
                    throw new IllegalStateException("Native bounded sort returned invalid RowKind metadata");
                }
                TinyIntVector kinds = (TinyIntVector) output.getVector(kindIndex);
                RowKind[] rowKinds = new RowKind[(int) count];
                for (int index = 0; index < count; index++) {
                    rowKinds[index] = RowKind.fromByteValue(kinds.get(index));
                }
                VectorSchemaRoot visible = output.removeVector(kindIndex);
                kinds.close();
                return ArrowRowDataBatch.wrap(visible, outputType, allocator)
                        .withRowKinds(rowKinds)
                        .withoutTimestamps();
            } finally {
                memoryManager.finishArrowTransfer();
            }
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
}
