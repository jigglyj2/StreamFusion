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
import tech.streamfusion.nativebridge.NativeBoundedRankBridge;
import tech.streamfusion.nativebridge.NativeMemoryManager;

/** Ownership-safe Arrow C Data transport for native bounded RANK. */
public final class ArrowBoundedRankCDataBridge {
    private ArrowBoundedRankCDataBridge() {}

    public static ArrowRowDataBatch process(
            long handle,
            ArrowRowDataBatch input,
            RowType outputType,
            BufferAllocator allocator,
            NativeMemoryManager memoryManager) {
        try (ArrowArray inputArray = ArrowArray.allocateNew(input.allocator());
                ArrowSchema inputSchema = ArrowSchema.allocateNew(input.allocator());
                ArrowArray outputArray = ArrowArray.allocateNew(allocator);
                ArrowSchema outputSchema = ArrowSchema.allocateNew(allocator);
                CDataDictionaryProvider dictionaries = new CDataDictionaryProvider()) {
            TinyIntVector kinds = inputKinds(input);
            try {
                VectorSchemaRoot exported =
                        input.root().addVector(input.root().getFieldVectors().size(), kinds);
                exported.setRowCount(input.size());
                Data.exportVectorSchemaRoot(input.allocator(), exported, null, inputArray, inputSchema);
                long count = NativeBoundedRankBridge.process(
                        handle,
                        inputArray.memoryAddress(),
                        inputSchema.memoryAddress(),
                        outputArray.memoryAddress(),
                        outputSchema.memoryAddress());
                if (count < 0 || count > Integer.MAX_VALUE) {
                    throw new IllegalStateException("Native bounded rank returned invalid row count " + count);
                }
                VectorSchemaRoot output =
                        Data.importVectorSchemaRoot(allocator, outputArray, outputSchema, dictionaries);
                output.setRowCount((int) count);
                int rowKindIndex = output.getFieldVectors().size() - 1;
                if (rowKindIndex < 0 || !(output.getVector(rowKindIndex) instanceof TinyIntVector)) {
                    output.close();
                    throw new IllegalStateException("Native bounded rank returned invalid RowKind metadata");
                }
                TinyIntVector outputKinds = (TinyIntVector) output.getVector(rowKindIndex);
                RowKind[] rowKinds = new RowKind[(int) count];
                for (int row = 0; row < count; row++) {
                    rowKinds[row] = RowKind.fromByteValue(outputKinds.get(row));
                }
                VectorSchemaRoot visible = output.removeVector(rowKindIndex);
                outputKinds.close();
                return ArrowRowDataBatch.wrap(visible, outputType, allocator)
                        .withRowKinds(rowKinds)
                        .withoutTimestamps();
            } finally {
                memoryManager.finishArrowTransfer();
                kinds.close();
            }
        }
    }

    private static TinyIntVector inputKinds(ArrowRowDataBatch input) {
        TinyIntVector vector = new TinyIntVector("__streamfusion_input_row_kind", input.allocator());
        try {
            vector.allocateNew(input.size());
            for (int row = 0; row < input.size(); row++) {
                vector.setSafe(row, input.rowKind(row).toByteValue());
            }
            vector.setValueCount(input.size());
            return vector;
        } catch (RuntimeException | Error failure) {
            vector.close();
            throw failure;
        }
    }
}
