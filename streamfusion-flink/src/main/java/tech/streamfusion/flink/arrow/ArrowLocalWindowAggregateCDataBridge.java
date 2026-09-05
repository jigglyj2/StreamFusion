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
import tech.streamfusion.nativebridge.NativeLocalWindowAggregateBridge;
import tech.streamfusion.nativebridge.NativeMemoryManager;

/** Arrow C Data transport for native local slicing-window partials. */
public final class ArrowLocalWindowAggregateCDataBridge {
    private ArrowLocalWindowAggregateCDataBridge() {}

    public static ArrowRowDataBatch execute(
            long handle,
            ArrowRowDataBatch input,
            boolean inputChangelog,
            RowType outputType,
            BufferAllocator allocator,
            NativeMemoryManager memoryManager) {
        try (ArrowArray inputArray = ArrowArray.allocateNew(input.allocator());
                ArrowSchema inputSchema = ArrowSchema.allocateNew(input.allocator());
                ArrowArray outputArray = ArrowArray.allocateNew(allocator);
                ArrowSchema outputSchema = ArrowSchema.allocateNew(allocator);
                CDataDictionaryProvider dictionaries = new CDataDictionaryProvider()) {
            TinyIntVector inputKinds = inputChangelog ? inputKinds(input, input.allocator()) : null;
            try {
                VectorSchemaRoot exported = input.root();
                if (inputKinds != null) {
                    exported = exported.addVector(exported.getFieldVectors().size(), inputKinds);
                }
                exported.setRowCount(input.size());
                Data.exportVectorSchemaRoot(input.allocator(), exported, null, inputArray, inputSchema);
                long rowCount;
                try {
                    rowCount = NativeLocalWindowAggregateBridge.process(
                            handle,
                            inputArray.memoryAddress(),
                            inputSchema.memoryAddress(),
                            outputArray.memoryAddress(),
                            outputSchema.memoryAddress());
                    if (rowCount < 0 || rowCount > Integer.MAX_VALUE) {
                        throw new IllegalStateException(
                                "Native local window aggregate returned invalid row count " + rowCount);
                    }
                    VectorSchemaRoot output =
                            Data.importVectorSchemaRoot(allocator, outputArray, outputSchema, dictionaries);
                    output.setRowCount((int) rowCount);
                    return ArrowRowDataBatch.wrap(output, outputType, allocator).withoutTimestamps();
                } finally {
                    memoryManager.finishArrowTransfer();
                }
            } finally {
                if (inputKinds != null) {
                    inputKinds.close();
                }
            }
        }
    }

    private static TinyIntVector inputKinds(ArrowRowDataBatch input, BufferAllocator allocator) {
        TinyIntVector vector = new TinyIntVector("__streamfusion_input_row_kind", allocator);
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
