/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.arrow;

import java.util.List;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import tech.streamfusion.nativebridge.NativeMemoryManager;
import tech.streamfusion.nativebridge.NativeWindowAggregateBridge;

/** Arrow C Data transport for persistent native window aggregation and timer firing. */
public final class ArrowWindowAggregateCDataBridge {
    private ArrowWindowAggregateCDataBridge() {}

    public static ArrowRowDataBatch process(
            long handle,
            ArrowRowDataBatch input,
            List<byte[]> preencodedKeys,
            boolean inputChangelog,
            long processingTime,
            RowType outputType,
            BufferAllocator allocator,
            NativeMemoryManager memoryManager) {
        try (ArrowArray inputArray = ArrowArray.allocateNew(input.allocator());
                ArrowSchema inputSchema = ArrowSchema.allocateNew(input.allocator());
                ArrowArray outputArray = ArrowArray.allocateNew(allocator);
                ArrowSchema outputSchema = ArrowSchema.allocateNew(allocator);
                CDataDictionaryProvider dictionaries = new CDataDictionaryProvider()) {
            VarBinaryVector keys =
                    preencodedKeys == null ? null : preencodedKeys(preencodedKeys, input.size(), input.allocator());
            TinyIntVector kinds = inputChangelog ? inputKinds(input, input.allocator()) : null;
            try {
                VectorSchemaRoot exported = input.root();
                if (kinds != null) {
                    exported = exported.addVector(exported.getFieldVectors().size(), kinds);
                }
                if (keys != null) {
                    exported = exported.addVector(exported.getFieldVectors().size(), keys);
                }
                exported.setRowCount(input.size());
                Data.exportVectorSchemaRoot(input.allocator(), exported, null, inputArray, inputSchema);
                long rows = NativeWindowAggregateBridge.process(
                        handle,
                        inputArray.memoryAddress(),
                        inputSchema.memoryAddress(),
                        outputArray.memoryAddress(),
                        outputSchema.memoryAddress(),
                        processingTime);
                return importOutput(rows, outputArray, outputSchema, outputType, allocator, dictionaries);
            } finally {
                memoryManager.finishArrowTransfer();
                if (keys != null) {
                    keys.close();
                }
                if (kinds != null) {
                    kinds.close();
                }
            }
        }
    }

    public static ArrowRowDataBatch advance(
            long handle,
            boolean processingTime,
            long timestamp,
            RowType outputType,
            BufferAllocator allocator,
            NativeMemoryManager memoryManager) {
        try (ArrowArray outputArray = ArrowArray.allocateNew(allocator);
                ArrowSchema outputSchema = ArrowSchema.allocateNew(allocator);
                CDataDictionaryProvider dictionaries = new CDataDictionaryProvider()) {
            try {
                long rows = NativeWindowAggregateBridge.advance(
                        handle, processingTime, timestamp, outputArray.memoryAddress(), outputSchema.memoryAddress());
                return importOutput(rows, outputArray, outputSchema, outputType, allocator, dictionaries);
            } finally {
                memoryManager.finishArrowTransfer();
            }
        }
    }

    private static ArrowRowDataBatch importOutput(
            long rows,
            ArrowArray outputArray,
            ArrowSchema outputSchema,
            RowType outputType,
            BufferAllocator allocator,
            CDataDictionaryProvider dictionaries) {
        if (rows < 0 || rows > Integer.MAX_VALUE) {
            throw new IllegalStateException("Native window aggregate returned invalid row count " + rows);
        }
        VectorSchemaRoot output = Data.importVectorSchemaRoot(allocator, outputArray, outputSchema, dictionaries);
        output.setRowCount((int) rows);
        int rowKindIndex = output.getFieldVectors().size() - 1;
        FieldVector field = rowKindIndex < 0 ? null : output.getVector(rowKindIndex);
        if (!(field instanceof TinyIntVector)) {
            output.close();
            throw new IllegalStateException("Native window aggregate did not return RowKind metadata");
        }
        RowKind[] rowKinds = new RowKind[output.getRowCount()];
        TinyIntVector kinds = (TinyIntVector) field;
        for (int row = 0; row < rowKinds.length; row++) {
            rowKinds[row] = RowKind.fromByteValue(kinds.get(row));
        }
        VectorSchemaRoot visible = output.removeVector(rowKindIndex);
        field.close();
        return ArrowRowDataBatch.wrap(visible, outputType, allocator)
                .withRowKinds(rowKinds)
                .withoutTimestamps();
    }

    private static VarBinaryVector preencodedKeys(List<byte[]> keys, int rows, BufferAllocator allocator) {
        if (keys.size() != rows) {
            throw new IllegalArgumentException("Window aggregate key count does not match the batch");
        }
        VarBinaryVector vector = new VarBinaryVector("__streamfusion_key", allocator);
        vector.allocateNew();
        for (int row = 0; row < rows; row++) {
            vector.setSafe(row, keys.get(row));
        }
        vector.setValueCount(rows);
        return vector;
    }

    private static TinyIntVector inputKinds(ArrowRowDataBatch input, BufferAllocator allocator) {
        TinyIntVector vector = new TinyIntVector("__streamfusion_input_row_kind", allocator);
        vector.allocateNew(input.size());
        for (int row = 0; row < input.size(); row++) {
            vector.setSafe(row, input.rowKind(row).toByteValue());
        }
        vector.setValueCount(input.size());
        return vector;
    }
}
