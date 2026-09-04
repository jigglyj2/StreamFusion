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
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.nativebridge.NativeLocalGroupAggregateBridge;
import tech.streamfusion.nativebridge.NativeMemoryManager;

/** Arrow C Data transport for the native local mini-batch aggregate. */
public final class ArrowLocalGroupAggregateCDataBridge {
    private ArrowLocalGroupAggregateCDataBridge() {}

    public static ArrowRowDataBatch execute(
            long handle,
            ArrowRowDataBatch input,
            List<byte[]> preencodedKeys,
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
            VarBinaryVector keyVector =
                    preencodedKeys == null ? null : preencodedKeys(preencodedKeys, input.size(), input.allocator());
            try {
                VectorSchemaRoot exported = input.root();
                if (inputKinds != null) {
                    exported = exported.addVector(exported.getFieldVectors().size(), inputKinds);
                }
                if (keyVector != null) {
                    exported = exported.addVector(exported.getFieldVectors().size(), keyVector);
                }
                exported.setRowCount(input.size());
                Data.exportVectorSchemaRoot(input.allocator(), exported, null, inputArray, inputSchema);
                long rowCount;
                try {
                    rowCount = NativeLocalGroupAggregateBridge.process(
                            handle,
                            inputArray.memoryAddress(),
                            inputSchema.memoryAddress(),
                            outputArray.memoryAddress(),
                            outputSchema.memoryAddress());
                    return importOutput(rowCount, outputArray, outputSchema, dictionaries, outputType, allocator);
                } finally {
                    memoryManager.finishArrowTransfer();
                }
            } finally {
                if (keyVector != null) {
                    keyVector.close();
                }
                if (inputKinds != null) {
                    inputKinds.close();
                }
            }
        }
    }

    private static VarBinaryVector preencodedKeys(List<byte[]> keys, int rowCount, BufferAllocator allocator) {
        if (keys.size() != rowCount) {
            throw new IllegalArgumentException("Local group aggregate key count does not match the batch");
        }
        VarBinaryVector vector = new VarBinaryVector("__streamfusion_key", allocator);
        try {
            vector.allocateNew();
            for (int row = 0; row < rowCount; row++) {
                vector.setSafe(row, keys.get(row));
            }
            vector.setValueCount(rowCount);
            return vector;
        } catch (RuntimeException | Error failure) {
            vector.close();
            throw failure;
        }
    }

    public static ArrowRowDataBatch finishBundle(
            long handle, RowType outputType, BufferAllocator allocator, NativeMemoryManager memoryManager) {
        try (ArrowArray outputArray = ArrowArray.allocateNew(allocator);
                ArrowSchema outputSchema = ArrowSchema.allocateNew(allocator);
                CDataDictionaryProvider dictionaries = new CDataDictionaryProvider()) {
            try {
                long rowCount = NativeLocalGroupAggregateBridge.finishBundle(
                        handle, outputArray.memoryAddress(), outputSchema.memoryAddress());
                return importOutput(rowCount, outputArray, outputSchema, dictionaries, outputType, allocator);
            } finally {
                memoryManager.finishArrowTransfer();
            }
        }
    }

    private static ArrowRowDataBatch importOutput(
            long rowCount,
            ArrowArray outputArray,
            ArrowSchema outputSchema,
            CDataDictionaryProvider dictionaries,
            RowType outputType,
            BufferAllocator allocator) {
        if (rowCount < 0 || rowCount > Integer.MAX_VALUE) {
            throw new IllegalStateException("Native local group aggregate returned invalid row count " + rowCount);
        }
        VectorSchemaRoot output = Data.importVectorSchemaRoot(allocator, outputArray, outputSchema, dictionaries);
        output.setRowCount((int) rowCount);
        return ArrowRowDataBatch.wrap(output, outputType, allocator).withoutTimestamps();
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
