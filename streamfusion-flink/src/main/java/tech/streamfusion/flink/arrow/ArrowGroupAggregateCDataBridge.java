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
import tech.streamfusion.nativebridge.NativeGroupAggregateBridge;
import tech.streamfusion.nativebridge.NativeMemoryManager;

/** Arrow C Data transport for one persistent native keyed group aggregate. */
public final class ArrowGroupAggregateCDataBridge {
    private ArrowGroupAggregateCDataBridge() {}

    public static ArrowRowDataBatch execute(
            long handle,
            ArrowRowDataBatch input,
            List<byte[]> preencodedKeys,
            boolean inputChangelog,
            RowType outputType,
            BufferAllocator allocator) {
        return execute(
                handle, input, preencodedKeys, inputChangelog, outputType, allocator, NativeMemoryManager.unbounded());
    }

    /** Flushes a native mini-batch without manufacturing an empty Java-side input batch. */
    public static ArrowRowDataBatch finishBundle(
            long handle, RowType outputType, BufferAllocator allocator, NativeMemoryManager memoryManager) {
        try (ArrowArray outputArray = ArrowArray.allocateNew(allocator);
                ArrowSchema outputSchema = ArrowSchema.allocateNew(allocator);
                CDataDictionaryProvider dictionaries = new CDataDictionaryProvider()) {
            try {
                long rowCount = NativeGroupAggregateBridge.finishBundle(
                        handle, outputArray.memoryAddress(), outputSchema.memoryAddress());
                if (rowCount < 0 || rowCount > Integer.MAX_VALUE) {
                    throw new IllegalStateException(
                            "Native group aggregate returned invalid bundle row count " + rowCount);
                }
                VectorSchemaRoot output =
                        Data.importVectorSchemaRoot(allocator, outputArray, outputSchema, dictionaries);
                output.setRowCount((int) rowCount);
                return removeRowKinds(output, outputType, allocator);
            } finally {
                memoryManager.finishArrowTransfer();
            }
        }
    }

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
            VarBinaryVector keyVector =
                    preencodedKeys == null ? null : preencodedKeys(preencodedKeys, input.size(), input.allocator());
            TinyIntVector inputKinds = inputChangelog ? inputKinds(input, input.allocator()) : null;
            long rowCount;
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
                try {
                    rowCount = NativeGroupAggregateBridge.process(
                            handle,
                            inputArray.memoryAddress(),
                            inputSchema.memoryAddress(),
                            outputArray.memoryAddress(),
                            outputSchema.memoryAddress());
                    if (rowCount < 0 || rowCount > Integer.MAX_VALUE) {
                        throw new IllegalStateException(
                                "Native group aggregate returned invalid row count " + rowCount);
                    }
                    VectorSchemaRoot output =
                            Data.importVectorSchemaRoot(allocator, outputArray, outputSchema, dictionaries);
                    output.setRowCount((int) rowCount);
                    return removeRowKinds(output, outputType, allocator);
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

    private static ArrowRowDataBatch removeRowKinds(
            VectorSchemaRoot output, RowType outputType, BufferAllocator allocator) {
        int rowKindIndex = output.getFieldVectors().size() - 1;
        FieldVector field = rowKindIndex < 0 ? null : output.getVector(rowKindIndex);
        if (!(field instanceof TinyIntVector)) {
            output.close();
            throw new IllegalStateException("Native group aggregate did not return RowKind metadata");
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

    private static VarBinaryVector preencodedKeys(List<byte[]> keys, int rowCount, BufferAllocator allocator) {
        if (keys.size() != rowCount) {
            throw new IllegalArgumentException("Group aggregate key count does not match the batch");
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
