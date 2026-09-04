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
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import tech.streamfusion.nativebridge.NativeMemoryManager;
import tech.streamfusion.nativebridge.NativeOverAggregateBridge;

/** Arrow C Data transport for persistent native streaming OVER aggregation. */
public final class ArrowOverAggregateCDataBridge {
    private ArrowOverAggregateCDataBridge() {}

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
                long rows = NativeOverAggregateBridge.process(
                        handle,
                        inputArray.memoryAddress(),
                        inputSchema.memoryAddress(),
                        outputArray.memoryAddress(),
                        outputSchema.memoryAddress(),
                        processingTime);
                if (rows < 0 || rows > Integer.MAX_VALUE) {
                    throw new IllegalStateException("Native OVER aggregate returned invalid row count " + rows);
                }
                VectorSchemaRoot output =
                        Data.importVectorSchemaRoot(allocator, outputArray, outputSchema, dictionaries);
                output.setRowCount((int) rows);
                return removeEnvelope(output, input, outputType, allocator);
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

    public static ArrowRowDataBatch advanceEventTime(
            long handle,
            long watermark,
            RowType outputType,
            BufferAllocator allocator,
            NativeMemoryManager memoryManager) {
        try (ArrowArray outputArray = ArrowArray.allocateNew(allocator);
                ArrowSchema outputSchema = ArrowSchema.allocateNew(allocator);
                CDataDictionaryProvider dictionaries = new CDataDictionaryProvider()) {
            try {
                long rows = NativeOverAggregateBridge.advanceEventTime(
                        handle, watermark, outputArray.memoryAddress(), outputSchema.memoryAddress());
                if (rows < 0 || rows > Integer.MAX_VALUE) {
                    throw new IllegalStateException("Native OVER aggregate returned invalid row count " + rows);
                }
                VectorSchemaRoot output =
                        Data.importVectorSchemaRoot(allocator, outputArray, outputSchema, dictionaries);
                output.setRowCount((int) rows);
                return removeTimerEnvelope(output, outputType, allocator);
            } finally {
                memoryManager.finishArrowTransfer();
            }
        }
    }

    public static ArrowRowDataBatch advanceProcessingTime(
            long handle,
            long timestamp,
            RowType outputType,
            BufferAllocator allocator,
            NativeMemoryManager memoryManager) {
        try (ArrowArray outputArray = ArrowArray.allocateNew(allocator);
                ArrowSchema outputSchema = ArrowSchema.allocateNew(allocator);
                CDataDictionaryProvider dictionaries = new CDataDictionaryProvider()) {
            try {
                long rows = NativeOverAggregateBridge.advanceProcessingTime(
                        handle, timestamp, outputArray.memoryAddress(), outputSchema.memoryAddress());
                if (rows < 0 || rows > Integer.MAX_VALUE) {
                    throw new IllegalStateException("Native OVER aggregate returned invalid row count " + rows);
                }
                VectorSchemaRoot output =
                        Data.importVectorSchemaRoot(allocator, outputArray, outputSchema, dictionaries);
                output.setRowCount((int) rows);
                return removeTimerEnvelope(output, outputType, allocator);
            } finally {
                memoryManager.finishArrowTransfer();
            }
        }
    }

    private static ArrowRowDataBatch removeEnvelope(
            VectorSchemaRoot output, ArrowRowDataBatch input, RowType outputType, BufferAllocator allocator) {
        int ordinalIndex = output.getFieldVectors().size() - 1;
        int kindIndex = ordinalIndex - 1;
        FieldVector ordinalField = ordinalIndex < 0 ? null : output.getVector(ordinalIndex);
        FieldVector kindField = kindIndex < 0 ? null : output.getVector(kindIndex);
        if (!(ordinalField instanceof IntVector) || !(kindField instanceof TinyIntVector)) {
            output.close();
            throw new IllegalStateException("Native OVER aggregate did not return envelope metadata");
        }
        int[] ordinals = new int[output.getRowCount()];
        RowKind[] rowKinds = new RowKind[output.getRowCount()];
        IntVector ordinalVector = (IntVector) ordinalField;
        TinyIntVector kindVector = (TinyIntVector) kindField;
        for (int row = 0; row < output.getRowCount(); row++) {
            ordinals[row] = ordinalVector.get(row);
            rowKinds[row] = RowKind.fromByteValue(kindVector.get(row));
        }
        VectorSchemaRoot withoutOrdinal = output.removeVector(ordinalIndex);
        ordinalField.close();
        VectorSchemaRoot visible = withoutOrdinal.removeVector(kindIndex);
        kindField.close();
        return ArrowRowDataBatch.wrap(visible, outputType, allocator)
                .selectEnvelopeFrom(input, ordinals)
                .withRowKinds(rowKinds);
    }

    private static ArrowRowDataBatch removeTimerEnvelope(
            VectorSchemaRoot output, RowType outputType, BufferAllocator allocator) {
        int ordinalIndex = output.getFieldVectors().size() - 1;
        int kindIndex = ordinalIndex - 1;
        FieldVector ordinalField = ordinalIndex < 0 ? null : output.getVector(ordinalIndex);
        FieldVector kindField = kindIndex < 0 ? null : output.getVector(kindIndex);
        if (!(ordinalField instanceof IntVector) || !(kindField instanceof TinyIntVector)) {
            output.close();
            throw new IllegalStateException("Native OVER aggregate did not return timer envelope metadata");
        }
        RowKind[] rowKinds = new RowKind[output.getRowCount()];
        TinyIntVector kindVector = (TinyIntVector) kindField;
        for (int row = 0; row < output.getRowCount(); row++) {
            rowKinds[row] = RowKind.fromByteValue(kindVector.get(row));
        }
        VectorSchemaRoot withoutOrdinal = output.removeVector(ordinalIndex);
        ordinalField.close();
        VectorSchemaRoot visible = withoutOrdinal.removeVector(kindIndex);
        kindField.close();
        return ArrowRowDataBatch.wrap(visible, outputType, allocator)
                .withRowKinds(rowKinds)
                .withoutTimestamps();
    }

    private static VarBinaryVector preencodedKeys(List<byte[]> keys, int rows, BufferAllocator allocator) {
        if (keys.size() != rows) {
            throw new IllegalArgumentException("OVER aggregate key count does not match the batch");
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
