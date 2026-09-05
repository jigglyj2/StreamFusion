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
import org.apache.flink.types.RowKind;
import tech.streamfusion.nativebridge.NativeMemoryManager;

/** Shared Arrow C Data transport for keyed operators that emit changelog rows from event-time timers. */
final class ArrowKeyedTimerCDataBridge {
    private ArrowKeyedTimerCDataBridge() {}

    static ArrowRowDataBatch process(
            long handle,
            ArrowRowDataBatch input,
            List<byte[]> preencodedKeys,
            RowType outputType,
            BufferAllocator allocator,
            NativeMemoryManager memoryManager,
            BatchProcessor processor,
            String operatorName) {
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
                long count = processor.process(
                        handle,
                        inputArray.memoryAddress(),
                        inputSchema.memoryAddress(),
                        outputArray.memoryAddress(),
                        outputSchema.memoryAddress());
                return importOutput(
                        count, outputArray, outputSchema, outputType, allocator, dictionaries, operatorName);
            } finally {
                memoryManager.finishArrowTransfer();
                if (keys != null) {
                    keys.close();
                }
                kinds.close();
            }
        }
    }

    static ArrowRowDataBatch advance(
            long handle,
            long watermark,
            RowType outputType,
            BufferAllocator allocator,
            NativeMemoryManager memoryManager,
            TimeAdvancer advancer,
            String operatorName) {
        try (ArrowArray outputArray = ArrowArray.allocateNew(allocator);
                ArrowSchema outputSchema = ArrowSchema.allocateNew(allocator);
                CDataDictionaryProvider dictionaries = new CDataDictionaryProvider()) {
            try {
                long count =
                        advancer.advance(handle, watermark, outputArray.memoryAddress(), outputSchema.memoryAddress());
                return importOutput(
                        count, outputArray, outputSchema, outputType, allocator, dictionaries, operatorName);
            } finally {
                memoryManager.finishArrowTransfer();
            }
        }
    }

    private static ArrowRowDataBatch importOutput(
            long count,
            ArrowArray outputArray,
            ArrowSchema outputSchema,
            RowType outputType,
            BufferAllocator allocator,
            CDataDictionaryProvider dictionaries,
            String operatorName) {
        if (count < 0 || count > Integer.MAX_VALUE) {
            throw new IllegalStateException("Native " + operatorName + " returned invalid row count " + count);
        }
        VectorSchemaRoot output = Data.importVectorSchemaRoot(allocator, outputArray, outputSchema, dictionaries);
        output.setRowCount((int) count);
        int rowKindIndex = output.getFieldVectors().size() - 1;
        if (rowKindIndex < 0 || !(output.getVector(rowKindIndex) instanceof TinyIntVector)) {
            output.close();
            throw new IllegalStateException("Native " + operatorName + " returned invalid RowKind metadata");
        }
        TinyIntVector kinds = (TinyIntVector) output.getVector(rowKindIndex);
        RowKind[] rowKinds = new RowKind[(int) count];
        for (int index = 0; index < count; index++) {
            rowKinds[index] = RowKind.fromByteValue(kinds.get(index));
        }
        VectorSchemaRoot visible = output.removeVector(rowKindIndex);
        kinds.close();
        return ArrowRowDataBatch.wrap(visible, outputType, allocator)
                .withRowKinds(rowKinds)
                .withoutTimestamps();
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

    @FunctionalInterface
    interface BatchProcessor {
        long process(long handle, long inputArray, long inputSchema, long outputArray, long outputSchema);
    }

    @FunctionalInterface
    interface TimeAdvancer {
        long advance(long handle, long timestamp, long outputArray, long outputSchema);
    }
}
