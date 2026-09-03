/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.arrow;

import java.util.ArrayList;
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
import tech.streamfusion.flink.exchange.ArrowExchangeInputBatch;
import tech.streamfusion.nativebridge.NativeIntervalJoinBridge;
import tech.streamfusion.nativebridge.NativeMemoryManager;

/** One Arrow C Data call for an interval-join input batch or timer firing. */
public final class ArrowIntervalJoinCDataBridge {
    private ArrowIntervalJoinCDataBridge() {}

    public static ArrowRowDataBatch process(
            long handle,
            int side,
            long processingTime,
            ArrowExchangeInputBatch input,
            List<byte[]> preencodedKeys,
            RowType outputType,
            BufferAllocator allocator,
            NativeMemoryManager memoryManager) {
        try (ArrowArray inputArray = ArrowArray.allocateNew(allocator);
                ArrowSchema inputSchema = ArrowSchema.allocateNew(allocator);
                ArrowArray outputArray = ArrowArray.allocateNew(allocator);
                ArrowSchema outputSchema = ArrowSchema.allocateNew(allocator);
                CDataDictionaryProvider dictionaries = new CDataDictionaryProvider()) {
            VarBinaryVector keys =
                    preencodedKeys == null ? null : preencodedKeys(allocator, input.size(), preencodedKeys);
            try {
                VectorSchemaRoot exported = input.transportRoot();
                if (keys != null) {
                    List<FieldVector> vectors = new ArrayList<>(exported.getFieldVectors());
                    vectors.add(keys);
                    exported = new VectorSchemaRoot(vectors);
                    exported.setRowCount(input.size());
                }
                Data.exportVectorSchemaRoot(allocator, exported, null, inputArray, inputSchema);
                long count = NativeIntervalJoinBridge.process(
                        handle,
                        side,
                        processingTime,
                        inputArray.memoryAddress(),
                        inputSchema.memoryAddress(),
                        outputArray.memoryAddress(),
                        outputSchema.memoryAddress());
                return importOutput(count, outputArray, outputSchema, outputType, allocator, dictionaries);
            } finally {
                memoryManager.finishArrowTransfer();
                if (keys != null) {
                    keys.close();
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
                long count = NativeIntervalJoinBridge.advance(
                        handle, processingTime, timestamp, outputArray.memoryAddress(), outputSchema.memoryAddress());
                return importOutput(count, outputArray, outputSchema, outputType, allocator, dictionaries);
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
            CDataDictionaryProvider dictionaries) {
        if (count < 0 || count > Integer.MAX_VALUE) {
            throw new IllegalStateException("Native interval join returned invalid row count " + count);
        }
        VectorSchemaRoot output = Data.importVectorSchemaRoot(allocator, outputArray, outputSchema, dictionaries);
        output.setRowCount((int) count);
        int ordinalIndex = output.getFieldVectors().size() - 1;
        int rowKindIndex = ordinalIndex - 1;
        if (rowKindIndex < 0
                || rowKindIndex != outputType.getFieldCount()
                || !(output.getVector(ordinalIndex) instanceof IntVector)
                || !(output.getVector(rowKindIndex) instanceof TinyIntVector)) {
            output.close();
            throw new IllegalStateException("Native interval join returned invalid changelog metadata");
        }
        RowKind[] kinds = new RowKind[output.getRowCount()];
        TinyIntVector kindVector = (TinyIntVector) output.getVector(rowKindIndex);
        for (int row = 0; row < output.getRowCount(); row++) {
            kinds[row] = RowKind.fromByteValue(kindVector.get(row));
        }
        FieldVector ordinal = output.getVector(ordinalIndex);
        VectorSchemaRoot withoutOrdinal = output.removeVector(ordinalIndex);
        ordinal.close();
        FieldVector kind = withoutOrdinal.getVector(rowKindIndex);
        VectorSchemaRoot visible = withoutOrdinal.removeVector(rowKindIndex);
        kind.close();
        return ArrowRowDataBatch.wrap(visible, outputType, allocator)
                .withRowKinds(kinds)
                .withoutTimestamps();
    }

    private static VarBinaryVector preencodedKeys(BufferAllocator allocator, int rowCount, List<byte[]> values) {
        if (values.size() != rowCount) {
            throw new IllegalArgumentException("Interval join key count does not match its Arrow batch");
        }
        VarBinaryVector vector = new VarBinaryVector("__streamfusion_key", allocator);
        vector.allocateNew();
        for (int row = 0; row < values.size(); row++) {
            vector.setSafe(row, values.get(row));
        }
        vector.setValueCount(values.size());
        return vector;
    }
}
