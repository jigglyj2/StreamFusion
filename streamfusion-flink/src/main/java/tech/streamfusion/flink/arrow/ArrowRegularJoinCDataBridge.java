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
import tech.streamfusion.nativebridge.NativeMemoryManager;
import tech.streamfusion.nativebridge.NativeRegularJoinBridge;

/** One Arrow C Data call for a regular join input batch and its complete changelog output. */
public final class ArrowRegularJoinCDataBridge {
    private ArrowRegularJoinCDataBridge() {}

    public static ArrowRowDataBatch execute(
            long handle,
            int side,
            ArrowRowDataBatch input,
            List<byte[]> preencodedKeys,
            RowType outputType,
            BufferAllocator allocator,
            NativeMemoryManager memoryManager) {
        try (ArrowArray inputArray = ArrowArray.allocateNew(input.allocator());
                ArrowSchema inputSchema = ArrowSchema.allocateNew(input.allocator());
                ArrowArray outputArray = ArrowArray.allocateNew(allocator);
                ArrowSchema outputSchema = ArrowSchema.allocateNew(allocator);
                CDataDictionaryProvider dictionaries = new CDataDictionaryProvider()) {
            TinyIntVector kinds = inputKinds(input);
            VarBinaryVector keys = preencodedKeys == null ? null : preencodedKeys(input, preencodedKeys);
            try {
                List<FieldVector> vectors = new ArrayList<>(input.root().getFieldVectors());
                vectors.add(kinds);
                if (keys != null) {
                    vectors.add(keys);
                }
                VectorSchemaRoot exported = new VectorSchemaRoot(vectors);
                exported.setRowCount(input.size());
                Data.exportVectorSchemaRoot(input.allocator(), exported, null, inputArray, inputSchema);
                long count = NativeRegularJoinBridge.process(
                        handle,
                        side,
                        inputArray.memoryAddress(),
                        inputSchema.memoryAddress(),
                        outputArray.memoryAddress(),
                        outputSchema.memoryAddress());
                if (count < 0 || count > Integer.MAX_VALUE) {
                    throw new IllegalStateException("Native regular join returned invalid row count " + count);
                }
                VectorSchemaRoot output =
                        Data.importVectorSchemaRoot(allocator, outputArray, outputSchema, dictionaries);
                output.setRowCount((int) count);
                return removeMetadata(output, outputType, allocator);
            } finally {
                memoryManager.finishArrowTransfer();
                if (keys != null) {
                    keys.close();
                }
                kinds.close();
            }
        }
    }

    /** Exports the received exchange vectors directly, retaining their native RowKind metadata. */
    public static ArrowRowDataBatch execute(
            long handle,
            int side,
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
                long count = NativeRegularJoinBridge.process(
                        handle,
                        side,
                        inputArray.memoryAddress(),
                        inputSchema.memoryAddress(),
                        outputArray.memoryAddress(),
                        outputSchema.memoryAddress());
                if (count < 0 || count > Integer.MAX_VALUE) {
                    throw new IllegalStateException("Native regular join returned invalid row count " + count);
                }
                VectorSchemaRoot output =
                        Data.importVectorSchemaRoot(allocator, outputArray, outputSchema, dictionaries);
                output.setRowCount((int) count);
                return removeMetadata(output, outputType, allocator);
            } finally {
                memoryManager.finishArrowTransfer();
                if (keys != null) {
                    keys.close();
                }
            }
        }
    }

    private static ArrowRowDataBatch removeMetadata(
            VectorSchemaRoot output, RowType outputType, BufferAllocator allocator) {
        int ordinalIndex = output.getFieldVectors().size() - 1;
        int rowKindIndex = ordinalIndex - 1;
        if (rowKindIndex < 0
                || !(output.getVector(ordinalIndex) instanceof IntVector)
                || !(output.getVector(rowKindIndex) instanceof TinyIntVector)) {
            output.close();
            throw new IllegalStateException("Native regular join did not return RowKind and ordinal metadata");
        }
        if (rowKindIndex != outputType.getFieldCount()) {
            output.close();
            throw new IllegalStateException("Native regular join output arity does not match the Flink row type");
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

    private static TinyIntVector inputKinds(ArrowRowDataBatch input) {
        TinyIntVector vector = new TinyIntVector("__streamfusion_input_row_kind", input.allocator());
        vector.allocateNew(input.size());
        for (int row = 0; row < input.size(); row++) {
            vector.setSafe(row, input.rowKind(row).toByteValue());
        }
        vector.setValueCount(input.size());
        return vector;
    }

    private static VarBinaryVector preencodedKeys(ArrowRowDataBatch input, List<byte[]> values) {
        return preencodedKeys(input.allocator(), input.size(), values);
    }

    private static VarBinaryVector preencodedKeys(BufferAllocator allocator, int rowCount, List<byte[]> values) {
        if (values.size() != rowCount) {
            throw new IllegalArgumentException("Regular join key count does not match its Arrow batch");
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
