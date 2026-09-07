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
import tech.streamfusion.nativebridge.NativeRegularJoinBridge;

/** Bounded terminal output and metadata helpers for the regular join Arrow boundary. */
public final class ArrowRegularJoinCDataBridge {
    private ArrowRegularJoinCDataBridge() {}

    /** Drains one bounded native join result batch after both inputs have ended. */
    public static ArrowRowDataBatch finish(
            long handle, RowType outputType, BufferAllocator allocator, NativeMemoryManager memoryManager) {
        try (ArrowArray outputArray = ArrowArray.allocateNew(allocator);
                ArrowSchema outputSchema = ArrowSchema.allocateNew(allocator);
                CDataDictionaryProvider dictionaries = new CDataDictionaryProvider()) {
            try {
                long count = NativeRegularJoinBridge.finish(
                        handle, outputArray.memoryAddress(), outputSchema.memoryAddress());
                if (count < 0 || count > Integer.MAX_VALUE) {
                    throw new IllegalStateException("Native regular join returned invalid terminal row count " + count);
                }
                VectorSchemaRoot output =
                        Data.importVectorSchemaRoot(allocator, outputArray, outputSchema, dictionaries);
                output.setRowCount((int) count);
                return removeMetadata(output, outputType, allocator, true);
            } finally {
                memoryManager.finishArrowTransfer();
            }
        }
    }

    static ArrowRowDataBatch removeMetadata(
            VectorSchemaRoot output, RowType outputType, BufferAllocator allocator, boolean insertOnly) {
        if (output.getFieldVectors().size() == outputType.getFieldCount() + 3) {
            // Compatibility edge for a plan with downstream stage record policies. The
            // shared owned-envelope reader validates/version-checks metadata and transfers
            // its payload owner; do not duplicate a join-specific envelope implementation.
            return NativePlanOutputEnvelope.read(output, outputType, allocator).batch();
        }
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
        TinyIntVector kindVector = (TinyIntVector) output.getVector(rowKindIndex);
        RowKind[] kinds = null;
        if (!insertOnly) {
            kinds = new RowKind[output.getRowCount()];
            for (int row = 0; row < output.getRowCount(); row++) {
                kinds[row] = RowKind.fromByteValue(kindVector.get(row));
            }
        }
        FieldVector ordinal = output.getVector(ordinalIndex);
        VectorSchemaRoot withoutOrdinal = output.removeVector(ordinalIndex);
        ordinal.close();
        FieldVector kind = withoutOrdinal.getVector(rowKindIndex);
        VectorSchemaRoot visible = withoutOrdinal.removeVector(rowKindIndex);
        kind.close();
        ArrowRowDataBatch batch = ArrowRowDataBatch.wrap(visible, outputType, allocator);
        if (kinds != null) {
            batch.withRowKinds(kinds);
        }
        return batch.withoutTimestamps();
    }

    static VarBinaryVector preencodedKeys(BufferAllocator allocator, int rowCount, List<byte[]> values) {
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
