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
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import tech.streamfusion.nativebridge.NativeChangelogNormalizeBridge;
import tech.streamfusion.nativebridge.NativeMemoryManager;

/** One Arrow C Data call for native keyed changelog normalization. */
public final class ArrowChangelogNormalizeCDataBridge {
    private ArrowChangelogNormalizeCDataBridge() {}

    public static ArrowRowDataBatch execute(
            long handle,
            long nowMillis,
            ArrowRowDataBatch input,
            List<byte[]> preencodedKeys,
            boolean[] filterResults,
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
            BitVector filters = filterResults == null ? null : filterResults(input, filterResults);
            try {
                List<FieldVector> vectors = new ArrayList<>(input.root().getFieldVectors());
                vectors.add(kinds);
                if (filters != null) {
                    vectors.add(filters);
                }
                if (keys != null) {
                    vectors.add(keys);
                }
                VectorSchemaRoot exported = new VectorSchemaRoot(vectors);
                exported.setRowCount(input.size());
                Data.exportVectorSchemaRoot(input.allocator(), exported, null, inputArray, inputSchema);
                long count = NativeChangelogNormalizeBridge.process(
                        handle,
                        nowMillis,
                        inputArray.memoryAddress(),
                        inputSchema.memoryAddress(),
                        outputArray.memoryAddress(),
                        outputSchema.memoryAddress());
                if (count < 0 || count > Integer.MAX_VALUE) {
                    throw new IllegalStateException("Native changelog normalize returned invalid row count " + count);
                }
                VectorSchemaRoot output =
                        Data.importVectorSchemaRoot(allocator, outputArray, outputSchema, dictionaries);
                output.setRowCount((int) count);
                return removeMetadata(output, outputType, input, allocator);
            } finally {
                memoryManager.finishArrowTransfer();
                if (keys != null) {
                    keys.close();
                }
                if (filters != null) {
                    filters.close();
                }
                kinds.close();
            }
        }
    }

    private static ArrowRowDataBatch removeMetadata(
            VectorSchemaRoot output, RowType outputType, ArrowRowDataBatch input, BufferAllocator allocator) {
        int ordinalIndex = output.getFieldVectors().size() - 1;
        int rowKindIndex = ordinalIndex - 1;
        if (rowKindIndex < 0
                || !(output.getVector(ordinalIndex) instanceof IntVector)
                || !(output.getVector(rowKindIndex) instanceof TinyIntVector)) {
            output.close();
            throw new IllegalStateException("Native changelog normalize did not return RowKind and ordinal metadata");
        }
        int[] inputRows = new int[output.getRowCount()];
        RowKind[] rowKinds = new RowKind[output.getRowCount()];
        IntVector ordinals = (IntVector) output.getVector(ordinalIndex);
        TinyIntVector kinds = (TinyIntVector) output.getVector(rowKindIndex);
        for (int row = 0; row < output.getRowCount(); row++) {
            inputRows[row] = ordinals.get(row);
            rowKinds[row] = RowKind.fromByteValue(kinds.get(row));
        }
        FieldVector ordinal = output.getVector(ordinalIndex);
        VectorSchemaRoot withoutOrdinal = output.removeVector(ordinalIndex);
        ordinal.close();
        FieldVector rowKind = withoutOrdinal.getVector(rowKindIndex);
        VectorSchemaRoot visible = withoutOrdinal.removeVector(rowKindIndex);
        rowKind.close();
        return ArrowRowDataBatch.wrap(visible, outputType, allocator)
                .selectEnvelopeFrom(input, inputRows)
                .withRowKinds(rowKinds);
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

    private static BitVector filterResults(ArrowRowDataBatch input, boolean[] values) {
        if (values.length != input.size()) {
            throw new IllegalArgumentException("Changelog normalize filter count does not match its Arrow batch");
        }
        BitVector vector = new BitVector("__streamfusion_filter_result", input.allocator());
        vector.allocateNew(input.size());
        for (int row = 0; row < values.length; row++) {
            vector.setSafe(row, values[row] ? 1 : 0);
        }
        vector.setValueCount(values.length);
        return vector;
    }

    private static VarBinaryVector preencodedKeys(ArrowRowDataBatch input, List<byte[]> values) {
        if (values.size() != input.size()) {
            throw new IllegalArgumentException("Changelog normalize key count does not match its Arrow batch");
        }
        VarBinaryVector vector = new VarBinaryVector("__streamfusion_key", input.allocator());
        vector.allocateNew();
        for (int row = 0; row < values.size(); row++) {
            vector.setSafe(row, values.get(row));
        }
        vector.setValueCount(values.size());
        return vector;
    }
}
