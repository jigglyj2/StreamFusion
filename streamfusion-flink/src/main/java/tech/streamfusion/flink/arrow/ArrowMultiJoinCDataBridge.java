/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.arrow;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
import tech.streamfusion.nativebridge.NativeMultiJoinBridge;

/** One Arrow C Data call for one multi-join input batch and its complete changelog output. */
public final class ArrowMultiJoinCDataBridge {
    private ArrowMultiJoinCDataBridge() {}

    public static ArrowRowDataBatch execute(
            long handle,
            int input,
            ArrowExchangeInputBatch batch,
            List<byte[]> preencodedKeys,
            Map<Integer, List<byte[]>> conditionValues,
            RowType outputType,
            BufferAllocator allocator,
            NativeMemoryManager memoryManager) {
        try (ArrowArray inputArray = ArrowArray.allocateNew(allocator);
                ArrowSchema inputSchema = ArrowSchema.allocateNew(allocator);
                ArrowArray outputArray = ArrowArray.allocateNew(allocator);
                ArrowSchema outputSchema = ArrowSchema.allocateNew(allocator);
                CDataDictionaryProvider dictionaries = new CDataDictionaryProvider()) {
            VarBinaryVector keys = preencodedKeys == null
                    ? null
                    : binaryVector(allocator, "__streamfusion_key", batch.size(), preencodedKeys, false);
            List<VarBinaryVector> conditions = new ArrayList<>(conditionValues.size());
            try {
                List<FieldVector> vectors =
                        new ArrayList<>(batch.transportRoot().getFieldVectors());
                if (keys != null) {
                    vectors.add(keys);
                }
                conditionValues.entrySet().stream()
                        .sorted(Map.Entry.comparingByKey())
                        .forEach(entry -> {
                            VarBinaryVector vector = binaryVector(
                                    allocator,
                                    "__streamfusion_condition_" + entry.getKey(),
                                    batch.size(),
                                    entry.getValue(),
                                    true);
                            conditions.add(vector);
                            vectors.add(vector);
                        });
                VectorSchemaRoot exported = new VectorSchemaRoot(vectors);
                exported.setRowCount(batch.size());
                Data.exportVectorSchemaRoot(allocator, exported, null, inputArray, inputSchema);
                long count = NativeMultiJoinBridge.process(
                        handle,
                        input,
                        inputArray.memoryAddress(),
                        inputSchema.memoryAddress(),
                        outputArray.memoryAddress(),
                        outputSchema.memoryAddress());
                if (count < 0 || count > Integer.MAX_VALUE) {
                    throw new IllegalStateException("Native multi-join returned invalid row count " + count);
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
                conditions.forEach(VarBinaryVector::close);
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
            throw new IllegalStateException("Native multi-join did not return RowKind and ordinal metadata");
        }
        if (rowKindIndex != outputType.getFieldCount()) {
            output.close();
            throw new IllegalStateException("Native multi-join output arity does not match the Flink row type");
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

    private static VarBinaryVector binaryVector(
            BufferAllocator allocator, String name, int rowCount, List<byte[]> values, boolean nullable) {
        if (values.size() != rowCount) {
            throw new IllegalArgumentException(name + " count does not match its Arrow batch");
        }
        VarBinaryVector vector = new VarBinaryVector(name, allocator);
        vector.allocateNew();
        for (int row = 0; row < values.size(); row++) {
            byte[] value = values.get(row);
            if (value == null) {
                if (!nullable) {
                    vector.close();
                    throw new IllegalArgumentException(name + " cannot contain nulls");
                }
                vector.setNull(row);
            } else {
                vector.setSafe(row, value);
            }
        }
        vector.setValueCount(values.size());
        return vector;
    }
}
