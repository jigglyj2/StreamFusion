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
import tech.streamfusion.nativebridge.NativeDeduplicateBridge;

/** Arrow C Data transport for one persistent native deduplicate handle. */
public final class ArrowDeduplicateCDataBridge {
    private ArrowDeduplicateCDataBridge() {}

    public static NativeDeduplicateResult execute(
            long handle,
            ArrowRowDataBatch input,
            List<byte[]> preencodedKeys,
            List<byte[]> storedRows,
            List<RowKind> inputRowKinds,
            BufferAllocator allocator) {
        try (ArrowArray inputArray = ArrowArray.allocateNew(allocator);
                ArrowSchema inputSchema = ArrowSchema.allocateNew(allocator);
                ArrowArray outputArray = ArrowArray.allocateNew(allocator);
                ArrowSchema outputSchema = ArrowSchema.allocateNew(allocator);
                CDataDictionaryProvider dictionaries = new CDataDictionaryProvider()) {
            VarBinaryVector keyVector =
                    preencodedKeys == null ? null : preencodedKeys(preencodedKeys, input.size(), allocator);
            VarBinaryVector storedRowVector = storedRows == null
                    ? null
                    : binaryMetadata("__streamfusion_stored_row", storedRows, input.size(), allocator);
            TinyIntVector inputKindVector =
                    inputRowKinds == null ? null : inputKinds(inputRowKinds, input.size(), allocator);
            long rowCount;
            try {
                VectorSchemaRoot exported = input.root();
                if (storedRowVector != null) {
                    exported = exported.addVector(exported.getFieldVectors().size(), storedRowVector);
                }
                if (inputKindVector != null) {
                    exported = exported.addVector(exported.getFieldVectors().size(), inputKindVector);
                }
                if (keyVector != null) {
                    exported = exported.addVector(exported.getFieldVectors().size(), keyVector);
                }
                exported.setRowCount(input.size());
                Data.exportVectorSchemaRoot(allocator, exported, null, inputArray, inputSchema);
                rowCount = NativeDeduplicateBridge.process(
                        handle,
                        inputArray.memoryAddress(),
                        inputSchema.memoryAddress(),
                        outputArray.memoryAddress(),
                        outputSchema.memoryAddress());
            } finally {
                if (keyVector != null) {
                    keyVector.close();
                }
                if (inputKindVector != null) {
                    inputKindVector.close();
                }
                if (storedRowVector != null) {
                    storedRowVector.close();
                }
            }
            if (rowCount < 0 || rowCount > Integer.MAX_VALUE) {
                throw new IllegalStateException("Native deduplicate returned invalid row count " + rowCount);
            }
            VectorSchemaRoot output = Data.importVectorSchemaRoot(allocator, outputArray, outputSchema, dictionaries);
            output.setRowCount((int) rowCount);
            return readSelection(output);
        }
    }

    /** Executes deduplicate directly on the internal Arrow transport. */
    public static NativeArrowDeduplicateResult executeArrow(
            long handle,
            ArrowRowDataBatch input,
            List<byte[]> preencodedKeys,
            RowType outputType,
            BufferAllocator allocator) {
        try (ArrowArray inputArray = ArrowArray.allocateNew(input.allocator());
                ArrowSchema inputSchema = ArrowSchema.allocateNew(input.allocator());
                ArrowArray outputArray = ArrowArray.allocateNew(allocator);
                ArrowSchema outputSchema = ArrowSchema.allocateNew(allocator);
                CDataDictionaryProvider dictionaries = new CDataDictionaryProvider()) {
            VarBinaryVector keyVector =
                    preencodedKeys == null ? null : preencodedKeys(preencodedKeys, input.size(), input.allocator());
            long rowCount;
            try {
                VectorSchemaRoot exported = input.root();
                if (keyVector != null) {
                    exported = exported.addVector(exported.getFieldVectors().size(), keyVector);
                    exported.setRowCount(input.size());
                }
                Data.exportVectorSchemaRoot(input.allocator(), exported, null, inputArray, inputSchema);
                rowCount = NativeDeduplicateBridge.processOutput(
                        handle,
                        inputArray.memoryAddress(),
                        inputSchema.memoryAddress(),
                        outputArray.memoryAddress(),
                        outputSchema.memoryAddress());
            } finally {
                if (keyVector != null) {
                    keyVector.close();
                }
            }
            if (rowCount < 0 || rowCount > Integer.MAX_VALUE) {
                throw new IllegalStateException("Native deduplicate returned invalid row count " + rowCount);
            }
            VectorSchemaRoot output = Data.importVectorSchemaRoot(allocator, outputArray, outputSchema, dictionaries);
            output.setRowCount((int) rowCount);
            return readArrowOutput(output, outputType, allocator);
        }
    }

    private static VarBinaryVector binaryMetadata(
            String name, List<byte[]> values, int rowCount, BufferAllocator allocator) {
        if (values.size() != rowCount) {
            throw new IllegalArgumentException(
                    "Deduplicate " + name + " count " + values.size() + " does not match row count " + rowCount);
        }
        VarBinaryVector vector = new VarBinaryVector(name, allocator);
        try {
            vector.allocateNew();
            for (int index = 0; index < rowCount; index++) {
                vector.setSafe(index, values.get(index));
            }
            vector.setValueCount(rowCount);
            return vector;
        } catch (RuntimeException | Error failure) {
            vector.close();
            throw failure;
        }
    }

    private static VarBinaryVector preencodedKeys(List<byte[]> keys, int rowCount, BufferAllocator allocator) {
        return binaryMetadata("__streamfusion_key", keys, rowCount, allocator);
    }

    private static TinyIntVector inputKinds(List<RowKind> kinds, int rowCount, BufferAllocator allocator) {
        if (kinds.size() != rowCount) {
            throw new IllegalArgumentException(
                    "Deduplicate RowKind count " + kinds.size() + " does not match row count " + rowCount);
        }
        TinyIntVector vector = new TinyIntVector("__streamfusion_input_row_kind", allocator);
        try {
            vector.allocateNew(rowCount);
            for (int index = 0; index < rowCount; index++) {
                vector.setSafe(index, kinds.get(index).toByteValue());
            }
            vector.setValueCount(rowCount);
            return vector;
        } catch (RuntimeException | Error failure) {
            vector.close();
            throw failure;
        }
    }

    private static NativeDeduplicateResult readSelection(VectorSchemaRoot output) {
        try {
            if (output.getFieldVectors().size() != 2 && output.getFieldVectors().size() != 3) {
                throw new IllegalStateException(
                        "Native deduplicate selection must contain exactly ordinal and RowKind metadata");
            }
            FieldVector ordinalField = output.getVector(0);
            FieldVector rowKindField = output.getVector(1);
            if (!(ordinalField instanceof IntVector) || !(rowKindField instanceof TinyIntVector)) {
                throw new IllegalStateException("Native deduplicate did not return ordinal and RowKind metadata");
            }
            int[] inputRows = new int[output.getRowCount()];
            RowKind[] rowKinds = new RowKind[output.getRowCount()];
            byte[][] storedRows = output.getFieldVectors().size() == 3 ? new byte[output.getRowCount()][] : null;
            IntVector ordinals = (IntVector) ordinalField;
            TinyIntVector kinds = (TinyIntVector) rowKindField;
            for (int index = 0; index < inputRows.length; index++) {
                inputRows[index] = ordinals.get(index);
                rowKinds[index] = RowKind.fromByteValue(kinds.get(index));
                if (storedRows != null) {
                    FieldVector storedField = output.getVector(2);
                    if (!(storedField instanceof VarBinaryVector)) {
                        throw new IllegalStateException("Native deduplicate stored-row metadata is not binary");
                    }
                    VarBinaryVector stored = (VarBinaryVector) storedField;
                    storedRows[index] = stored.isNull(index) ? null : stored.get(index);
                }
            }
            return new NativeDeduplicateResult(inputRows, rowKinds, storedRows);
        } finally {
            output.close();
        }
    }

    private static NativeArrowDeduplicateResult readArrowOutput(
            VectorSchemaRoot output, RowType outputType, BufferAllocator allocator) {
        int ordinalIndex = output.getFieldVectors().size() - 1;
        int rowKindIndex = ordinalIndex - 1;
        if (rowKindIndex < 0
                || !(output.getVector(ordinalIndex) instanceof IntVector)
                || !(output.getVector(rowKindIndex) instanceof TinyIntVector)) {
            output.close();
            throw new IllegalStateException("Native Arrow deduplicate did not return RowKind and ordinal metadata");
        }
        int[] inputRows = new int[output.getRowCount()];
        RowKind[] rowKinds = new RowKind[output.getRowCount()];
        IntVector ordinals = (IntVector) output.getVector(ordinalIndex);
        TinyIntVector kinds = (TinyIntVector) output.getVector(rowKindIndex);
        for (int index = 0; index < inputRows.length; index++) {
            inputRows[index] = ordinals.get(index);
            rowKinds[index] = RowKind.fromByteValue(kinds.get(index));
        }
        FieldVector ordinalVector = output.getVector(ordinalIndex);
        VectorSchemaRoot withoutOrdinal = output.removeVector(ordinalIndex);
        ordinalVector.close();
        FieldVector rowKindVector = withoutOrdinal.getVector(rowKindIndex);
        VectorSchemaRoot visible = withoutOrdinal.removeVector(rowKindIndex);
        rowKindVector.close();
        return new NativeArrowDeduplicateResult(
                ArrowRowDataBatch.wrap(visible, outputType, allocator), inputRows, rowKinds);
    }
}
