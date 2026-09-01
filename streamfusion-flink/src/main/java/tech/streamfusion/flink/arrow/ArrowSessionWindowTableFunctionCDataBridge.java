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
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import tech.streamfusion.nativebridge.NativeMemoryManager;
import tech.streamfusion.nativebridge.NativeSessionWindowTableFunctionBridge;

/** Arrow C Data transport for stateful SESSION Window TVF input and timer output. */
public final class ArrowSessionWindowTableFunctionCDataBridge {
    private ArrowSessionWindowTableFunctionCDataBridge() {}

    public static ArrowRowDataBatch process(
            long handle,
            ArrowRowDataBatch input,
            List<byte[]> preencodedKeys,
            List<byte[]> storedRows,
            long processingTime,
            RowType inputType,
            RowType outputType,
            BufferAllocator allocator,
            NativeMemoryManager memoryManager) {
        try (ArrowArray inputArray = ArrowArray.allocateNew(input.allocator());
                ArrowSchema inputSchema = ArrowSchema.allocateNew(input.allocator());
                ArrowArray outputArray = ArrowArray.allocateNew(allocator);
                ArrowSchema outputSchema = ArrowSchema.allocateNew(allocator);
                CDataDictionaryProvider dictionaries = new CDataDictionaryProvider()) {
            VarBinaryVector rows =
                    binaryMetadata("__streamfusion_stored_row", storedRows, input.size(), input.allocator());
            TinyIntVector kinds = inputKinds(input, input.allocator());
            VarBinaryVector keys = preencodedKeys == null
                    ? null
                    : binaryMetadata("__streamfusion_key", preencodedKeys, input.size(), input.allocator());
            try {
                VectorSchemaRoot exported = input.root();
                exported = exported.addVector(exported.getFieldVectors().size(), rows);
                exported = exported.addVector(exported.getFieldVectors().size(), kinds);
                if (keys != null) {
                    exported = exported.addVector(exported.getFieldVectors().size(), keys);
                }
                exported.setRowCount(input.size());
                Data.exportVectorSchemaRoot(input.allocator(), exported, null, inputArray, inputSchema);
                long count = NativeSessionWindowTableFunctionBridge.process(
                        handle,
                        inputArray.memoryAddress(),
                        inputSchema.memoryAddress(),
                        outputArray.memoryAddress(),
                        outputSchema.memoryAddress(),
                        processingTime);
                return importOutput(count, outputArray, outputSchema, inputType, outputType, allocator, dictionaries);
            } finally {
                memoryManager.finishArrowTransfer();
                if (keys != null) {
                    keys.close();
                }
                kinds.close();
                rows.close();
            }
        }
    }

    public static ArrowRowDataBatch advance(
            long handle,
            boolean processingTime,
            long timestamp,
            RowType inputType,
            RowType outputType,
            BufferAllocator allocator,
            NativeMemoryManager memoryManager) {
        try (ArrowArray outputArray = ArrowArray.allocateNew(allocator);
                ArrowSchema outputSchema = ArrowSchema.allocateNew(allocator);
                CDataDictionaryProvider dictionaries = new CDataDictionaryProvider()) {
            try {
                long count = NativeSessionWindowTableFunctionBridge.advance(
                        handle, processingTime, timestamp, outputArray.memoryAddress(), outputSchema.memoryAddress());
                return importOutput(count, outputArray, outputSchema, inputType, outputType, allocator, dictionaries);
            } finally {
                memoryManager.finishArrowTransfer();
            }
        }
    }

    private static ArrowRowDataBatch importOutput(
            long count,
            ArrowArray outputArray,
            ArrowSchema outputSchema,
            RowType inputType,
            RowType outputType,
            BufferAllocator allocator,
            CDataDictionaryProvider dictionaries) {
        if (count < 0 || count > Integer.MAX_VALUE) {
            throw new IllegalStateException("Native session Window TVF returned invalid row count " + count);
        }
        try (VectorSchemaRoot output =
                Data.importVectorSchemaRoot(allocator, outputArray, outputSchema, dictionaries)) {
            output.setRowCount((int) count);
            if (output.getFieldVectors().size() != 4
                    || !(output.getVector(0) instanceof VarBinaryVector)
                    || !(output.getVector(1) instanceof TimeStampMilliVector)
                    || !(output.getVector(2) instanceof TimeStampMilliVector)
                    || !(output.getVector(3) instanceof TinyIntVector)) {
                throw new IllegalStateException("Native session Window TVF returned invalid row metadata");
            }
            VarBinaryVector rows = (VarBinaryVector) output.getVector(0);
            TimeStampMilliVector starts = (TimeStampMilliVector) output.getVector(1);
            TimeStampMilliVector ends = (TimeStampMilliVector) output.getVector(2);
            TinyIntVector kinds = (TinyIntVector) output.getVector(3);
            List<RowData> values = new ArrayList<>((int) count);
            RowKind[] rowKinds = new RowKind[(int) count];
            for (int index = 0; index < count; index++) {
                byte[] bytes = rows.get(index);
                BinaryRowData row = new BinaryRowData(inputType.getFieldCount());
                row.pointTo(MemorySegmentFactory.wrap(bytes), 0, bytes.length);
                long start = starts.get(index);
                long end = ends.get(index);
                RowKind kind = RowKind.fromByteValue(kinds.get(index));
                row.setRowKind(kind);
                JoinedRowData joined = new JoinedRowData(
                        row,
                        GenericRowData.of(
                                TimestampData.fromEpochMillis(start),
                                TimestampData.fromEpochMillis(end),
                                TimestampData.fromEpochMillis(end - 1)));
                joined.setRowKind(kind);
                values.add(joined);
                rowKinds[index] = kind;
            }
            return ArrowRowDataBatch.transpose(values, outputType, allocator)
                    .withRowKinds(rowKinds)
                    .withoutTimestamps();
        }
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
}
