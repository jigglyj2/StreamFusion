/*
 * Copyright 2026 StreamFusion Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package tech.streamfusion.flink.arrow;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.nativebridge.NativeCalcBridge;
import tech.streamfusion.nativebridge.NativeExecutionContext;

/** Ownership-safe Arrow C Data transfer for one native execution batch. */
public final class ArrowCDataBridge {
    private ArrowCDataBridge() {}

    public static ArrowRowDataBatch execute(
            byte[] serializedPlan, ArrowRowDataBatch input, RowType outputType, BufferAllocator allocator) {
        VectorSchemaRoot output = executeNative(
                input,
                allocator,
                (inputArray, inputSchema, outputArray, outputSchema) -> NativeCalcBridge.executeArrow(
                        serializedPlan, inputArray, inputSchema, outputArray, outputSchema));
        return ArrowRowDataBatch.wrap(output, outputType, allocator);
    }

    public static ArrowRowDataBatch execute(
            NativeExecutionContext context, ArrowRowDataBatch input, RowType outputType, BufferAllocator allocator) {
        VectorSchemaRoot output = executeNative(
                input,
                allocator,
                (inputArray, inputSchema, outputArray, outputSchema) ->
                        NativeCalcBridge.executeArrow(context, inputArray, inputSchema, outputArray, outputSchema));
        return ArrowRowDataBatch.wrap(output, outputType, allocator);
    }

    public static NativeCalcResult executeWithSelection(
            byte[] serializedPlan, ArrowRowDataBatch input, RowType outputType, BufferAllocator allocator) {
        VectorSchemaRoot output = executeNative(
                input,
                allocator,
                (inputArray, inputSchema, outputArray, outputSchema) -> NativeCalcBridge.executeArrow(
                        serializedPlan, inputArray, inputSchema, outputArray, outputSchema));
        return removeSelection(output, outputType, allocator);
    }

    public static NativeCalcResult executeWithSelection(
            NativeExecutionContext context, ArrowRowDataBatch input, RowType outputType, BufferAllocator allocator) {
        VectorSchemaRoot output = executeNative(
                input,
                allocator,
                (inputArray, inputSchema, outputArray, outputSchema) ->
                        NativeCalcBridge.executeArrow(context, inputArray, inputSchema, outputArray, outputSchema));
        return removeSelection(output, outputType, allocator);
    }

    private static NativeCalcResult removeSelection(
            VectorSchemaRoot output, RowType outputType, BufferAllocator allocator) {
        int ordinalIndex = output.getFieldVectors().size() - 1;
        FieldVector ordinalVector = output.getVector(ordinalIndex);
        if (!(ordinalVector instanceof IntVector)) {
            output.close();
            throw new IllegalStateException("Native calc did not return its INT input-row ordinal");
        }
        int[] inputRows = new int[output.getRowCount()];
        IntVector ordinals = (IntVector) ordinalVector;
        for (int index = 0; index < inputRows.length; index++) {
            inputRows[index] = ordinals.get(index);
        }
        VectorSchemaRoot visibleOutput = output.removeVector(ordinalIndex);
        ordinalVector.close();
        return new NativeCalcResult(ArrowRowDataBatch.wrap(visibleOutput, outputType, allocator), inputRows);
    }

    private static VectorSchemaRoot executeNative(
            ArrowRowDataBatch input, BufferAllocator allocator, NativeCalcInvocation invocation) {
        BufferAllocator inputAllocator = input.allocator();
        try (ArrowArray inputArray = ArrowArray.allocateNew(inputAllocator);
                ArrowSchema inputSchema = ArrowSchema.allocateNew(inputAllocator);
                ArrowArray outputArray = ArrowArray.allocateNew(allocator);
                ArrowSchema outputSchema = ArrowSchema.allocateNew(allocator);
                CDataDictionaryProvider dictionaries = new CDataDictionaryProvider()) {
            Data.exportVectorSchemaRoot(inputAllocator, input.root(), null, inputArray, inputSchema);
            long rowCount = invocation.execute(
                    inputArray.memoryAddress(),
                    inputSchema.memoryAddress(),
                    outputArray.memoryAddress(),
                    outputSchema.memoryAddress());
            if (rowCount < 0 || rowCount > Integer.MAX_VALUE) {
                throw new IllegalStateException("Native calc returned invalid row count " + rowCount);
            }
            VectorSchemaRoot output = Data.importVectorSchemaRoot(allocator, outputArray, outputSchema, dictionaries);
            output.setRowCount((int) rowCount);
            return output;
        }
    }

    @FunctionalInterface
    private interface NativeCalcInvocation {
        long execute(long inputArray, long inputSchema, long outputArray, long outputSchema);
    }
}
