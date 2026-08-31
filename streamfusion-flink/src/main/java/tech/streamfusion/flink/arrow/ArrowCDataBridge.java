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
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.nativebridge.NativeCalcBridge;
import tech.streamfusion.nativebridge.NativeExecutionContext;

/** Ownership-safe Arrow C Data transfer for one native execution batch. */
public final class ArrowCDataBridge {
    private ArrowCDataBridge() {}

    /** Task-lifetime C Data executor that negotiates stable input/output schemas once. */
    public static final class ReusableExecution {
        private final NativeExecutionContext context;
        private final RowType outputType;
        private final BufferAllocator allocator;
        private Schema inputSchema;
        private Schema outputSchema;

        public ReusableExecution(NativeExecutionContext context, RowType outputType, BufferAllocator allocator) {
            this.context = context;
            this.outputType = outputType;
            this.allocator = allocator;
        }

        public NativeCalcResult executeWithSelection(ArrowRowDataBatch input) {
            VectorSchemaRoot output = executeNative(input);
            return removeSelection(output, outputType, allocator);
        }

        /** Executes a batch whose row envelope is position-independent. */
        public ArrowRowDataBatch execute(ArrowRowDataBatch input) {
            VectorSchemaRoot output = executeNative(input);
            return removeUnusedSelection(output, outputType, allocator);
        }

        private VectorSchemaRoot executeNative(ArrowRowDataBatch input) {
            Schema currentInputSchema = input.root().getSchema();
            if (inputSchema != null && !inputSchema.equals(currentInputSchema)) {
                throw new IllegalStateException("Arrow input schema changed after native negotiation");
            }
            boolean negotiate = inputSchema == null;
            BufferAllocator inputAllocator = input.allocator();
            try (ArrowArray inputArray = ArrowArray.allocateNew(inputAllocator);
                    ArrowArray outputArray = ArrowArray.allocateNew(allocator);
                    ArrowSchema inputSchemaHandle = negotiate ? ArrowSchema.allocateNew(inputAllocator) : null;
                    ArrowSchema outputSchemaHandle = negotiate ? ArrowSchema.allocateNew(allocator) : null;
                    CDataDictionaryProvider dictionaries = new CDataDictionaryProvider()) {
                if (negotiate) {
                    Data.exportVectorSchemaRoot(inputAllocator, input.root(), null, inputArray, inputSchemaHandle);
                } else {
                    Data.exportVectorSchemaRoot(inputAllocator, input.root(), null, inputArray);
                }
                long rowCount = NativeCalcBridge.executeArrow(
                        context,
                        inputArray.memoryAddress(),
                        negotiate ? inputSchemaHandle.memoryAddress() : 0,
                        outputArray.memoryAddress(),
                        negotiate ? outputSchemaHandle.memoryAddress() : 0);
                validateRowCount(rowCount);
                VectorSchemaRoot output;
                if (negotiate) {
                    output = Data.importVectorSchemaRoot(allocator, outputArray, outputSchemaHandle, dictionaries);
                    inputSchema = currentInputSchema;
                    outputSchema = output.getSchema();
                } else {
                    output = VectorSchemaRoot.create(outputSchema, allocator);
                    try {
                        Data.importIntoVectorSchemaRoot(allocator, outputArray, output, dictionaries);
                    } catch (RuntimeException | Error failure) {
                        output.close();
                        throw failure;
                    }
                }
                output.setRowCount((int) rowCount);
                return output;
            }
        }
    }

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

    private static ArrowRowDataBatch removeUnusedSelection(
            VectorSchemaRoot output, RowType outputType, BufferAllocator allocator) {
        int ordinalIndex = output.getFieldVectors().size() - 1;
        FieldVector ordinalVector = output.getVector(ordinalIndex);
        if (!(ordinalVector instanceof IntVector)) {
            output.close();
            throw new IllegalStateException("Native calc did not return its INT input-row ordinal");
        }
        VectorSchemaRoot visibleOutput = output.removeVector(ordinalIndex);
        ordinalVector.close();
        return ArrowRowDataBatch.wrap(visibleOutput, outputType, allocator);
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
            validateRowCount(rowCount);
            VectorSchemaRoot output = Data.importVectorSchemaRoot(allocator, outputArray, outputSchema, dictionaries);
            output.setRowCount((int) rowCount);
            return output;
        }
    }

    private static void validateRowCount(long rowCount) {
        if (rowCount < 0 || rowCount > Integer.MAX_VALUE) {
            throw new IllegalStateException("Native calc returned invalid row count " + rowCount);
        }
    }

    @FunctionalInterface
    private interface NativeCalcInvocation {
        long execute(long inputArray, long inputSchema, long outputArray, long outputSchema);
    }
}
