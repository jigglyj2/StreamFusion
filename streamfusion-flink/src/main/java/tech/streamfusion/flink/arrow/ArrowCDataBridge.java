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
import org.apache.arrow.c.ArrowArrayStream;
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
import tech.streamfusion.nativebridge.NativeMemoryManager;

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

        /** Executes one input through the Arrow C Stream output boundary. */
        public NativeOutputStream executeStream(ArrowRowDataBatch input) {
            Schema currentInputSchema = input.root().getSchema();
            if (inputSchema != null && !inputSchema.equals(currentInputSchema)) {
                throw new IllegalStateException("Arrow input schema changed after native negotiation");
            }
            boolean negotiate = inputSchema == null;
            BufferAllocator inputAllocator = input.allocator();
            try (ArrowArray inputArray = ArrowArray.allocateNew(inputAllocator);
                    ArrowSchema inputSchemaHandle = negotiate ? ArrowSchema.allocateNew(inputAllocator) : null) {
                if (negotiate) {
                    Data.exportVectorSchemaRoot(inputAllocator, input.root(), null, inputArray, inputSchemaHandle);
                } else {
                    Data.exportVectorSchemaRoot(inputAllocator, input.root(), null, inputArray);
                }
                ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator);
                try {
                    NativeCalcBridge.executeArrowStream(
                            context,
                            inputArray.memoryAddress(),
                            negotiate ? inputSchemaHandle.memoryAddress() : 0,
                            stream.memoryAddress());
                    NativeOutputStream output = new NativeOutputStream(stream, outputType, allocator, outputSchema);
                    if (outputSchema == null) {
                        outputSchema = output.schema();
                    }
                    inputSchema = currentInputSchema;
                    return output;
                } catch (RuntimeException | Error failure) {
                    releaseStream(stream);
                    throw failure;
                }
            }
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

    /** Pull-based ownership wrapper over one native Arrow C Stream invocation. */
    public static final class NativeOutputStream implements AutoCloseable {
        private final ArrowArrayStream stream;
        private final RowType outputType;
        private final BufferAllocator allocator;
        private final CDataDictionaryProvider dictionaries = new CDataDictionaryProvider();
        private final Schema schema;
        private boolean closed;

        private NativeOutputStream(
                ArrowArrayStream stream, RowType outputType, BufferAllocator allocator, Schema expectedSchema) {
            this.stream = stream;
            this.outputType = outputType;
            this.allocator = allocator;
            try (ArrowSchema schemaHandle = ArrowSchema.allocateNew(allocator)) {
                stream.getSchema(schemaHandle);
                this.schema = Data.importSchema(allocator, schemaHandle, dictionaries);
            } catch (java.io.IOException error) {
                throw new IllegalStateException("Failed to import native Arrow stream schema", error);
            }
            if (expectedSchema != null && !expectedSchema.equals(schema)) {
                throw new IllegalStateException("Arrow output schema changed after native negotiation");
            }
        }

        private Schema schema() {
            return schema;
        }

        public ArrowRowDataBatch next() {
            VectorSchemaRoot root = nextRoot();
            return root == null ? null : removeUnusedSelection(root, outputType, allocator);
        }

        public NativeCalcResult nextWithSelection() {
            VectorSchemaRoot root = nextRoot();
            return root == null ? null : removeSelection(root, outputType, allocator);
        }

        private VectorSchemaRoot nextRoot() {
            try (ArrowArray array = ArrowArray.allocateNew(allocator)) {
                stream.getNext(array);
                ArrowArray.Snapshot snapshot = array.snapshot();
                if (snapshot.release == 0) {
                    return null;
                }
                if (snapshot.length < 0 || snapshot.length > Integer.MAX_VALUE) {
                    throw new IllegalStateException("Native Arrow stream returned an invalid row count");
                }
                VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
                try {
                    Data.importIntoVectorSchemaRoot(allocator, array, root, dictionaries);
                    root.setRowCount((int) snapshot.length);
                    return root;
                } catch (RuntimeException | Error failure) {
                    root.close();
                    throw failure;
                }
            } catch (java.io.IOException error) {
                throw new IllegalStateException("Failed to read native Arrow stream", error);
            }
        }

        @Override
        public void close() {
            if (!closed) {
                closed = true;
                try {
                    releaseStream(stream);
                } finally {
                    dictionaries.close();
                }
            }
        }
    }

    private static void releaseStream(ArrowArrayStream stream) {
        try {
            if (stream.snapshot().release != 0) {
                stream.release();
            }
        } finally {
            stream.close();
        }
    }

    public static ArrowRowDataBatch execute(
            byte[] serializedPlan,
            ArrowRowDataBatch input,
            RowType outputType,
            BufferAllocator allocator,
            NativeMemoryManager memoryManager) {
        VectorSchemaRoot output = executeNative(
                input,
                allocator,
                (inputArray, inputSchema, outputArray, outputSchema) -> NativeCalcBridge.executeArrow(
                        serializedPlan, inputArray, inputSchema, outputArray, outputSchema, memoryManager));
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
            byte[] serializedPlan,
            ArrowRowDataBatch input,
            RowType outputType,
            BufferAllocator allocator,
            NativeMemoryManager memoryManager) {
        VectorSchemaRoot output = executeNative(
                input,
                allocator,
                (inputArray, inputSchema, outputArray, outputSchema) -> NativeCalcBridge.executeArrow(
                        serializedPlan, inputArray, inputSchema, outputArray, outputSchema, memoryManager));
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
