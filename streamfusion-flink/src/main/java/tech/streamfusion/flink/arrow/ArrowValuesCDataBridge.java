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
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.nativebridge.NativeExecutionContext;
import tech.streamfusion.nativebridge.NativeValuesBridge;

/** Imports a source-free native VALUES result through Arrow C Data. */
public final class ArrowValuesCDataBridge {
    private ArrowValuesCDataBridge() {}

    public static ArrowRowDataBatch execute(byte[] serializedPlan, RowType outputType, BufferAllocator allocator) {
        return execute(
                outputType,
                allocator,
                (outputArray, outputSchema) ->
                        NativeValuesBridge.executeArrow(serializedPlan, outputArray, outputSchema));
    }

    public static ArrowRowDataBatch execute(
            NativeExecutionContext context, RowType outputType, BufferAllocator allocator) {
        return execute(
                outputType,
                allocator,
                (outputArray, outputSchema) -> NativeValuesBridge.executeArrow(context, outputArray, outputSchema));
    }

    private static ArrowRowDataBatch execute(
            RowType outputType, BufferAllocator allocator, NativeValuesInvocation invocation) {
        try (ArrowArray outputArray = ArrowArray.allocateNew(allocator);
                ArrowSchema outputSchema = ArrowSchema.allocateNew(allocator);
                CDataDictionaryProvider dictionaries = new CDataDictionaryProvider()) {
            long rowCount = invocation.execute(outputArray.memoryAddress(), outputSchema.memoryAddress());
            if (rowCount < 0 || rowCount > Integer.MAX_VALUE) {
                throw new IllegalStateException("Native VALUES returned invalid row count " + rowCount);
            }
            VectorSchemaRoot output = Data.importVectorSchemaRoot(allocator, outputArray, outputSchema, dictionaries);
            output.setRowCount((int) rowCount);
            return ArrowRowDataBatch.wrap(output, outputType);
        }
    }

    @FunctionalInterface
    private interface NativeValuesInvocation {
        long execute(long outputArray, long outputSchema);
    }
}
