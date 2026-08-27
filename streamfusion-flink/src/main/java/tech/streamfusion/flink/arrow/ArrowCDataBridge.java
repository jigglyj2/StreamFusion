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
import tech.streamfusion.nativebridge.NativeCalcBridge;

/** Ownership-safe Arrow C Data transfer for one native execution batch. */
public final class ArrowCDataBridge {
    private ArrowCDataBridge() {}

    public static ArrowRowDataBatch execute(
            byte[] serializedPlan, ArrowRowDataBatch input, RowType outputType, BufferAllocator allocator) {
        try (ArrowArray inputArray = ArrowArray.allocateNew(allocator);
                ArrowSchema inputSchema = ArrowSchema.allocateNew(allocator);
                ArrowArray outputArray = ArrowArray.allocateNew(allocator);
                ArrowSchema outputSchema = ArrowSchema.allocateNew(allocator);
                CDataDictionaryProvider dictionaries = new CDataDictionaryProvider()) {
            Data.exportVectorSchemaRoot(allocator, input.root(), null, inputArray, inputSchema);
            long rowCount = NativeCalcBridge.executeArrow(
                    serializedPlan,
                    inputArray.memoryAddress(),
                    inputSchema.memoryAddress(),
                    outputArray.memoryAddress(),
                    outputSchema.memoryAddress());
            if (rowCount < 0 || rowCount > Integer.MAX_VALUE) {
                throw new IllegalStateException("Native calc returned invalid row count " + rowCount);
            }
            VectorSchemaRoot output = Data.importVectorSchemaRoot(allocator, outputArray, outputSchema, dictionaries);
            output.setRowCount((int) rowCount);
            return ArrowRowDataBatch.wrap(output, outputType);
        }
    }
}
