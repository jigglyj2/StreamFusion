/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.arrow;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.exchange.ArrowExchangeInputBatch;
import tech.streamfusion.flink.exchange.NativeExchangeFrame;
import tech.streamfusion.nativebridge.NativeExchangeBridge;
import tech.streamfusion.nativebridge.NativeMemoryManager;

/** Imports one schema-free native exchange frame through Arrow C Data. */
public final class ArrowExchangeInputCDataBridge {
    private ArrowExchangeInputCDataBridge() {}

    public static ArrowExchangeInputBatch decode(
            byte[] serializedPlan, NativeExchangeFrame frame, RowType rowType, BufferAllocator allocator) {
        return decode(serializedPlan, frame, rowType, allocator, NativeMemoryManager.unbounded());
    }

    public static ArrowExchangeInputBatch decode(
            byte[] serializedPlan,
            NativeExchangeFrame frame,
            RowType rowType,
            BufferAllocator allocator,
            NativeMemoryManager memoryManager) {
        try (ArrowArray outputArray = ArrowArray.allocateNew(allocator);
                ArrowSchema outputSchema = ArrowSchema.allocateNew(allocator);
                CDataDictionaryProvider dictionaries = new CDataDictionaryProvider()) {
            long rows = NativeExchangeBridge.decodeArrowBatch(
                    serializedPlan,
                    frame.ipcPayload(),
                    frame.metadataLength(),
                    outputArray.memoryAddress(),
                    outputSchema.memoryAddress(),
                    memoryManager);
            if (rows < 0 || rows > Integer.MAX_VALUE) {
                throw new IllegalStateException("Native exchange returned invalid row count " + rows);
            }
            VectorSchemaRoot root = Data.importVectorSchemaRoot(allocator, outputArray, outputSchema, dictionaries);
            root.setRowCount((int) rows);
            return new ArrowExchangeInputBatch(root, rowType);
        }
    }
}
