/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.arrow;

import java.util.List;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import tech.streamfusion.flink.exchange.NativeExchangeFrame;
import tech.streamfusion.flink.exchange.NativeExchangeFrames;
import tech.streamfusion.nativebridge.NativeExchangeBridge;

/** Ownership-safe Arrow C Data input for native exchange routing and IPC framing. */
public final class ArrowExchangeCDataBridge {
    private ArrowExchangeCDataBridge() {}

    public static List<NativeExchangeFrame> route(
            byte[] serializedPlan, int parallelism, ArrowRowDataBatch input, BufferAllocator allocator) {
        try (ArrowArray inputArray = ArrowArray.allocateNew(allocator);
                ArrowSchema inputSchema = ArrowSchema.allocateNew(allocator)) {
            Data.exportVectorSchemaRoot(allocator, input.root(), null, inputArray, inputSchema);
            byte[] encoded = NativeExchangeBridge.routeArrowBatch(
                    serializedPlan, parallelism, inputArray.memoryAddress(), inputSchema.memoryAddress());
            return NativeExchangeFrames.decode(encoded);
        }
    }
}
