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
import tech.streamfusion.nativebridge.NativeMemoryManager;

/** Ownership-safe Arrow C Data input for native exchange routing and IPC framing. */
public final class ArrowExchangeCDataBridge {
    private ArrowExchangeCDataBridge() {}

    public static List<NativeExchangeFrame> route(
            byte[] serializedPlan,
            ArrowRowDataBatch input,
            BufferAllocator allocator,
            NativeMemoryManager memoryManager) {
        return route(
                input,
                (array, schema) -> NativeExchangeBridge.routeArrowBatch(serializedPlan, array, schema, memoryManager));
    }

    public static List<NativeExchangeFrame> route(
            tech.streamfusion.nativebridge.NativeExchangeRouter router, ArrowRowDataBatch input) {
        return route(input, router::route);
    }

    @FunctionalInterface
    private interface NativeCall {
        byte[] route(long array, long schema);
    }

    private static List<NativeExchangeFrame> route(ArrowRowDataBatch input, NativeCall call) {
        BufferAllocator inputAllocator = input.allocator();
        try (ArrowArray inputArray = ArrowArray.allocateNew(inputAllocator);
                ArrowSchema inputSchema = ArrowSchema.allocateNew(inputAllocator)) {
            try {
                Data.exportVectorSchemaRoot(inputAllocator, input.root(), null, inputArray, inputSchema);
                byte[] encoded = call.route(inputArray.memoryAddress(), inputSchema.memoryAddress());
                return NativeExchangeFrames.decode(encoded);
            } finally {
                ArrowCDataBridge.releaseInputExports(inputArray, inputSchema);
            }
        }
    }
}
