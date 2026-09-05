/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.arrow;

import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.nativebridge.NativeMemoryManager;
import tech.streamfusion.nativebridge.NativeWindowDeduplicateBridge;

/** Arrow C Data transport for native window deduplication and timer output. */
public final class ArrowWindowDeduplicateCDataBridge {
    private ArrowWindowDeduplicateCDataBridge() {}

    public static ArrowRowDataBatch process(
            long handle,
            ArrowRowDataBatch input,
            List<byte[]> preencodedKeys,
            RowType outputType,
            BufferAllocator allocator,
            NativeMemoryManager memoryManager) {
        return ArrowKeyedTimerCDataBridge.process(
                handle,
                input,
                preencodedKeys,
                outputType,
                allocator,
                memoryManager,
                NativeWindowDeduplicateBridge::process,
                "window deduplicate");
    }

    public static ArrowRowDataBatch advance(
            long handle,
            long watermark,
            RowType outputType,
            BufferAllocator allocator,
            NativeMemoryManager memoryManager) {
        return ArrowKeyedTimerCDataBridge.advance(
                handle,
                watermark,
                outputType,
                allocator,
                memoryManager,
                NativeWindowDeduplicateBridge::advance,
                "window deduplicate");
    }
}
