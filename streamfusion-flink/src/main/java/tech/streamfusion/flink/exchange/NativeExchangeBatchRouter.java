/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.exchange;

import java.io.Serializable;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import tech.streamfusion.flink.arrow.ArrowExchangeCDataBridge;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.nativebridge.NativeMemoryManager;

/** Injectable native routing boundary used by the runtime operator and its ordering tests. */
@FunctionalInterface
interface NativeExchangeBatchRouter extends Serializable {
    NativeExchangeBatchRouter JNI = (plan, batch, allocator, memoryManager) ->
            ArrowExchangeCDataBridge.route(plan, batch, allocator, memoryManager);

    List<NativeExchangeFrame> route(
            byte[] serializedPlan,
            ArrowRowDataBatch batch,
            BufferAllocator allocator,
            NativeMemoryManager memoryManager);
}
