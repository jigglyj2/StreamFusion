/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.exchange;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.arrow.ArrowExchangeInputCDataBridge;

/** Injectable receiving boundary for native exchange runtime tests. */
@FunctionalInterface
interface NativeExchangeFrameDecoder {
    NativeExchangeFrameDecoder JNI =
            (plan, frame, rowType, allocator) -> ArrowExchangeInputCDataBridge.decode(plan, frame, rowType, allocator);

    ArrowExchangeInputBatch decode(
            byte[] serializedPlan, NativeExchangeFrame frame, RowType rowType, BufferAllocator allocator);
}
