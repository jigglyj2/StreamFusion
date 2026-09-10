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
    NativeExchangeBatchRouter JNI = new Native();

    default Session open(byte[] plan, NativeMemoryManager memory) {
        return (batch, allocator) -> route(plan, batch, allocator, memory);
    }

    interface Session extends AutoCloseable {
        List<NativeExchangeFrame> route(ArrowRowDataBatch batch, BufferAllocator allocator);

        @Override
        default void close() {}
    }

    final class Native implements NativeExchangeBatchRouter {
        private static final long serialVersionUID = 1L;

        @Override
        public List<NativeExchangeFrame> route(
                byte[] plan, ArrowRowDataBatch batch, BufferAllocator allocator, NativeMemoryManager memory) {
            return ArrowExchangeCDataBridge.route(plan, batch, allocator, memory);
        }

        @Override
        public Session open(byte[] plan, NativeMemoryManager memory) {
            var router = new tech.streamfusion.nativebridge.NativeExchangeRouter(plan, memory);
            return new Session() {
                @Override
                public List<NativeExchangeFrame> route(ArrowRowDataBatch batch, BufferAllocator allocator) {
                    return ArrowExchangeCDataBridge.route(router, batch);
                }

                @Override
                public void close() {
                    router.close();
                }
            };
        }
    }

    List<NativeExchangeFrame> route(
            byte[] serializedPlan,
            ArrowRowDataBatch batch,
            BufferAllocator allocator,
            NativeMemoryManager memoryManager);
}
