/*
 * Copyright 2026 StreamFusion Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tech.streamfusion.nativebridge;

import java.util.concurrent.atomic.AtomicLong;

/** JNI boundary for native multi-input UNION ALL execution. */
public final class NativeUnionBridge {
    private static final AtomicLong EXECUTED_BATCHES = new AtomicLong();

    static {
        NativeLibraryLoader.load();
    }

    private NativeUnionBridge() {}

    public static long executeArrow(
            byte[] serializedPlan,
            long[] inputArrayAddresses,
            long[] inputSchemaAddresses,
            long outputArrayAddress,
            long outputSchemaAddress) {
        try (NativeExecutionContext context =
                new NativeExecutionContext(serializedPlan, NativeMemoryManager.unbounded())) {
            return executeArrow(
                    context, inputArrayAddresses, inputSchemaAddresses, outputArrayAddress, outputSchemaAddress);
        }
    }

    public static long executeArrow(
            NativeExecutionContext context,
            long[] inputArrayAddresses,
            long[] inputSchemaAddresses,
            long outputArrayAddress,
            long outputSchemaAddress) {
        long rows = executeArrowBatches(
                context.handle(), inputArrayAddresses, inputSchemaAddresses, outputArrayAddress, outputSchemaAddress);
        EXECUTED_BATCHES.incrementAndGet();
        return rows;
    }

    public static long executedBatchCount() {
        return EXECUTED_BATCHES.get();
    }

    public static void resetMetrics() {
        EXECUTED_BATCHES.set(0);
    }

    private static native long executeArrowBatches(
            long executionContext,
            long[] inputArrayAddresses,
            long[] inputSchemaAddresses,
            long outputArrayAddress,
            long outputSchemaAddress);
}
