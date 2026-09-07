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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;
import tech.streamfusion.nativebridge.NativeExecutionContext;
import tech.streamfusion.nativebridge.NativeMemoryManager;

class NativeManagedMemoryBridgeTest extends ArrowCDataBridgeTestSupport {
    @Test
    void liveInvocationRejectsBothEdgesAndCancellationAllowsCleanStatelessReuse() {
        for (boolean drain : List.of(false, true)) {
            TrackingMemoryManager memory = new TrackingMemoryManager(64L << 20);
            RowType rowType = RowType.of(new IntType(false));
            try (RootAllocator allocator = new RootAllocator(64L << 20);
                    NativeExecutionContext context = new NativeExecutionContext(chainedSelectionPlan(), memory);
                    ArrowRowDataBatch input = ArrowRowDataBatch.transpose(
                            List.of(GenericRowData.of(1), GenericRowData.of(2), GenericRowData.of(3)),
                            rowType,
                            allocator)) {
                var execution = new ArrowCDataBridge.ReusableExecution(context, rowType, allocator);
                try (var active = execution.executeStream(input)) {
                    assertThatThrownBy(() -> execution.executeStream(input))
                            .isInstanceOf(IllegalStateException.class)
                            .hasMessageContaining("active or failed invocation");
                    assertThatThrownBy(() -> ArrowCDataBridge.executeWithSelection(context, input, rowType, allocator))
                            .isInstanceOf(IllegalStateException.class)
                            .hasMessageContaining("active or failed invocation");
                    if (drain) {
                        try (var output = active.nextWithSelection()) {
                            assertThat(output.batch().rowView(0).getInt(0)).isEqualTo(12);
                            assertThat(output.batch().rowView(1).getInt(0)).isEqualTo(13);
                        }
                        assertThat(active.nextWithSelection()).isNull();
                    }
                }
                try (var next = execution.executeStream(input)) {
                    try (var output = next.nextWithSelection()) {
                        assertThat(output.batch().rowView(0).getInt(0)).isEqualTo(12);
                        assertThat(output.batch().rowView(1).getInt(0)).isEqualTo(13);
                    }
                    assertThat(next.nextWithSelection()).isNull();
                }
            }
            assertThat(memory.reserved()).isZero();
        }
    }

    @Test
    void importedOutputKeepsNativeBuffersAdmittedAfterStreamAndContextClose() {
        TrackingMemoryManager memory = new TrackingMemoryManager(64L << 20);
        RowType rowType = RowType.of(new IntType(false));
        try (RootAllocator allocator = new RootAllocator(64L << 20);
                NativeExecutionContext context = new NativeExecutionContext(chainedSelectionPlan(), memory);
                ArrowRowDataBatch input = ArrowRowDataBatch.transpose(
                        List.of(GenericRowData.of(1), GenericRowData.of(2), GenericRowData.of(3)),
                        rowType,
                        allocator)) {
            var execution = new ArrowCDataBridge.ReusableExecution(context, rowType, allocator);
            var stream = execution.executeStream(input);
            try (NativeCalcResult result = stream.nextWithSelection()) {
                assertThat(stream.nextWithSelection()).isNull();
                stream.close();
                context.close();
                assertThat(memory.reserved()).isPositive();
                assertThat(result.batch().rowView(0).getInt(0)).isEqualTo(12);
                assertThat(result.batch().rowView(1).getInt(0)).isEqualTo(13);
            } finally {
                stream.close();
            }
            assertThat(memory.reserved()).isZero();
        }
    }

    @Test
    void liveStreamKeepsPlanSchemaAndRuntimeAdmittedAfterContextHandleCloses() {
        for (boolean drain : List.of(false, true)) {
            TrackingMemoryManager memory = new TrackingMemoryManager(64L << 20);
            RowType rowType = RowType.of(new IntType(false));
            try (RootAllocator allocator = new RootAllocator(64L << 20);
                    NativeExecutionContext context = new NativeExecutionContext(chainedSelectionPlan(), memory);
                    ArrowRowDataBatch input = ArrowRowDataBatch.transpose(
                            List.of(GenericRowData.of(1), GenericRowData.of(2), GenericRowData.of(3)),
                            rowType,
                            allocator)) {
                var execution = new ArrowCDataBridge.ReusableExecution(context, rowType, allocator);
                try (var stream = execution.executeStream(input)) {
                    long admitted = memory.reserved();
                    assertThat(admitted).isGreaterThan(256L << 10);
                    context.close();
                    assertThat(memory.reserved()).isEqualTo(admitted);
                    if (drain) {
                        try (NativeCalcResult output = stream.nextWithSelection()) {
                            assertThat(output.batch().size()).isEqualTo(2);
                            assertThat(output.batch().rowView(0).getInt(0)).isEqualTo(12);
                        }
                        assertThat(stream.nextWithSelection()).isNull();
                    }
                    // A cancelled, never-polled stream has the same last-owner cleanup contract.
                }
                assertThat(memory.reserved()).isZero();
            }
            assertThat(memory.reserved()).isZero();
        }
    }

    @Test
    void accountsNativePlanScratchAndOutputThroughHostCallbacks() {
        byte[] plan = chainedSelectionPlan();
        TrackingMemoryManager memory = new TrackingMemoryManager(64L << 20);
        RowType rowType = RowType.of(new IntType(false));
        List<RowData> rows = List.of(GenericRowData.of(1), GenericRowData.of(2), GenericRowData.of(3));

        try (RootAllocator allocator = new RootAllocator(64L << 20);
                NativeExecutionContext context = new NativeExecutionContext(plan, memory);
                ArrowRowDataBatch input = ArrowRowDataBatch.transpose(rows, rowType, allocator);
                NativeCalcResult result = ArrowCDataBridge.executeWithSelection(context, input, rowType, allocator)) {
            assertThat(result.batch().size()).isEqualTo(2);
            assertThat(memory.peak()).isGreaterThan(plan.length);
            assertThat(memory.reserved()).isGreaterThan(plan.length);
        }

        assertThat(memory.reserved()).isZero();
    }

    @Test
    void rejectsNativeScratchThatExceedsTheHostBudget() {
        for (String edge : List.of("batch", "cached-batch", "stream")) {
            assertInputAdmissionRecovery(edge);
        }
    }

    private static void assertInputAdmissionRecovery(String edge) {
        byte[] plan = chainedSelectionPlan();
        TrackingMemoryManager memory = new TrackingMemoryManager(64L << 20);
        RowType rowType = RowType.of(new IntType(false));
        List<RowData> rows = List.of(GenericRowData.of(1), GenericRowData.of(2), GenericRowData.of(3));

        try (RootAllocator allocator = new RootAllocator(64L << 20);
                NativeExecutionContext context = new NativeExecutionContext(plan, memory);
                ArrowRowDataBatch input = ArrowRowDataBatch.transpose(rows, rowType, allocator)) {
            var execution = new ArrowCDataBridge.ReusableExecution(context, rowType, allocator);
            java.util.function.Supplier<NativeCalcResult> invoke = () -> {
                if (edge.equals("batch")) {
                    return ArrowCDataBridge.executeWithSelection(context, input, rowType, allocator);
                }
                if (edge.equals("cached-batch")) return execution.executeWithSelection(input);
                try (var stream = execution.executeStream(input)) {
                    NativeCalcResult output = stream.nextWithSelection();
                    assertThat(stream.nextWithSelection()).isNull();
                    return output;
                }
            };
            try (NativeCalcResult ignored = invoke.get()) {
                // Warm the task-lifetime schema and physical-plan caches before constraining
                // the remaining per-batch scratch allowance.
            }
            long retained = memory.reserved();
            long peak = memory.peak();
            long javaRetained = allocator.getAllocatedMemory();
            // Deny the large-owner ordinal workspace consistently at every edge.
            memory.setLimit(retained + 4L);
            for (int attempt = 0; attempt < 2; attempt++) {
                assertThatThrownBy(invoke::get)
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining("Resources exhausted")
                        .hasMessageContaining("Flink denied")
                        .hasMessageContaining("native input row ordinal");
                assertThat(memory.reserved()).isEqualTo(retained);
                assertThat(memory.peak()).isEqualTo(peak);
                assertThat(allocator.getAllocatedMemory()).isEqualTo(javaRetained);
                assertThat(input.rowView(0).getInt(0)).isEqualTo(1);
            }
            memory.setLimit(64L << 20);
            try (NativeCalcResult result = invoke.get()) {
                assertThat(result.batch().size()).isEqualTo(2);
                assertThat(result.batch().rowView(0).getInt(0)).isEqualTo(12);
            }
        }

        assertThat(memory.reserved()).isZero();
    }

    @Test
    void reusesOneLoweredPhysicalPlanAcrossArrowBatches() {
        byte[] plan = chainedSelectionPlan();
        TrackingMemoryManager memory = new TrackingMemoryManager(64L << 20);
        RowType rowType = RowType.of(new IntType(false));

        try (RootAllocator allocator = new RootAllocator(64L << 20);
                NativeExecutionContext context = new NativeExecutionContext(plan, memory)) {
            for (int offset : new int[] {0, 10}) {
                List<RowData> rows = List.of(
                        GenericRowData.of(offset + 1), GenericRowData.of(offset + 2), GenericRowData.of(offset + 3));
                try (ArrowRowDataBatch input = ArrowRowDataBatch.transpose(rows, rowType, allocator);
                        NativeCalcResult result =
                                ArrowCDataBridge.executeWithSelection(context, input, rowType, allocator)) {
                    if (offset == 0) {
                        assertThat(result.batch().size()).isEqualTo(2);
                        assertThat(result.batch().rowView(0).getInt(0)).isEqualTo(12);
                        assertThat(result.batch().rowView(1).getInt(0)).isEqualTo(13);
                    } else {
                        assertThat(result.batch().size()).isEqualTo(3);
                        assertThat(result.batch().rowView(0).getInt(0)).isEqualTo(21);
                        assertThat(result.batch().rowView(1).getInt(0)).isEqualTo(22);
                        assertThat(result.batch().rowView(2).getInt(0)).isEqualTo(23);
                    }
                }
            }
        }

        assertThat(memory.reserved()).isZero();
    }

    private static final class TrackingMemoryManager implements NativeMemoryManager {
        private long limit;
        private long reserved;
        private long peak;

        private TrackingMemoryManager(long limit) {
            this.limit = limit;
        }

        @Override
        public synchronized boolean tryReserve(long bytes) {
            if (bytes < 0 || bytes > limit - reserved) {
                return false;
            }
            reserved += bytes;
            peak = Math.max(peak, reserved);
            return true;
        }

        @Override
        public synchronized void release(long bytes) {
            if (bytes < 0 || bytes > reserved) {
                throw new IllegalStateException("Invalid native-memory release: " + bytes);
            }
            reserved -= bytes;
        }

        @Override
        public synchronized long limit() {
            return limit;
        }

        private synchronized void setLimit(long limit) {
            if (limit < reserved) {
                throw new IllegalArgumentException("Limit cannot be smaller than the active reservation");
            }
            this.limit = limit;
        }

        private synchronized long reserved() {
            return reserved;
        }

        private synchronized long peak() {
            return peak;
        }
    }
}
