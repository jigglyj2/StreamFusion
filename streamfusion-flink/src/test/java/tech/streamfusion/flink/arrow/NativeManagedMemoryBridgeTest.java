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
            assertThat(memory.reserved()).isEqualTo(plan.length);
        }

        assertThat(memory.reserved()).isZero();
    }

    @Test
    void rejectsNativeScratchThatExceedsTheHostBudget() {
        byte[] plan = chainedSelectionPlan();
        TrackingMemoryManager memory = new TrackingMemoryManager(plan.length + 4L);
        RowType rowType = RowType.of(new IntType(false));
        List<RowData> rows = List.of(GenericRowData.of(1), GenericRowData.of(2), GenericRowData.of(3));

        try (RootAllocator allocator = new RootAllocator(64L << 20);
                NativeExecutionContext context = new NativeExecutionContext(plan, memory);
                ArrowRowDataBatch input = ArrowRowDataBatch.transpose(rows, rowType, allocator)) {
            assertThatThrownBy(() -> ArrowCDataBridge.executeWithSelection(context, input, rowType, allocator))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Resources exhausted")
                    .hasMessageContaining("requested 12 bytes")
                    .hasMessageContaining("native input-row ordinal");
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
        private final long limit;
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
        public long limit() {
            return limit;
        }

        private synchronized long reserved() {
            return reserved;
        }

        private synchronized long peak() {
            return peak;
        }
    }
}
