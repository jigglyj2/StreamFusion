/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.exchange;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.TestingNativeMemoryManager;
import tech.streamfusion.flink.arrow.ArrowExchangeCDataBridge;
import tech.streamfusion.flink.arrow.ArrowExchangeInputCDataBridge;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.nativebridge.NativeMemoryManager;

class NativeExchangeCDataRoundTripTest {
    @Test
    void preservesRowsChangelogAndTimestampsAcrossNativeFrames() {
        RowType rowType = RowType.of(new IntType(false));
        byte[] plan = NativeExchangePlanSerializer.hash(rowType, new int[] {0}, 128);
        GenericRowData insert = GenericRowData.of(1);
        insert.setRowKind(RowKind.INSERT);
        GenericRowData delete = GenericRowData.of(42);
        delete.setRowKind(RowKind.DELETE);
        List<ResultRow> output = new ArrayList<>();
        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                ArrowRowDataBatch input = ArrowRowDataBatch.transpose(List.of(insert, delete), rowType, allocator)
                        .withEnvelope(
                                new RowKind[] {RowKind.INSERT, RowKind.DELETE},
                                new boolean[] {true, false},
                                new long[] {100, 0});
                ArrowExchangeBatch.EnvelopeBatch envelope = ArrowExchangeBatch.withEnvelope(input, rowType)) {
            NativeMemoryManager memoryManager = TestingNativeMemoryManager.create();
            List<NativeExchangeFrame> frames =
                    ArrowExchangeCDataBridge.route(plan, envelope.batch(), allocator, memoryManager);
            for (NativeExchangeFrame frame : frames) {
                try (ArrowExchangeInputBatch decoded =
                        ArrowExchangeInputCDataBridge.decode(plan, frame, rowType, allocator, memoryManager)) {
                    for (int row = 0; row < decoded.size(); row++) {
                        output.add(new ResultRow(
                                decoded.rowView(row).getInt(0),
                                decoded.rowView(row).getRowKind(),
                                decoded.hasTimestamp(row),
                                decoded.timestamp(row)));
                    }
                }
            }
        }
        output.sort(Comparator.comparingInt(result -> result.value));

        assertThat(output)
                .usingRecursiveFieldByFieldElementComparator()
                .containsExactly(
                        new ResultRow(1, RowKind.INSERT, true, 100L),
                        new ResultRow(42, RowKind.DELETE, false, Long.MIN_VALUE));
    }

    @Test
    void hidesANativelyGeneratedComplexRoutingKeyFromAnOrdinaryReader() {
        RowType rowType = RowType.of(new ArrayType(false, new IntType(false)));
        byte[] plan = NativeExchangePlanSerializer.hash(rowType, new int[] {0}, 128, 4, false, true);
        GenericRowData insert = GenericRowData.of(new GenericArrayData(new int[] {7, 9}));
        insert.setRowKind(RowKind.INSERT);
        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                ArrowRowDataBatch input = ArrowRowDataBatch.transpose(List.of(insert), rowType, allocator);
                ArrowExchangeBatch.EnvelopeBatch envelope = ArrowExchangeBatch.withEnvelope(input, rowType)) {
            NativeMemoryManager memoryManager = TestingNativeMemoryManager.create();
            List<NativeExchangeFrame> frames =
                    ArrowExchangeCDataBridge.route(plan, envelope.batch(), allocator, memoryManager);

            assertThat(frames).hasSize(1);
            try (ArrowExchangeInputBatch decoded =
                    ArrowExchangeInputCDataBridge.decode(plan, frames.get(0), rowType, allocator, memoryManager)) {
                assertThat(decoded.transportRoot().getFieldVectors()).hasSize(4);
                assertThat(decoded.arrowBatch().root().getFieldVectors()).hasSize(1);
                assertThat(decoded.rowView(0).getArray(0).size()).isEqualTo(2);
                assertThat(decoded.rowView(0).getArray(0).getInt(0)).isEqualTo(7);
                assertThat(decoded.rowView(0).getArray(0).getInt(1)).isEqualTo(9);
            }
        }
    }

    @Test
    void cachedRouterReusesItsPlanPreservesFrameBytesAndReleasesOwnership() {
        RowType type = RowType.of(new IntType(false));
        for (boolean unaligned : new boolean[] {false, true}) {
            byte[] plan = NativeExchangePlanSerializer.hash(type, new int[] {0}, 128, 4, unaligned, false);
            NativeMemoryManager memory = TestingNativeMemoryManager.create();
            try (RootAllocator allocator = new RootAllocator();
                    ArrowRowDataBatch input = ArrowRowDataBatch.transpose(
                            List.of(GenericRowData.of(1), GenericRowData.of(42), GenericRowData.of(1)),
                            type,
                            allocator);
                    ArrowExchangeBatch.EnvelopeBatch envelope = ArrowExchangeBatch.withEnvelope(input, type)) {
                var expected = ArrowExchangeCDataBridge.route(plan, envelope.batch(), allocator, memory);
                var router = new tech.streamfusion.nativebridge.NativeExchangeRouter(plan, memory);
                try {
                    long available = memory.available();
                    assertThat(available).isLessThan(memory.limit());
                    for (int batch = 0; batch < 3; batch++) {
                        var actual = ArrowExchangeCDataBridge.route(router, envelope.batch());
                        assertThat(actual).hasSize(expected.size());
                        for (int frame = 0; frame < actual.size(); frame++) {
                            assertThat(actual.get(frame).keyGroup())
                                    .isEqualTo(expected.get(frame).keyGroup());
                            assertThat(actual.get(frame).metadata())
                                    .isEqualTo(expected.get(frame).metadata());
                            assertThat(actual.get(frame).body())
                                    .isEqualTo(expected.get(frame).body());
                        }
                        assertThat(memory.available()).isEqualTo(available);
                    }
                } finally {
                    router.close();
                }
                router.close();
                assertThat(memory.available()).isEqualTo(memory.limit());
                long arrowBytes = allocator.getAllocatedMemory();
                org.assertj.core.api.Assertions.assertThatThrownBy(
                                () -> ArrowExchangeCDataBridge.route(router, envelope.batch()))
                        .hasMessageContaining("closed");
                assertThat(allocator.getAllocatedMemory()).isEqualTo(arrowBytes);
                org.assertj.core.api.Assertions.assertThatThrownBy(() ->
                                ArrowExchangeCDataBridge.route(new byte[] {1}, envelope.batch(), allocator, memory))
                        .isInstanceOf(IllegalStateException.class);
                assertThat(allocator.getAllocatedMemory()).isEqualTo(arrowBytes);
                org.assertj.core.api.Assertions.assertThatThrownBy(
                                () -> new tech.streamfusion.nativebridge.NativeExchangeRouter(new byte[] {1}, memory))
                        .isInstanceOf(IllegalStateException.class);
                assertThat(memory.available()).isEqualTo(memory.limit());
            }
        }
    }

    @Test
    void deniedWorkspaceReleasesExportsAndLeavesTheCachedRouterUsable() {
        RowType type = RowType.of(new IntType(false));
        byte[] plan = NativeExchangePlanSerializer.hash(type, new int[] {0}, 32768, 4, true, false);
        NativeMemoryManager memory = TestingNativeMemoryManager.create(2L << 20);
        try (RootAllocator allocator = new RootAllocator();
                ArrowRowDataBatch input = ArrowRowDataBatch.transpose(List.of(GenericRowData.of(42)), type, allocator);
                ArrowExchangeBatch.EnvelopeBatch envelope = ArrowExchangeBatch.withEnvelope(input, type);
                var router = new tech.streamfusion.nativebridge.NativeExchangeRouter(plan, memory)) {
            long arrowBytes = allocator.getAllocatedMemory();
            long available = memory.available();
            long competingReservation = available - (64L << 10);
            assertThat(memory.tryReserve(competingReservation)).isTrue();
            org.assertj.core.api.Assertions.assertThatThrownBy(
                            () -> ArrowExchangeCDataBridge.route(router, envelope.batch()))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Flink denied");
            assertThat(memory.available()).isEqualTo(64L << 10);
            assertThat(allocator.getAllocatedMemory()).isEqualTo(arrowBytes);
            memory.release(competingReservation);
            assertThat(ArrowExchangeCDataBridge.route(router, envelope.batch())).hasSize(1);
            assertThat(memory.available()).isEqualTo(available);
            assertThat(allocator.getAllocatedMemory()).isEqualTo(arrowBytes);
        }
        assertThat(memory.available()).isEqualTo(memory.limit());
    }

    private static final class ResultRow {
        private final int value;
        private final RowKind kind;
        private final boolean hasTimestamp;
        private final long timestamp;

        private ResultRow(int value, RowKind kind, boolean hasTimestamp, long timestamp) {
            this.value = value;
            this.kind = kind;
            this.hasTimestamp = hasTimestamp;
            this.timestamp = timestamp;
        }
    }
}
