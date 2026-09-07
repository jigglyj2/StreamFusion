/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.join;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import tech.streamfusion.flink.TestingNativeMemoryManager;
import tech.streamfusion.flink.arrow.ArrowRegularJoinOutputStream;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.exchange.ArrowExchangeInputBatch;
import tech.streamfusion.nativebridge.NativeMemoryManager;
import tech.streamfusion.nativebridge.NativeRegularJoinBridge;

class StreamFusionRegularJoinStreamTest {
    private static final RowType INPUT = RowType.of(new BigIntType(false), new VarCharType());
    private static final RowType OUTPUT =
            RowType.of(new BigIntType(false), new VarCharType(), new BigIntType(false), new VarCharType());
    private static final byte[] PLAN = StreamFusionRegularJoinPlan.create(
            INPUT, INPUT, new int[] {0}, new int[] {0}, new boolean[] {true}, FlinkJoinType.INNER, null);

    @Test
    void drainsHotKeyInsertAndRetractionThroughBoundedCStreamOnBothBackends(@TempDir Path temporary) {
        for (boolean rocks : List.of(false, true)) {
            NativeMemoryManager memory = TestingNativeMemoryManager.create();
            long handle = rocks
                    ? NativeRegularJoinBridge.createRocksDb(
                            PLAN, 128, 0, 127, temporary.resolve("rocks"), 32L << 20, memory)
                    : NativeRegularJoinBridge.create(PLAN, 128, 0, 127, memory);
            try (RootAllocator allocator = new RootAllocator(128L << 20)) {
                seed(handle, allocator, memory);
                for (RowKind kind : List.of(RowKind.INSERT, RowKind.DELETE)) {
                    try (ArrowExchangeInputBatch input = input(allocator, 1, "right", kind);
                            ArrowRegularJoinOutputStream stream = new ArrowRegularJoinOutputStream(
                                    handle, 1, input, null, OUTPUT, allocator, memory)) {
                        assertThatThrownBy(() -> NativeRegularJoinBridge.snapshot(handle, 0))
                                .hasMessageContaining("undrained");
                        int rows = 0;
                        int batches = 0;
                        while (true) {
                            try (ArrowRowDataBatch output = stream.next()) {
                                if (output == null) {
                                    break;
                                }
                                assertThat(output.size()).isBetween(1, 4096);
                                for (int index = 0; index < output.size(); index++) {
                                    assertThat(output.rowKind(index)).isEqualTo(kind);
                                    assertThat(output.rowView(index)
                                                    .getString(1)
                                                    .toString())
                                            .isEqualTo("left" + rows++);
                                    assertThat(output.rowView(index)
                                                    .getString(3)
                                                    .toString())
                                            .isEqualTo("right0");
                                }
                                batches++;
                            }
                        }
                        assertThat(rows).isEqualTo(5003);
                        assertThat(batches).isGreaterThan(1);
                    }
                }
                assertThat(NativeRegularJoinBridge.statistics(handle)).containsExactly(5L, 3L, 0L);
                NativeRegularJoinBridge.snapshot(handle, 0);
            } finally {
                NativeRegularJoinBridge.destroy(handle);
            }
            assertThat(memory.available()).isEqualTo(memory.limit());
        }
    }

    @Test
    void streamOwnsNativeProcessorAfterHandleReleaseAndCancelsSafely() {
        NativeMemoryManager memory = TestingNativeMemoryManager.create();
        long handle = NativeRegularJoinBridge.create(PLAN, 128, 0, 127, memory);
        boolean released = false;
        try (RootAllocator allocator = new RootAllocator(128L << 20)) {
            seed(handle, allocator, memory);
            try (ArrowExchangeInputBatch input = input(allocator, 1, "right", RowKind.INSERT);
                    ArrowRegularJoinOutputStream stream =
                            new ArrowRegularJoinOutputStream(handle, 1, input, null, OUTPUT, allocator, memory)) {
                NativeRegularJoinBridge.destroy(handle);
                released = true;
                try (ArrowRowDataBatch output = stream.next()) {
                    assertThat(output.size()).isBetween(1, 4096);
                }
                // Closing before exhaustion releases the retained input/state and the last Arc.
            }
        } finally {
            if (!released) {
                NativeRegularJoinBridge.destroy(handle);
            }
        }
        assertThat(memory.available()).isEqualTo(memory.limit());
    }

    private static void seed(long handle, BufferAllocator allocator, NativeMemoryManager memory) {
        try (ArrowExchangeInputBatch input = input(allocator, 5003, "left", RowKind.INSERT);
                ArrowRegularJoinOutputStream stream =
                        new ArrowRegularJoinOutputStream(handle, 0, input, null, OUTPUT, allocator, memory)) {
            assertThat(stream.next()).isNull();
        }
    }

    private static ArrowExchangeInputBatch input(BufferAllocator allocator, int count, String prefix, RowKind kind) {
        BigIntVector keys = new BigIntVector("key", allocator);
        VarCharVector values = new VarCharVector("payload", allocator);
        TinyIntVector kinds = new TinyIntVector("__streamfusion_row_kind", allocator);
        BigIntVector timestamps = new BigIntVector("__streamfusion_timestamp", allocator);
        VectorSchemaRoot root = new VectorSchemaRoot(List.of(keys, values, kinds, timestamps));
        root.allocateNew();
        timestamps.allocateNew(count);
        for (int index = 0; index < count; index++) {
            keys.setSafe(index, 1L);
            values.setSafe(index, (prefix + index).getBytes(StandardCharsets.UTF_8));
            kinds.setSafe(index, kind.toByteValue());
            timestamps.setNull(index);
        }
        root.setRowCount(count);
        return new ArrowExchangeInputBatch(root, INPUT);
    }
}
