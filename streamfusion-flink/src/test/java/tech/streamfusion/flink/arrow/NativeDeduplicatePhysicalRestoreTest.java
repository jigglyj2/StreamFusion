/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.arrow;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import tech.streamfusion.flink.TestingNativeMemoryManager;
import tech.streamfusion.nativebridge.NativeDeduplicateBridge;
import tech.streamfusion.proto.plan.v1.Deduplicate;
import tech.streamfusion.proto.plan.v1.Input;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;

class NativeDeduplicatePhysicalRestoreTest {
    private static final RowType TYPE =
            RowType.of(new BigIntType(false), new TimestampType(3), new VarCharType(VarCharType.MAX_LENGTH));
    private static final int ROWS = 1280;
    private static final String PAYLOAD = "abcdefgh".repeat(1024);

    @Test
    void importsAPhysicalCheckpointLargerThanWorkingMemoryAndPreservesState(@TempDir Path directory) {
        byte[] plan = plan();
        Path checkpoint = directory.resolve("checkpoint");
        var sourceMemory = TestingNativeMemoryManager.create();
        long source = NativeDeduplicateBridge.createRocksDb(
                plan, 1, 0, 0, directory.resolve("source"), 1L << 20, sourceMemory);
        try (RootAllocator allocator = new RootAllocator()) {
            try {
                for (int start = 0; start < ROWS; start += 64) {
                    List<RowData> rows = new ArrayList<>();
                    for (int key = start; key < start + 64; key++) {
                        rows.add(row(key, 1000));
                    }
                    try (ArrowRowDataBatch input = ArrowRowDataBatch.transpose(rows, TYPE, allocator);
                            NativeArrowDeduplicateResult output =
                                    ArrowDeduplicateCDataBridge.executeArrow(source, input, null, TYPE, allocator)) {
                        assertThat(output.size()).isEqualTo(rows.size());
                    }
                }
                assertThat(NativeDeduplicateBridge.snapshot(source, 0).length).isGreaterThan(4 << 20);
                NativeDeduplicateBridge.checkpointRocks(source, checkpoint);
            } finally {
                NativeDeduplicateBridge.destroy(source);
            }
            assertThat(sourceMemory.available()).isEqualTo(sourceMemory.limit());
            for (boolean rocks : new boolean[] {true, false}) {
                var targetMemory = TestingNativeMemoryManager.create(rocks ? 4L << 20 : 64L << 20);
                long target = rocks
                        ? NativeDeduplicateBridge.createRocksDb(
                                plan, 1, 0, 0, directory.resolve("target"), 1L << 20, targetMemory)
                        : NativeDeduplicateBridge.create(plan, 1, 0, 0, targetMemory);
                try {
                    NativeDeduplicateBridge.importRocksCheckpoint(target, checkpoint, 0, 0, 1L << 20);
                    assertThatThrownBy(() ->
                                    NativeDeduplicateBridge.importRocksCheckpoint(target, checkpoint, 0, 0, 1L << 20))
                            .isInstanceOf(IllegalStateException.class);
                    // Every key must retain its prior row: an older value cannot become a new insert.
                    for (int start = 0; start < ROWS; start += 16) {
                        List<RowData> rows = new ArrayList<>();
                        for (int key = start; key < start + 16; key++) {
                            rows.add(row(key, 500));
                        }
                        try (ArrowRowDataBatch input = ArrowRowDataBatch.transpose(rows, TYPE, allocator);
                                NativeArrowDeduplicateResult output = ArrowDeduplicateCDataBridge.executeArrow(
                                        target, input, null, TYPE, allocator)) {
                            assertThat(output.size()).isZero();
                        }
                    }
                    try (ArrowRowDataBatch input =
                                    ArrowRowDataBatch.transpose(List.of(row(ROWS - 1, 2000)), TYPE, allocator);
                            NativeArrowDeduplicateResult output =
                                    ArrowDeduplicateCDataBridge.executeArrow(target, input, null, TYPE, allocator)) {
                        ArrowRowDataBatch selected = output.selectEnvelopeFrom(input);
                        assertThat(selected.size()).isEqualTo(2);
                        assertThat(selected.rowKind(0)).isEqualTo(RowKind.UPDATE_BEFORE);
                        assertThat(selected.rowView(0).getTimestamp(1, 3).getMillisecond())
                                .isEqualTo(1000);
                        assertThat(selected.rowKind(1)).isEqualTo(RowKind.UPDATE_AFTER);
                        assertThat(selected.rowView(1).getTimestamp(1, 3).getMillisecond())
                                .isEqualTo(2000);
                        assertThat(selected.rowView(0).getString(2).toString()).isEqualTo(PAYLOAD);
                    }
                } finally {
                    NativeDeduplicateBridge.destroy(target);
                }
                assertThat(targetMemory.available()).isEqualTo(targetMemory.limit());
            }
        }
    }

    @Test
    void rejectsMissingAndIncompleteCheckpointsWithoutCreatingEmptyState(@TempDir Path directory) throws Exception {
        var memory = TestingNativeMemoryManager.create();
        long handle = NativeDeduplicateBridge.create(plan(), 1, 0, 0, memory);
        Path missing = directory.resolve("missing");
        Path empty = java.nio.file.Files.createDirectory(directory.resolve("empty"));
        Path corrupt = java.nio.file.Files.createDirectory(directory.resolve("corrupt"));
        byte[] invalidCurrent = "MANIFEST-999999\n".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        java.nio.file.Files.write(corrupt.resolve("CURRENT"), invalidCurrent);
        try {
            for (Path checkpoint : List.of(missing, empty, corrupt)) {
                assertThatThrownBy(
                                () -> NativeDeduplicateBridge.importRocksCheckpoint(handle, checkpoint, 0, 0, 1L << 20))
                        .isInstanceOf(IllegalStateException.class);
            }
            assertThat(java.nio.file.Files.exists(missing.resolve("CURRENT"))).isFalse();
            assertThat(java.nio.file.Files.exists(empty.resolve("CURRENT"))).isFalse();
            assertThat(java.nio.file.Files.readAllBytes(corrupt.resolve("CURRENT")))
                    .isEqualTo(invalidCurrent);
            assertThat(java.nio.file.Files.exists(corrupt.resolve("MANIFEST-999999")))
                    .isFalse();
        } finally {
            NativeDeduplicateBridge.destroy(handle);
        }
        assertThat(memory.available()).isEqualTo(memory.limit());
    }

    private static byte[] plan() {
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder()
                        .setDeduplicate(Deduplicate.newBuilder()
                                .setInput(Operator.newBuilder().setInput(Input.newBuilder()))
                                .addKeyIndices(0)
                                .setOrderIndex(1)
                                .setKeepLast(true)
                                .setGenerateUpdateBefore(true)
                                .setGenerateInsert(true)))
                .build()
                .toByteArray();
    }

    private static GenericRowData row(long key, long timestamp) {
        return GenericRowData.of(key, TimestampData.fromEpochMillis(timestamp), StringData.fromString(PAYLOAD));
    }
}
