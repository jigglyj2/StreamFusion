/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;
import static tech.streamfusion.flink.planner.SharedSessionWindowFixture.*;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.flink.arrow.ArrowNativePlanDispatcher;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowWindowAggregateCDataBridge;
import tech.streamfusion.nativebridge.NativeExecutionContext;
import tech.streamfusion.nativebridge.NativeStateResources;
import tech.streamfusion.nativebridge.NativeWindowAggregateBridge;
import tech.streamfusion.proto.plan.v1.NativeControlInvocation;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.NativeStageControl;

/** Retained SFWS/SFWI state migrates through the production JNI restore into the shared tree. */
class GeneratedLegacySessionMigrationParityTest {
    @ParameterizedTest(name = "targetRocks={0}, physical={1}")
    @CsvSource({"false,false", "true,false", "false,true", "true,true"})
    void generatedMergesAfterMigrationMatchCompleteFlinkChangelog(
            boolean rocks, boolean physical, @TempDir Path directory) throws Exception {
        var inputSerializer = new RowDataSerializer(INPUT);
        for (int seed : List.of(3, 19, 71)) {
            var memory = new SharedAggregateRegionParityTest.Memory();
            var snapshots = new ArrayList<byte[]>();
            Path checkpoint = directory.resolve("checkpoint-" + seed);
            try (var flink = GlobalWindowFlinkOracle.create(
                            SlicingWindowFlinkPlan.stage("WindowAggregate", SQL), rocks, null);
                    var allocator = new RootAllocator(64L << 20)) {
                var currentPlan = NativePlan.parseFrom(plan());
                var legacyPlan = currentPlan.toBuilder()
                        .setRoot(currentPlan.getRoot().getCalc().getInput())
                        .build()
                        .toByteArray();
                long legacy = physical
                        ? NativeWindowAggregateBridge.createRocksDb(
                                legacyPlan, 16, 0, 15, directory.resolve("legacy-" + seed), 1L << 20, memory)
                        : NativeWindowAggregateBridge.create(legacyPlan, 16, 0, 15, memory);
                try {
                    var initial = rows(seed, false);
                    for (var row : initial)
                        flink.processElement(new StreamRecord<>(inputSerializer.toBinaryRow(row), 123));
                    try (var batch = ArrowRowDataBatch.transpose(initial, INPUT, allocator);
                            var output = ArrowWindowAggregateCDataBridge.process(
                                    legacy, batch, null, false, 0, OUTPUT, allocator, memory)) {
                        compare(flink, output);
                    }
                    flink.processWatermark(new Watermark(15000));
                    try (var output =
                            ArrowWindowAggregateCDataBridge.advance(legacy, false, 15000, OUTPUT, allocator, memory)) {
                        compare(flink, output);
                    }
                    if (physical) NativeWindowAggregateBridge.checkpointRocks(legacy, checkpoint);
                    else
                        for (int group = 0; group < 16; group++)
                            snapshots.add(NativeWindowAggregateBridge.snapshot(legacy, group));
                } finally {
                    NativeWindowAggregateBridge.destroy(legacy);
                }
                assertThat(memory.available()).isEqualTo(memory.limit());
                var binding = (rocks
                                ? NativeStateResources.rocksDb(
                                        3, 16, 0, 15, directory.resolve("target-" + seed), 1L << 20)
                                : NativeStateResources.memory(3, 16, 0, 15))
                        .toBuilder().setRestoredWatermark(15000).build();
                try (var context = new NativeExecutionContext(
                                plan(), memory, NativeStateResources.serialize(List.of(binding)));
                        var dispatcher = new ArrowNativePlanDispatcher(context, List.of(INPUT), OUTPUT, allocator)) {
                    if (physical) context.state().importCheckpoint(3, checkpoint, 0, 15, 1L << 20);
                    else
                        for (int group = 0; group < 16; group++)
                            context.state().restore(3, group, snapshots.get(group));
                    var later = rows(seed, true);
                    for (int offset = 0; offset < later.size(); offset += 7) {
                        var rows = later.subList(offset, Math.min(later.size(), offset + 7));
                        for (var row : rows)
                            flink.processElement(new StreamRecord<>(inputSerializer.toBinaryRow(row), 123));
                        var actual = new DataOutputSerializer(128);
                        try (var batch = ArrowRowDataBatch.transpose(rows, INPUT, allocator)) {
                            dispatcher.process(0, batch, output -> append(output, actual));
                        }
                        compare(flink, actual);
                    }
                    // Unique frontiers preserve Flink's exact row ordering across key groups.
                    for (long base : List.of(49999L, 109999L))
                        for (int key = 0; key < 9; key++) {
                            long watermark = base + key;
                            flink.processWatermark(new Watermark(watermark));
                            var actual = new DataOutputSerializer(128);
                            dispatcher.control(
                                    NativeControlInvocation.newBuilder()
                                            .setProtocolVersion(1)
                                            .addStages(NativeStageControl.newBuilder()
                                                    .setPlanNodeId(3)
                                                    .setWatermarkMillis(watermark))
                                            .build()
                                            .toByteArray(),
                                    output -> append(output, actual));
                            compare(flink, actual);
                        }
                }
                assertThat(memory.available()).isEqualTo(memory.limit());
                assertThat(allocator.getAllocatedMemory()).isZero();
            }
        }
    }

    private static List<RowData> rows(int seed, boolean later) {
        var random = new Random(seed);
        var rows = new ArrayList<RowData>();
        for (int key = 0; key < 9; key++) {
            var times = later ? List.of(-30000L, 0L, 20000L, 30000L, 100000L) : List.of(10000L, 40000L);
            for (long time : times)
                rows.add(GenericRowData.of(key == 8 ? null : (long) key, TimestampData.fromEpochMillis(time + key)));
            for (int i = 0, count = random.nextInt(5); i < count; i++)
                rows.add(GenericRowData.of(key == 8 ? null : (long) key, TimestampData.fromEpochMillis(10000 + key)));
        }
        return rows;
    }

    private static void append(ArrowRowDataBatch output, DataOutputSerializer bytes) {
        var serializer = new RowDataSerializer(OUTPUT);
        try {
            for (int row = 0; row < output.size(); row++) {
                assertThat(output.hasTimestamp(row)).isFalse();
                var value = output.rowView(row);
                value.setRowKind(output.rowKind(row));
                serializer.serialize(value, bytes);
            }
        } catch (java.io.IOException error) {
            throw new RuntimeException(error);
        }
    }

    private static void compare(
            KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> flink, ArrowRowDataBatch output)
            throws Exception {
        var actual = new DataOutputSerializer(128);
        append(output, actual);
        compare(flink, actual);
    }

    private static void compare(
            KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> flink, DataOutputSerializer actual)
            throws Exception {
        var expected = new DataOutputSerializer(128);
        var serializer = new RowDataSerializer(OUTPUT);
        for (var event : flink.getOutput())
            if (event instanceof StreamRecord<?>) {
                var record = (StreamRecord<?>) event;
                assertThat(record.hasTimestamp()).isFalse();
                serializer.serialize((RowData) record.getValue(), expected);
            }
        flink.getOutput().clear();
        assertThat(actual.getCopyOfBuffer()).containsExactly(expected.getCopyOfBuffer());
    }
}
