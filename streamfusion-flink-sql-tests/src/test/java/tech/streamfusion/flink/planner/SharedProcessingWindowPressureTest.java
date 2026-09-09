/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;
import static tech.streamfusion.flink.planner.SharedProcessingWindowFixture.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.exchange.NativeExchangePlanSerializer;

/** A repeated timer exposes exactly which updates Flink's bounded buffer published under pressure. */
class SharedProcessingWindowPressureTest {
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void originalFlinkMemoryShareDeterminesPressureFlushesAcrossArrowBatchBoundaries(boolean rocks) throws Exception {
        for (int batchSize : List.of(511, 4096)) {
            var factory = factory(10000, NativeExchangePlanSerializer.singleton(INPUT), 6, 64);
            try (var flink = ProcessingTimeWindowClockTest.oracle(rocks, 10000, null, 6.0 / 64);
                    var target = new KeyedNativeMetricHarness(rocks, factory, 1, OUTPUT, null, 1, 0);
                    var allocator = new RootAllocator(64L << 20)) {
                time(flink, target, 10001);
                input(flink, target, allocator, 2, batchSize);
                time(flink, target, 19999);
                time(flink, target, 1);
                input(flink, target, allocator, 300000, batchSize);
                flink.setProcessingTime(9999);
                target.setProcessingTime(9999);
                var counts = flink.getOutput().stream()
                        .filter(StreamRecord.class::isInstance)
                        .map(event -> ((RowData) ((StreamRecord<?>) event).getValue()).getLong(1))
                        .collect(java.util.stream.Collectors.toList());
                assertThat(counts).hasSize(1);
                assertThat(counts.get(0)).isStrictlyBetween(0L, 300000L);
                long published = counts.get(0);
                compare(flink, target);
                flink.prepareSnapshotPreBarrier(1);
                target.region().prepareSnapshotPreBarrier(1);
                // All remaining updates are now in keyed state, but the timer was already removed.
                compare(flink, target);
                time(flink, target, 1);
                input(flink, target, allocator, 1, batchSize);
                flink.setProcessingTime(9999);
                target.setProcessingTime(9999);
                var row = (RowData) ((StreamRecord<?>) flink.getOutput().peek()).getValue();
                assertThat(row.getLong(1)).isEqualTo(300000 - published);
                compare(flink, target);
            }
        }
    }

    private static void input(
            KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> flink,
            KeyedNativeMetricHarness target,
            RootAllocator allocator,
            int count,
            int batchSize)
            throws Exception {
        for (int offset = 0; offset < count; offset += batchSize) {
            int size = Math.min(batchSize, count - offset);
            ProcessingTimeWindowClockTest.input(flink, null, size);
            var rows = new ArrayList<RowData>();
            for (int i = 0; i < size; i++) rows.add(GenericRowData.of(null, null));
            try (var batch = ArrowRowDataBatch.transpose(rows, INPUT, allocator)) {
                target.processElement(0, new StreamRecord<>(batch));
            }
            compare(flink, target);
        }
    }

    private static void time(
            KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> flink,
            KeyedNativeMetricHarness target,
            long time)
            throws Exception {
        flink.setProcessingTime(time);
        target.setProcessingTime(time);
        compare(flink, target);
    }

    private static void compare(
            KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> flink, KeyedNativeMetricHarness target)
            throws Exception {
        var expected = new DataOutputSerializer(128);
        for (var event : flink.getOutput()) StageEventBytes.encode(OUTPUT, (StreamElement) event, expected);
        flink.getOutput().clear();
        target.drainControls();
        assertThat(target.output.getCopyOfBuffer()).containsExactly(expected.getCopyOfBuffer());
        target.output.clear();
    }
}
