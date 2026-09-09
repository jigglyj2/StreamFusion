/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;
import static tech.streamfusion.flink.planner.SharedProcessingWindowFixture.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;

/** Identical controlled clocks through the SQL-generated Flink operator and shared native factory. */
class SharedProcessingWindowParityTest {
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void generatedClockTransitionsAndTerminalControlsMatchCompleteFlinkChangelog(boolean rocks) throws Exception {
        for (int gap : List.of(1000, 10000, 37000))
            for (int seed : List.of(3, 19, 71)) {
                var random = new Random(seed);
                try (var flink = oracle(rocks, gap, null);
                        var target = new KeyedNativeMetricHarness(rocks, factory(gap), 1, OUTPUT, null, 1, 0);
                        var allocator = new RootAllocator(64L << 20)) {
                    for (int window = 0; window < 3; window++) {
                        long start = window * (long) gap;
                        Long key = window == 1 ? null : (long) seed;
                        time(flink, target, start + 1);
                        input(flink, target, allocator, key, 1 + random.nextInt(47), 7, true);
                        time(flink, target, start + gap - 2);
                        input(flink, target, allocator, key, 1 + random.nextInt(47), 31);
                        // Flink's network watermark valve forwards strictly advancing values.
                        var watermark = new Watermark(start + gap);
                        watermark(flink, target, watermark);
                        time(flink, target, start + gap - 1);
                    }
                    time(flink, target, 3L * gap + 1);
                    input(flink, target, allocator, null, 3, 1);
                    watermark(flink, target, Watermark.MAX_WATERMARK);
                    checkpoint(flink, target, 8);
                    flink.getOperator().finish();
                    target.region().finish();
                    compare(flink, target);
                    assertThat(target.maxOutputBatchRows).isPositive();
                }
            }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void backwardAndRepeatedTimersPreserveBufferedVisibilityIncludingZeroCount(boolean rocks) throws Exception {
        for (int gap : List.of(1000, 10000, 37000)) {
            try (var flink = oracle(rocks, gap, null);
                    var target = new KeyedNativeMetricHarness(rocks, factory(gap), 1, OUTPUT, null, 1, 0);
                    var allocator = new RootAllocator(64L << 20)) {
                time(flink, target, gap + 1);
                input(flink, target, allocator, null, 2, 1);
                time(flink, target, 2L * gap - 1);
                time(flink, target, 1);
                input(flink, target, allocator, null, 3, 2);
                time(flink, target, gap - 1);
                checkpoint(flink, target, 9);
                time(flink, target, 1);
                input(flink, target, allocator, null, 5, 3);
                time(flink, target, gap - 1);
                checkpoint(flink, target, 10);
                time(flink, target, 1);
                input(flink, target, allocator, null, 1, 1);
                time(flink, target, gap - 1);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void checkpointRestorePreservesPendingTimersAndBufferedCounts(boolean rocks) throws Exception {
        OperatorSubtaskState flinkSnapshot;
        OperatorSubtaskState nativeSnapshot;
        try (var flink = oracle(rocks, 10000, null);
                var target = new KeyedNativeMetricHarness(rocks, factory(10000), 1, OUTPUT, null, 1, 0);
                var allocator = new RootAllocator(64L << 20)) {
            time(flink, target, 10001);
            input(flink, target, allocator, null, 4, 3);
            checkpoint(flink, target, 1);
            flinkSnapshot = flink.snapshot(1, 10002);
            nativeSnapshot = target.snapshot(1, 10002);
        }
        try (var flink = oracle(rocks, 10000, flinkSnapshot);
                var target = new KeyedNativeMetricHarness(rocks, factory(10000), 1, OUTPUT, nativeSnapshot, 1, 0);
                var allocator = new RootAllocator(64L << 20)) {
            watermark(flink, target, Watermark.MAX_WATERMARK);
            time(flink, target, 19998);
            input(flink, target, allocator, null, 2, 1);
            time(flink, target, 19999);
        } finally {
            flinkSnapshot.discardState();
            nativeSnapshot.discardState();
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

    private static void checkpoint(
            KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> flink,
            KeyedNativeMetricHarness target,
            long id)
            throws Exception {
        flink.prepareSnapshotPreBarrier(id);
        target.region().prepareSnapshotPreBarrier(id);
        compare(flink, target);
    }

    private static void input(
            KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> flink,
            KeyedNativeMetricHarness target,
            RootAllocator allocator,
            Long key,
            int count,
            int batchSize)
            throws Exception {
        input(flink, target, allocator, key, count, batchSize, false);
    }

    private static void input(
            KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> flink,
            KeyedNativeMetricHarness target,
            RootAllocator allocator,
            Long key,
            int count,
            int batchSize,
            boolean ipc)
            throws Exception {
        for (int offset = 0; offset < count; offset += batchSize) {
            int size = Math.min(batchSize, count - offset);
            flink.getOperator()
                    .getMetricGroup()
                    .getIOMetricGroup()
                    .getNumRecordsInCounter()
                    .inc(size);
            ProcessingTimeWindowClockTest.input(flink, key, size);
            var rows = new ArrayList<RowData>();
            for (int i = 0; i < size; i++) rows.add(GenericRowData.of(key, null));
            try (var batch = ArrowRowDataBatch.transpose(rows, INPUT, allocator)) {
                if (ipc) {
                    try (var envelope =
                            tech.streamfusion.flink.exchange.ArrowExchangeBatch.withEnvelope(batch, INPUT)) {
                        for (var frame : tech.streamfusion.flink.arrow.ArrowExchangeCDataBridge.route(
                                tech.streamfusion.flink.exchange.NativeExchangePlanSerializer.singleton(INPUT),
                                envelope.batch(),
                                allocator,
                                target.memory)) {
                            target.processElement(0, new StreamRecord<>(frame));
                        }
                    }
                } else target.processElement(0, new StreamRecord<>(batch, 123));
            }
            compare(flink, target);
        }
    }

    private static void compare(
            KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> flink, KeyedNativeMetricHarness target)
            throws Exception {
        var expected = new DataOutputSerializer(128);
        var metrics = RegisteredMetricSurface.metrics(flink.getOperator().getMetricGroup());
        for (var event : flink.getOutput()) {
            if (event instanceof StreamRecord<?>)
                flink.getOperator()
                        .getMetricGroup()
                        .getIOMetricGroup()
                        .getNumRecordsOutCounter()
                        .inc();
            if (event instanceof Watermark)
                ((org.apache.flink.streaming.runtime.metrics.WatermarkGauge) metrics.get("currentOutputWatermark"))
                        .setCurrentWatermark(((Watermark) event).getTimestamp());
            StageEventBytes.encode(OUTPUT, (StreamElement) event, expected);
        }
        flink.getOutput().clear();
        target.drainControls();
        assertThat(target.output.getCopyOfBuffer()).containsExactly(expected.getCopyOfBuffer());
        target.output.clear();
        var actual = RegisteredMetricSurface.metrics(target.stage(3));
        ((org.apache.flink.metrics.MeterView) metrics.get("lateRecordsDroppedRate")).update();
        ((org.apache.flink.metrics.MeterView) actual.get("lateRecordsDroppedRate")).update();
        RegisteredMetricSurface.compare(metrics, actual);
    }

    private static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> oracle(
            boolean rocks, int gap, OperatorSubtaskState restored) throws Exception {
        var flink = ProcessingTimeWindowClockTest.oracle(rocks, gap, restored);
        flink.getOperator()
                .getMetricGroup()
                .gauge("currentInputWatermark", new org.apache.flink.streaming.runtime.metrics.WatermarkGauge());
        flink.getOperator()
                .getMetricGroup()
                .gauge("currentOutputWatermark", new org.apache.flink.streaming.runtime.metrics.WatermarkGauge());
        return flink;
    }

    private static void watermark(
            KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> flink,
            KeyedNativeMetricHarness target,
            Watermark watermark)
            throws Exception {
        var metrics = RegisteredMetricSurface.metrics(flink.getOperator().getMetricGroup());
        ((org.apache.flink.streaming.runtime.metrics.WatermarkGauge) metrics.get("currentInputWatermark"))
                .setCurrentWatermark(watermark.getTimestamp());
        flink.processWatermark(watermark);
        target.processWatermark(0, watermark);
        compare(flink, target);
    }
}
