/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;

/** DISTINCT window state and restored clocks through canonical, aligned and unaligned snapshots. */
class DistinctWindowRecoveryTest {
    @ParameterizedTest
    @CsvSource({
        "false,false,0",
        "false,true,0",
        "true,false,0",
        "true,true,0",
        "false,false,1",
        "false,true,1",
        "true,false,1",
        "true,true,1",
        "false,false,2",
        "false,true,2",
        "true,false,2",
        "true,true,2"
    })
    void restoredCompositeKeysKeepPresenceAndLateRecordMetrics(boolean strings, boolean rocks, int mode)
            throws Exception {
        var fixture = new DistinctWindowFixture(strings);
        OperatorSubtaskState referenceState;
        OperatorSubtaskState nativeState;
        try (var allocator = new RootAllocator(64L << 20)) {
            try (var flink = fixture.oracle(rocks, null);
                    var source = region(fixture, rocks, null, 1, 0)) {
                input(fixture, flink, source, allocator, 0, 2000);
                input(fixture, flink, source, allocator, 1, 4000);
                watermark(fixture, flink, source, 1999);
                flink.prepareSnapshotPreBarrier(7);
                nativeState = SharedWindowRuntimeRecoveryTest.snapshot(source, mode, 7);
                referenceState = flink.snapshot(7, 7);
                assertThat(nativeState.getManagedOperatorState()).isNotEmpty();
            }
            try (var flink = fixture.oracle(rocks, referenceState);
                    var target = region(fixture, mode == 0 ? !rocks : rocks, nativeState, 1, 0)) {
                input(fixture, flink, target, allocator, 2, -2000);
                watermark(fixture, flink, target, 999);
                for (int phase = 0; phase < 6; phase++) {
                    input(fixture, flink, target, allocator, phase + 3, (phase + 2) * 2000L);
                    watermark(fixture, flink, target, (phase + 2) * 2000L - 1);
                }
                watermark(fixture, flink, target, Long.MAX_VALUE);
            } finally {
                referenceState.discardState();
                nativeState.discardState();
            }
        }
    }

    static KeyedNativeMetricHarness region(
            DistinctWindowFixture fixture, boolean rocks, OperatorSubtaskState state, int parallelism, int subtask)
            throws Exception {
        return new KeyedNativeMetricHarness(rocks, fixture.factory(), 1, fixture.output, state, parallelism, subtask);
    }

    static void input(
            DistinctWindowFixture fixture,
            KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> flink,
            KeyedNativeMetricHarness target,
            RootAllocator allocator,
            int phase,
            long end)
            throws Exception {
        var rows = new ArrayList<RowData>();
        var serializer = new RowDataSerializer(fixture.flinkInput);
        for (int row = 0; row < 64; row++) {
            int id = (phase * 7 + row) % 64;
            flink.processElement(new StreamRecord<>(serializer.toBinaryRow(fixture.row(id, end, false)), 123));
            rows.add(fixture.row(id, end, true));
        }
        try (var batch = ArrowRowDataBatch.transpose(rows, fixture.input, allocator)) {
            target.processElement(0, new StreamRecord<>(batch));
        }
        compare(fixture, flink, target);
    }

    static void watermark(
            DistinctWindowFixture fixture,
            KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> flink,
            KeyedNativeMetricHarness target,
            long value)
            throws Exception {
        flink.processWatermark(new Watermark(value));
        target.processWatermark(0, new Watermark(value));
        compare(fixture, flink, target);
    }

    static void compare(
            DistinctWindowFixture fixture,
            KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> flink,
            KeyedNativeMetricHarness target)
            throws Exception {
        var expected = new DataOutputSerializer(128);
        for (var event : flink.getOutput()) StageEventBytes.encode(fixture.output, (StreamElement) event, expected);
        flink.getOutput().clear();
        target.drainControls();
        assertThat(WindowTimerEventBytes.canonical(fixture.output, fixture.keys + 1, target.output.getCopyOfBuffer()))
                .containsExactly(
                        WindowTimerEventBytes.canonical(fixture.output, fixture.keys + 1, expected.getCopyOfBuffer()));
        target.output.clear();
        var reference = RegisteredMetricSurface.metrics(flink.getOperator().getMetricGroup());
        var actual = RegisteredMetricSurface.metrics(target.stage(3));
        var names = List.of("numLateRecordsDropped", "lateRecordsDroppedRate", "watermarkLatency");
        reference.keySet().retainAll(names);
        actual.keySet().retainAll(names);
        assertThat(reference).hasSize(3);
        RegisteredMetricSurface.compare(reference, actual);
    }
}
