/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFinalizer;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.operator.StreamFusionNativeRegionOperatorFactory;

/** Real Flink union clocks and keyed checkpoints through the shared Java/native region lifecycle. */
class SharedWindowRuntimeRecoveryTest {
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
    void restoredWindowClocksPrecedeInputAndClampReplayedWatermarks(boolean attached, boolean rocks, int mode)
            throws Exception {
        OperatorSubtaskState flinkState;
        OperatorSubtaskState nativeState;
        try (var allocator = new RootAllocator(64L << 20)) {
            try (var flink = oracle(attached, rocks, null);
                    var source = region(attached, rocks, null, 1, 0)) {
                input(attached, flink, source, allocator, 2, 2000);
                input(attached, flink, source, allocator, 5, 4000);
                watermark(attached, flink, source, 1999);
                input(attached, flink, source, allocator, 9, 2000);
                flink.prepareSnapshotPreBarrier(7);
                nativeState = snapshot(source, mode, 7);
                flinkState = flink.snapshot(7, 7);
                assertThat(nativeState.getManagedOperatorState()).isNotEmpty();
            }
            try (var flink = oracle(attached, rocks, flinkState);
                    var target = region(attached, mode == 0 ? !rocks : rocks, nativeState, 1, 0)) {
                // This is already fully late even for shared HOP windows. It must be dropped
                // before any replayed watermark has reached the restored operator.
                input(attached, flink, target, allocator, 99, -2000);
                watermark(attached, flink, target, 999);
                input(attached, flink, target, allocator, 7, 4000);
                var random = new java.util.Random(mode * 17L + (attached ? 1 : 0));
                for (int phase = 0; phase < 6; phase++) {
                    for (int row = 0; row < 5; row++)
                        input(
                                attached,
                                flink,
                                target,
                                allocator,
                                random.nextInt(99) + 1,
                                (phase + random.nextInt(7) - 3) * 2000L);
                    watermark(attached, flink, target, 3999 + phase * 2000L);
                }
                watermark(attached, flink, target, Long.MAX_VALUE);
            } finally {
                flinkState.discardState();
                nativeState.discardState();
            }
        }
    }

    @ParameterizedTest
    @CsvSource({
        "false,false,false",
        "false,true,false",
        "true,false,false",
        "true,true,false",
        "false,false,true",
        "false,true,true",
        "true,false,true",
        "true,true,true"
    })
    void rescalingTakesTheMinimumUnionClockEvenWithoutLiveKeyedWindows(
            boolean attached, boolean rocks, boolean initialClock) throws Exception {
        long minimum = initialClock ? Long.MIN_VALUE : 1999;
        OperatorSubtaskState firstState;
        OperatorSubtaskState secondState;
        try (var first = region(attached, rocks, null, 2, 0);
                var second = region(attached, rocks, null, 2, 1)) {
            first.processWatermark(0, new Watermark(minimum));
            second.processWatermark(0, new Watermark(5999));
            firstState = snapshot(first, 0, 1);
            secondState = snapshot(second, 0, 1);
        }
        var combined = AbstractStreamOperatorTestHarness.repackageState(firstState, secondState);
        var assigned = AbstractStreamOperatorTestHarness.repartitionOperatorState(combined, 16, 2, 1, 0);
        try (var target = region(attached, !rocks, assigned, 1, 0);
                var allocator = new RootAllocator(64L << 20);
                var flink = oracle(attached, rocks, null)) {
            flink.processWatermark(new Watermark(minimum));
            flink.getOutput().clear();
            // The maximum restored clock would incorrectly drop this window's partial.
            input(attached, flink, target, allocator, 7, 4000);
            watermark(attached, flink, target, 999);
            watermark(attached, flink, target, 3999);
            watermark(attached, flink, target, Long.MAX_VALUE);
        } finally {
            firstState.discardState();
            secondState.discardState();
        }
    }

    private static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> oracle(
            boolean attached, boolean rocks, OperatorSubtaskState restore) throws Exception {
        return attached
                ? GlobalWindowFlinkOracle.create(
                        SlicingWindowFlinkPlan.stage("GlobalWindowAggregate", AttachedSlicingWindowFixture.sql(true)),
                        rocks,
                        restore)
                : GlobalWindowFlinkOracle.create(rocks, restore);
    }

    private static RowType output(boolean attached) {
        return attached ? AttachedSlicingWindowFixture.OUTPUT : SharedSlicingWindowFixture.OUTPUT;
    }

    protected KeyedNativeMetricHarness region(
            boolean attached, boolean rocks, OperatorSubtaskState restore, int parallelism, int subtask)
            throws Exception {
        var factory = new StreamFusionNativeRegionOperatorFactory(
                List.of(SharedSlicingWindowFixture.INPUT),
                output(attached),
                attached ? AttachedSlicingWindowFixture.plan(true) : SharedSlicingWindowFixture.plan(),
                List.of(3L));
        return new KeyedNativeMetricHarness(rocks, factory, 1, output(attached), restore, parallelism, subtask);
    }

    private static OperatorSubtaskState snapshot(KeyedNativeMetricHarness source, int mode, long id) throws Exception {
        source.region().prepareSnapshotPreBarrier(id);
        if (mode == 0)
            return source.snapshotWithLocalState(id, id, SavepointType.savepoint(SavepointFormatType.CANONICAL))
                    .getJobManagerOwnedState();
        var location = CheckpointStorageLocationReference.getDefault();
        var options = mode == 1
                ? CheckpointOptions.alignedNoTimeout(CheckpointType.CHECKPOINT, location)
                : CheckpointOptions.unaligned(CheckpointType.CHECKPOINT, location);
        return OperatorSnapshotFinalizer.create(
                        source.region().snapshotState(id, id, options, new MemCheckpointStreamFactory(64 << 20)))
                .getJobManagerOwnedState();
    }

    private static void input(
            boolean attached,
            KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> flink,
            KeyedNativeMetricHarness target,
            RootAllocator allocator,
            long count,
            long end)
            throws Exception {
        var inputType = attached ? AttachedSlicingWindowFixture.FLINK_INPUT : SharedSlicingWindowFixture.FLINK_INPUT;
        var row = attached ? GenericRowData.of(1L, count, 1L, end) : GenericRowData.of(1L, count, end);
        flink.processElement(new StreamRecord<>(new RowDataSerializer(inputType).toBinaryRow(row), 123));
        var nativeRow = GenericRowData.of(
                1L,
                attached ? AttachedSlicingWindowFixture.partial(count, 1) : SharedSlicingWindowFixture.count(count),
                end - (attached ? 6000 : 2000),
                end);
        try (var batch = ArrowRowDataBatch.transpose(List.of(nativeRow), SharedSlicingWindowFixture.INPUT, allocator)) {
            target.processElement(0, new StreamRecord<>(batch));
        }
        compare(attached, flink, target);
    }

    private static void watermark(
            boolean attached,
            KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> flink,
            KeyedNativeMetricHarness target,
            long value)
            throws Exception {
        flink.processWatermark(new Watermark(value));
        target.processWatermark(0, new Watermark(value));
        compare(attached, flink, target);
    }

    private static void compare(
            boolean attached,
            KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> flink,
            KeyedNativeMetricHarness target)
            throws Exception {
        var expected = new DataOutputSerializer(128);
        for (var event : flink.getOutput()) StageEventBytes.encode(output(attached), (StreamElement) event, expected);
        flink.getOutput().clear();
        target.drainControls();
        for (var output : target.outputs) {
            assertThat(output.getCopyOfBuffer()).containsExactly(expected.getCopyOfBuffer());
            output.clear();
        }
        var referenceMetrics =
                RegisteredMetricSurface.metrics(flink.getOperator().getMetricGroup());
        var nativeMetrics = RegisteredMetricSurface.metrics(target.stage(3));
        var names = List.of("numLateRecordsDropped", "lateRecordsDroppedRate", "watermarkLatency");
        referenceMetrics.keySet().retainAll(names);
        nativeMetrics.keySet().retainAll(names);
        assertThat(referenceMetrics).hasSize(3);
        RegisteredMetricSurface.compare(referenceMetrics, nativeMetrics);
    }
}
