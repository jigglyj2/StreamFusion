/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;
import static tech.streamfusion.flink.planner.SharedWindowJoinFixture.*;

import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.flink.arrow.ArrowExchangeCDataBridge;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.exchange.ArrowExchangeBatch;
import tech.streamfusion.flink.exchange.NativeExchangePlanSerializer;
import tech.streamfusion.flink.operator.StreamFusionNativeRegionOperatorFactory;

class SharedWindowJoinRescalingTest {
    @ParameterizedTest
    @CsvSource({"false,0", "true,0", "false,1", "true,1", "false,2", "true,2"})
    void pendingWindowsAndDuplicateOrderSurviveFlinkRepartitioning(boolean rocks, int mode) throws Exception {
        var fixture = new SharedWindowJoinFixture(true, true);
        try (var oracle = fixture.join(rocks);
                var calc = fixture.calc();
                var allocator = new RootAllocator(64L << 20)) {
            OperatorSubtaskState snapshot;
            try (var source = region(fixture, rocks, null, 1, 0)) {
                for (int key = 0; key < 32; key++) {
                    send(List.of(source), oracle, allocator, 0, row(key, "left-a"));
                    send(List.of(source), oracle, allocator, 0, row(key, "left-b"));
                    send(List.of(source), oracle, allocator, 1, row(key, "right-a"));
                }
                assertThat(source.output.length()).isZero();
                snapshot = snapshot(source, mode);
            }
            var first = AbstractStreamOperatorTestHarness.repartitionOperatorState(snapshot, 16, 1, 2, 0);
            var second = AbstractStreamOperatorTestHarness.repartitionOperatorState(snapshot, 16, 1, 2, 1);
            boolean targetRocks = mode == 0 ? !rocks : rocks;
            try (var lower = region(fixture, targetRocks, first, 2, 0);
                    var upper = region(fixture, targetRocks, second, 2, 1)) {
                var targets = List.of(lower, upper);
                for (int key = 0; key < 32; key++) send(targets, oracle, allocator, 1, row(key, "right-b"));
                var expected = new DataOutputSerializer(128);
                var actual = new DataOutputSerializer(128);
                // Distinct deadlines make order deterministic across partitions. Both sides
                // retain duplicate insertion order within each closed window.
                for (int key = 0; key < 32; key++) {
                    var watermark = new Watermark(999 + key * 10L);
                    for (int port = 0; port < 2; port++) {
                        oracle.accept(port, watermark);
                        for (var target : targets) target.processWatermark(port, watermark);
                    }
                    for (var event : oracle.drain()) calc.accept(event);
                    for (var event : calc.drain())
                        if (event instanceof StreamRecord) StageEventBytes.encode(OUTPUT, event, expected);
                    for (var target : targets) {
                        // Runtime controls have their own stage/metric parity tests.
                        actual.write(target.output.getCopyOfBuffer());
                        target.output.clear();
                        target.controls.clear();
                    }
                    assertThat(actual.getCopyOfBuffer())
                            .as("rocks=%s mode=%s key=%s", rocks, mode, key)
                            .isEqualTo(expected.getCopyOfBuffer());
                }
                assertThat(expected.length()).isPositive();
            } finally {
                snapshot.discardState();
            }
        }
    }

    private static GenericRowData row(int key, String label) {
        return GenericRowData.of((long) key, 1000 + key * 10L, 5L, StringData.fromString(label));
    }

    private static byte[] exchange(int parallelism) {
        return NativeExchangePlanSerializer.hash(INPUT, new int[] {0}, 16, parallelism, true);
    }

    private static KeyedNativeMetricHarness region(
            SharedWindowJoinFixture fixture, boolean rocks, OperatorSubtaskState restore, int parallelism, int subtask)
            throws Exception {
        byte[] exchange = exchange(parallelism);
        return new KeyedNativeMetricHarness(
                rocks,
                new StreamFusionNativeRegionOperatorFactory(
                        List.of(INPUT, INPUT), OUTPUT, fixture.plan(), List.of(id(0)), List.of(exchange, exchange)),
                2,
                OUTPUT,
                restore,
                parallelism,
                subtask);
    }

    private static void send(
            List<KeyedNativeMetricHarness> targets,
            FlinkJoinMetricOracle oracle,
            RootAllocator allocator,
            int port,
            GenericRowData row)
            throws Exception {
        oracle.accept(
                port,
                new StreamRecord<RowData>(
                        new RowDataSerializer(INPUT).toBinaryRow(row).copy()));
        try (var batch = ArrowRowDataBatch.transpose(List.of(row), INPUT, allocator);
                var envelope = ArrowExchangeBatch.withEnvelope(batch, INPUT, null)) {
            for (var frame : ArrowExchangeCDataBridge.route(
                    exchange(targets.size()), envelope.batch(), allocator, targets.get(0).memory)) {
                int subtask =
                        KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(16, targets.size(), frame.keyGroup());
                targets.get(subtask).processElement(port, new StreamRecord<>(frame));
            }
        }
    }

    private static OperatorSubtaskState snapshot(KeyedNativeMetricHarness source, int mode) throws Exception {
        source.region().prepareSnapshotPreBarrier(1);
        if (mode == 0)
            return source.snapshotWithLocalState(
                            1,
                            1,
                            org.apache.flink.runtime.checkpoint.SavepointType.savepoint(
                                    org.apache.flink.core.execution.SavepointFormatType.CANONICAL))
                    .getJobManagerOwnedState();
        var location = org.apache.flink.runtime.state.CheckpointStorageLocationReference.getDefault();
        var options = mode == 1
                ? org.apache.flink.runtime.checkpoint.CheckpointOptions.alignedNoTimeout(
                        org.apache.flink.runtime.checkpoint.CheckpointType.CHECKPOINT, location)
                : org.apache.flink.runtime.checkpoint.CheckpointOptions.unaligned(
                        org.apache.flink.runtime.checkpoint.CheckpointType.CHECKPOINT, location);
        return org.apache.flink.streaming.api.operators.OperatorSnapshotFinalizer.create(source.region()
                        .snapshotState(
                                1,
                                1,
                                options,
                                new org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory(64 << 20)))
                .getJobManagerOwnedState();
    }
}
