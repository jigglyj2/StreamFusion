/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;
import static tech.streamfusion.flink.planner.SharedSessionWindowFixture.*;
import static tech.streamfusion.flink.planner.SharedWindowRuntimeRecoveryTest.snapshot;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.flink.arrow.ArrowExchangeCDataBridge;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.exchange.ArrowExchangeBatch;
import tech.streamfusion.flink.exchange.NativeExchangePlanSerializer;
import tech.streamfusion.flink.operator.StreamFusionNativeRegionOperatorFactory;

/** Session merging across Flink-managed snapshots, key-group repartition and Arrow IPC routing. */
class SharedSessionWindowRecoveryTest {
    @ParameterizedTest(name = "rocks={0}, mode={1}")
    @CsvSource({"false,0", "true,0", "false,1", "true,1", "false,2", "true,2"})
    void restoresAndRescalesLiveMergingNamespaces(boolean rocks, int mode) throws Exception {
        for (int seed : List.of(3, 19, 71)) {
            var states = new ArrayList<OperatorSubtaskState>();
            try (var flink = GlobalWindowFlinkOracle.create(
                            SlicingWindowFlinkPlan.stage("WindowAggregate", SQL), rocks, null);
                    var allocator = new RootAllocator(64L << 20)) {
                OperatorSubtaskState initial;
                try (var source = region(rocks, null, 1, 0)) {
                    input(flink, List.of(source), allocator, seed, 0);
                    watermark(flink, List.of(source), 15000);
                    initial = snapshot(source, mode, 1);
                    states.add(initial);
                    assertThat(initial.getManagedOperatorState()).isNotEmpty();
                    if (rocks && mode != 0) {
                        var base = (IncrementalRemoteKeyedStateHandle)
                                initial.getManagedKeyedState().iterator().next();
                        source.notifyOfCompletedCheckpoint(1);
                        initial = snapshot(source, mode, 2);
                        states.add(initial);
                        var next = (IncrementalRemoteKeyedStateHandle)
                                initial.getManagedKeyedState().iterator().next();
                        assertThat(next.getCheckpointedSize()).isLessThan(base.getCheckpointedSize());
                    } else assertThat(initial.getRawKeyedState()).hasSize(1);
                }
                var combined = AbstractStreamOperatorTestHarness.repackageState(initial);
                var zeroState = AbstractStreamOperatorTestHarness.repartitionOperatorState(combined, 16, 1, 2, 0);
                var oneState = AbstractStreamOperatorTestHarness.repartitionOperatorState(combined, 16, 1, 2, 1);
                boolean scaledRocks = mode == 0 ? !rocks : rocks;
                try (var zero = region(scaledRocks, zeroState, 2, 0);
                        var one = region(scaledRocks, oneState, 2, 1)) {
                    input(flink, List.of(zero, one), allocator, seed, 1);
                    var zeroCheckpoint = snapshot(zero, mode, 3);
                    var oneCheckpoint = snapshot(one, mode, 3);
                    states.add(zeroCheckpoint);
                    states.add(oneCheckpoint);
                    combined = AbstractStreamOperatorTestHarness.repackageState(zeroCheckpoint, oneCheckpoint);
                }
                var back = AbstractStreamOperatorTestHarness.repartitionOperatorState(combined, 16, 2, 1, 0);
                try (var target = region(rocks, back, 1, 0)) {
                    input(flink, List.of(target), allocator, seed, 2);
                    // Distinct timer frontiers retain exact row order, without canonicalizing ties.
                    for (int key = 0; key < 65; key++) watermark(flink, List.of(target), 49999 + key);
                    for (int key = 0; key < 65; key++) watermark(flink, List.of(target), 79999 + key);
                    watermark(flink, List.of(target), Long.MAX_VALUE);
                }
                assertThat(allocator.getAllocatedMemory()).isZero();
            } finally {
                for (var state : states) state.discardState();
            }
        }
    }

    static KeyedNativeMetricHarness region(boolean rocks, OperatorSubtaskState state, int parallelism, int subtask)
            throws Exception {
        var factory = new StreamFusionNativeRegionOperatorFactory(
                List.of(INPUT), OUTPUT, plan(), List.of(3L), List.of(exchange(parallelism)));
        return new KeyedNativeMetricHarness(rocks, factory, 1, OUTPUT, state, parallelism, subtask);
    }

    private static byte[] exchange(int parallelism) {
        return NativeExchangePlanSerializer.hash(INPUT, new int[] {0}, 16, parallelism, true);
    }

    private static void input(
            KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> flink,
            List<KeyedNativeMetricHarness> targets,
            RootAllocator allocator,
            int seed,
            int phase)
            throws Exception {
        var rows = new ArrayList<RowData>();
        var serializer = new RowDataSerializer(INPUT);
        var random = new Random(seed * 17L + phase);
        for (int key = 0; key < 65; key++) {
            var times = phase == 0
                    ? List.of(10000L, 40000L)
                    : phase == 1 ? List.of(-20000L, 0L, 20000L, 30000L) : List.of(5000L, 70000L);
            for (long time : times) {
                var row = GenericRowData.of(key == 64 ? null : (long) key, TimestampData.fromEpochMillis(time + key));
                rows.add(row);
                flink.processElement(new StreamRecord<>(serializer.toBinaryRow(row), 123));
            }
            for (int i = 0, repeats = random.nextInt(5); i < repeats; i++) {
                var row = GenericRowData.of(key == 64 ? null : (long) key, TimestampData.fromEpochMillis(10000 + key));
                rows.add(row);
                flink.processElement(new StreamRecord<>(serializer.toBinaryRow(row), 123));
            }
        }
        var groups = new java.util.HashSet<Integer>();
        var tasks = new java.util.HashSet<Integer>();
        for (int offset = 0; offset < rows.size(); offset += 31)
            try (var batch = ArrowRowDataBatch.transpose(
                            rows.subList(offset, Math.min(rows.size(), offset + 31)), INPUT, allocator);
                    var envelope = ArrowExchangeBatch.withEnvelope(batch, INPUT)) {
                for (var frame : ArrowExchangeCDataBridge.route(
                        exchange(targets.size()), envelope.batch(), allocator, targets.get(0).memory)) {
                    int subtask = KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(
                            16, targets.size(), frame.keyGroup());
                    groups.add(frame.keyGroup());
                    tasks.add(subtask);
                    targets.get(subtask).processElement(0, new StreamRecord<>(frame));
                }
            }
        assertThat(tasks).hasSize(targets.size());
        assertThat(groups.size()).isGreaterThan(8);
        compare(flink, targets);
    }

    private static void watermark(
            KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> flink,
            List<KeyedNativeMetricHarness> targets,
            long watermark)
            throws Exception {
        flink.processWatermark(new Watermark(watermark));
        for (var target : targets) target.processWatermark(0, new Watermark(watermark));
        compare(flink, targets);
    }

    private static void compare(
            KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> flink,
            List<KeyedNativeMetricHarness> targets)
            throws Exception {
        var expected = new DataOutputSerializer(128);
        for (var event : flink.getOutput())
            if (event instanceof StreamRecord<?>) StageEventBytes.encode(OUTPUT, (StreamRecord<?>) event, expected);
        flink.getOutput().clear();
        var actual = new DataOutputSerializer(128);
        for (var target : targets) {
            actual.write(target.output.getCopyOfBuffer());
            target.output.clear();
            target.controls.clear();
        }
        assertThat(actual.getCopyOfBuffer()).containsExactly(expected.getCopyOfBuffer());
    }
}
