/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;
import static tech.streamfusion.flink.planner.SharedProcessingWindowFixture.*;
import static tech.streamfusion.flink.planner.SharedWindowRuntimeRecoveryTest.snapshot;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.SharedStateRegistryImpl;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.flink.arrow.ArrowExchangeCDataBridge;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.exchange.ArrowExchangeBatch;
import tech.streamfusion.flink.exchange.NativeExchangePlanSerializer;

/** Pending processing-time namespaces through canonical, aligned and unaligned keyed recovery. */
class SharedProcessingWindowRecoveryTest {
    @ParameterizedTest(name = "rocks={0}, mode={1}")
    @CsvSource({"false,0", "true,0", "false,1", "true,1", "false,2", "true,2"})
    void restoresAbsoluteTimersAndRescalesOneToTwoToOne(boolean rocks, int mode) throws Exception {
        for (int seed : List.of(3, 19, 71)) {
            var states = new ArrayList<OperatorSubtaskState>();
            var registry = new SharedStateRegistryImpl();
            try (var allocator = new RootAllocator(64L << 20)) {
                OperatorSubtaskState initial;
                OperatorSubtaskState reference;
                try (var flink = oracle(rocks, null);
                        var source = region(rocks, null, 1, 0)) {
                    time(flink, List.of(source), 1);
                    rows(flink, List.of(source), allocator, List.of(GenericRowData.of(7L, null)));
                    time(flink, List.of(source), 9999);
                    time(flink, List.of(source), 10001);
                    input(flink, List.of(source), allocator, seed, 0);
                    flink.processWatermark(Watermark.MAX_WATERMARK);
                    source.processWatermark(0, Watermark.MAX_WATERMARK);
                    compare(flink, List.of(source));
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
                    flink.prepareSnapshotPreBarrier(2);
                    reference = flink.snapshot(2, 2);
                    // The task harness has no coordinator to resolve reused RocksDB SST handles.
                    reference.registerSharedStates(registry, 2);
                    states.add(reference);
                }
                var combined = AbstractStreamOperatorTestHarness.repackageState(initial);
                var zeroState = AbstractStreamOperatorTestHarness.repartitionOperatorState(combined, 16, 1, 2, 0);
                var oneState = AbstractStreamOperatorTestHarness.repartitionOperatorState(combined, 16, 1, 2, 1);
                boolean scaledRocks = mode == 0 ? !rocks : rocks;
                try (var flink = oracle(rocks, reference);
                        var zero = region(scaledRocks, zeroState, 2, 0);
                        var one = region(scaledRocks, oneState, 2, 1)) {
                    var targets = List.of(zero, one);
                    // Flink resets processing buffer progress on restore, independently of
                    // the MAX union watermark and the earlier pre-checkpoint timer callback.
                    time(flink, targets, 1);
                    rows(flink, targets, allocator, List.of(GenericRowData.of(null, null)));
                    time(flink, targets, 9999);
                    time(flink, targets, 10002);
                    input(flink, targets, allocator, seed, 1);
                    var zeroCheckpoint = snapshot(zero, mode, 3);
                    var oneCheckpoint = snapshot(one, mode, 3);
                    states.add(zeroCheckpoint);
                    states.add(oneCheckpoint);
                    combined = AbstractStreamOperatorTestHarness.repackageState(zeroCheckpoint, oneCheckpoint);
                    flink.prepareSnapshotPreBarrier(3);
                    reference = flink.snapshot(3, 3);
                    reference.registerSharedStates(registry, 3);
                    states.add(reference);
                }
                var back = AbstractStreamOperatorTestHarness.repartitionOperatorState(combined, 16, 2, 1, 0);
                try (var flink = oracle(rocks, reference);
                        var target = region(rocks, back, 1, 0)) {
                    time(flink, List.of(target), 10003);
                    input(flink, List.of(target), allocator, seed, 2);
                    time(flink, List.of(target), 19999);
                    time(flink, List.of(target), 20001);
                    rows(flink, List.of(target), allocator, List.of(GenericRowData.of(7L, null)));
                    flink.getOperator().finish();
                    target.region().finish();
                    compare(flink, List.of(target));
                }
                assertThat(allocator.getAllocatedMemory()).isZero();
            } finally {
                for (var state : states) state.discardState();
                registry.unregisterUnusedState(Long.MAX_VALUE);
                registry.close();
            }
        }
    }

    private static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> oracle(
            boolean rocks, OperatorSubtaskState state) throws Exception {
        return ProcessingTimeWindowClockTest.oracle(rocks, 10000, state);
    }

    private static KeyedNativeMetricHarness region(
            boolean rocks, OperatorSubtaskState state, int parallelism, int subtask) throws Exception {
        return new KeyedNativeMetricHarness(
                rocks, factory(10000, exchange(parallelism), 1, 1), 1, OUTPUT, state, parallelism, subtask);
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
        var random = new Random(seed * 17L + phase);
        var rows = new ArrayList<RowData>();
        for (int key = 0; key < 65; key++)
            for (int repeat = 0, count = 1 + random.nextInt(7); repeat < count; repeat++)
                rows.add(GenericRowData.of(key == 64 ? null : (long) key, null));
        rows(flink, targets, allocator, rows);
    }

    private static void rows(
            KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> flink,
            List<KeyedNativeMetricHarness> targets,
            RootAllocator allocator,
            List<RowData> rows)
            throws Exception {
        var serializer = new RowDataSerializer(INPUT);
        for (var row : rows) flink.processElement(new StreamRecord<>(serializer.toBinaryRow(row), 123));
        var groups = new java.util.HashSet<Integer>();
        var subtasks = new java.util.HashSet<Integer>();
        for (int offset = 0; offset < rows.size(); offset += 31)
            try (var batch = ArrowRowDataBatch.transpose(
                            rows.subList(offset, Math.min(rows.size(), offset + 31)), INPUT, allocator);
                    var envelope = ArrowExchangeBatch.withEnvelope(batch, INPUT)) {
                for (var frame : ArrowExchangeCDataBridge.route(
                        exchange(targets.size()), envelope.batch(), allocator, targets.get(0).memory)) {
                    int subtask = KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(
                            16, targets.size(), frame.keyGroup());
                    groups.add(frame.keyGroup());
                    subtasks.add(subtask);
                    targets.get(subtask).processElement(0, new StreamRecord<>(frame));
                }
            }
        if (rows.size() > 65) {
            assertThat(subtasks).hasSize(targets.size());
            assertThat(groups.size()).isGreaterThan(8);
        }
        compare(flink, targets);
    }

    private static void time(
            KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> flink,
            List<KeyedNativeMetricHarness> targets,
            long time)
            throws Exception {
        flink.setProcessingTime(time);
        for (var target : targets) target.setProcessingTime(time);
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
        // TimerHeapInternalTimer compares only the deadline. Canonicalize independent
        // keys tied at that deadline, retaining every changelog byte and envelope.
        assertThat(WindowTimerEventBytes.canonical(OUTPUT, 3, actual.getCopyOfBuffer()))
                .containsExactly(WindowTimerEventBytes.canonical(OUTPUT, 3, expected.getCopyOfBuffer()));
    }
}
