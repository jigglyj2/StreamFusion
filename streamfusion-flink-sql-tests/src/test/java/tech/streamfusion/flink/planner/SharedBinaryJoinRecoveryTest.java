/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;
import static tech.streamfusion.flink.planner.SharedBinaryJoinMetricFixture.*;

import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFinalizer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.flink.arrow.ArrowExchangeCDataBridge;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.exchange.NativeExchangePlanSerializer;
import tech.streamfusion.flink.operator.StreamFusionNativeRegionOperatorFactory;

/** Actual Rust routing, shared keyed region, and Flink snapshot/repartition/restore APIs. */
class SharedBinaryJoinRecoveryTest {
    @ParameterizedTest(name = "rocks={0}, mode={1}")
    @CsvSource({"false,0", "true,0", "false,1", "true,1", "false,2", "true,2"})
    void canonicalBackendSwitchAndAlignedUnalignedRescalingPreserveJoinChangelog(boolean rocks, int mode)
            throws Exception {
        var fixture = new SharedBinaryJoinMetricFixture(false);
        try (var oracle = fixture.join(rocks);
                var calc = fixture.calc();
                var allocator = new RootAllocator(64L << 20)) {
            OperatorSubtaskState initial;
            try (var source = region(fixture, rocks, null, 1, 0)) {
                compare(List.of(source), oracle, calc, allocator, 0, RowKind.INSERT);
                compare(List.of(source), oracle, calc, allocator, 1, RowKind.INSERT);
                initial = snapshot(source, mode, 1);
                if (rocks && mode != 0) {
                    var first = (IncrementalRemoteKeyedStateHandle)
                            initial.getManagedKeyedState().iterator().next();
                    source.notifyOfCompletedCheckpoint(1);
                    var nextState = snapshot(source, mode, 2);
                    var next = (IncrementalRemoteKeyedStateHandle)
                            nextState.getManagedKeyedState().iterator().next();
                    assertThat(next.getCheckpointedSize()).isLessThan(first.getCheckpointedSize());
                    initial = nextState;
                } else assertThat(initial.getRawKeyedState()).hasSize(1);
            }
            var state = AbstractStreamOperatorTestHarness.repackageState(initial);
            var assigned0 = AbstractStreamOperatorTestHarness.repartitionOperatorState(state, 16, 1, 2, 0);
            var assigned1 = AbstractStreamOperatorTestHarness.repartitionOperatorState(state, 16, 1, 2, 1);
            OperatorSubtaskState combined;
            boolean scaledRocks = mode == 0 ? !rocks : rocks;
            try (var first = region(fixture, scaledRocks, assigned0, 2, 0);
                    var second = region(fixture, scaledRocks, assigned1, 2, 1)) {
                compare(List.of(first, second), oracle, calc, allocator, 0, RowKind.DELETE);
                compare(List.of(first, second), oracle, calc, allocator, 0, RowKind.UPDATE_AFTER);
                combined = AbstractStreamOperatorTestHarness.repackageState(
                        snapshot(first, mode, 3), snapshot(second, mode, 3));
            }
            var assignedBack = AbstractStreamOperatorTestHarness.repartitionOperatorState(combined, 16, 2, 1, 0);
            try (var target = region(fixture, rocks, assignedBack, 1, 0)) {
                compare(List.of(target), oracle, calc, allocator, 1, RowKind.UPDATE_BEFORE);
            }
            assertThat(allocator.getAllocatedMemory()).isZero();
        }
    }

    private static KeyedNativeMetricHarness region(
            SharedBinaryJoinMetricFixture fixture,
            boolean rocks,
            OperatorSubtaskState state,
            int parallelism,
            int subtask)
            throws Exception {
        byte[] exchange = exchange(parallelism);
        var factory = new StreamFusionNativeRegionOperatorFactory(
                List.of(INPUT, INPUT), OUTPUT, fixture.plan(), List.of(id(0)), List.of(exchange, exchange));
        return new KeyedNativeMetricHarness(rocks, factory, 2, OUTPUT, state, parallelism, subtask);
    }

    private static byte[] exchange(int parallelism) {
        return NativeExchangePlanSerializer.hash(INPUT, new int[] {0}, 16, parallelism, true);
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

    private static void compare(
            List<KeyedNativeMetricHarness> targets,
            FlinkMultiInputMetricOracle oracle,
            FlinkStageMetricOracle calc,
            RootAllocator allocator,
            int port,
            RowKind kind)
            throws Exception {
        var seenGroups = new java.util.HashSet<Integer>();
        var seenTasks = new java.util.HashSet<Integer>();
        for (int key = 0; key < 64; key++) {
            var value =
                    GenericRowData.of((long) key, key % 7 == 0 ? null : StringData.fromString("é-" + port + "-" + key));
            value.setRowKind(kind);
            oracle.accept(
                    port,
                    new StreamRecord<>(
                            new RowDataSerializer(INPUT).toBinaryRow(value).copy()));
            for (var event : oracle.drain()) calc.accept(event);
            var expected = new DataOutputSerializer(128);
            for (var event : calc.drain()) StageEventBytes.encode(OUTPUT, event, expected);
            try (var batch = ArrowRowDataBatch.transpose(List.of(value), INPUT, allocator)
                            .withRowKinds(new RowKind[] {kind});
                    var envelope =
                            tech.streamfusion.flink.exchange.ArrowExchangeBatch.withEnvelope(batch, INPUT, null)) {
                for (var frame : ArrowExchangeCDataBridge.route(
                        exchange(targets.size()), envelope.batch(), allocator, targets.get(0).memory)) {
                    int subtask = KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(
                            16, targets.size(), frame.keyGroup());
                    seenGroups.add(frame.keyGroup());
                    seenTasks.add(subtask);
                    targets.get(subtask).processElement(port, new StreamRecord<>(frame));
                }
            }
            var actual = new DataOutputSerializer(128);
            for (var target : targets) {
                actual.write(target.output.getCopyOfBuffer());
                target.output.clear();
            }
            assertThat(actual.getCopyOfBuffer())
                    .as("key=%s port=%s kind=%s", key, port, kind)
                    .containsExactly(expected.getCopyOfBuffer());
        }
        assertThat(seenTasks).hasSize(targets.size());
        assertThat(seenGroups.size()).isGreaterThan(8);
    }
}
