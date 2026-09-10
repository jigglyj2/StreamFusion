/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;
import static tech.streamfusion.flink.planner.SharedAppendTopNFixture.id;
import static tech.streamfusion.flink.planner.SharedWindowRuntimeRecoveryTest.snapshot;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.flink.arrow.ArrowExchangeCDataBridge;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.exchange.NativeExchangePlanSerializer;
import tech.streamfusion.flink.operator.StreamFusionNativeRegionOperatorFactory;

/** Generated changelog against uninterrupted Flink across actual Flink snapshot/repartition APIs. */
class SharedAppendTopNRecoveryTest {
    @ParameterizedTest(name = "rocks={0}, mode={1}, ascending={2}")
    @CsvSource({
        "false,0,false",
        "true,0,false",
        "false,1,false",
        "true,1,false",
        "false,2,false",
        "true,2,false",
        "false,0,true",
        "true,0,true",
        "false,1,true",
        "true,1,true",
        "false,2,true",
        "true,2,true"
    })
    void checkpointsBackendSwitchAndRescalingPreserveEveryAppendTopNTransition(
            boolean rocks, int mode, boolean ascending) throws Exception {
        for (int seed : List.of(3, 19, 71)) {
            var fixture = new SharedAppendTopNFixture(ascending, true, true);
            try (var first = fixture.oracle(0, rocks);
                    var top = fixture.oracle(1, rocks);
                    var tail = fixture.oracle(2, rocks);
                    var allocator = new RootAllocator(64L << 20)) {
                var oracles = List.of(first, top, tail);
                OperatorSubtaskState initial;
                try (var source = region(fixture, rocks, null, 1, 0)) {
                    compare(fixture, List.of(source), oracles, allocator, seed, 0);
                    compare(fixture, List.of(source), oracles, allocator, seed, 1);
                    initial = snapshot(source, mode, 1);
                    if (rocks && mode != 0) {
                        var base = (IncrementalRemoteKeyedStateHandle)
                                initial.getManagedKeyedState().iterator().next();
                        source.notifyOfCompletedCheckpoint(1);
                        var next = snapshot(source, mode, 2);
                        var incremental = (IncrementalRemoteKeyedStateHandle)
                                next.getManagedKeyedState().iterator().next();
                        assertThat(incremental.getCheckpointedSize()).isLessThan(base.getCheckpointedSize());
                        initial = next;
                    } else assertThat(initial.getRawKeyedState()).hasSize(1);
                }
                var state = AbstractStreamOperatorTestHarness.repackageState(initial);
                var assigned0 = AbstractStreamOperatorTestHarness.repartitionOperatorState(state, 16, 1, 2, 0);
                var assigned1 = AbstractStreamOperatorTestHarness.repartitionOperatorState(state, 16, 1, 2, 1);
                OperatorSubtaskState combined;
                boolean scaledRocks = mode == 0 ? !rocks : rocks;
                try (var zero = region(fixture, scaledRocks, assigned0, 2, 0);
                        var one = region(fixture, scaledRocks, assigned1, 2, 1)) {
                    compare(fixture, List.of(zero, one), oracles, allocator, seed, 2);
                    combined = AbstractStreamOperatorTestHarness.repackageState(
                            snapshot(zero, mode, 3), snapshot(one, mode, 3));
                }
                var back = AbstractStreamOperatorTestHarness.repartitionOperatorState(combined, 16, 2, 1, 0);
                try (var target = region(fixture, rocks, back, 1, 0)) {
                    compare(fixture, List.of(target), oracles, allocator, seed, 3);
                }
                assertThat(allocator.getAllocatedMemory()).isZero();
            }
        }
    }

    private static KeyedNativeMetricHarness region(
            SharedAppendTopNFixture fixture, boolean rocks, OperatorSubtaskState state, int parallelism, int subtask)
            throws Exception {
        var factory = new StreamFusionNativeRegionOperatorFactory(
                List.of(fixture.top.input),
                fixture.top.output,
                fixture.plan(),
                List.of(id(1)),
                List.of(exchange(fixture, parallelism)));
        return new KeyedNativeMetricHarness(rocks, factory, 1, fixture.top.output, state, parallelism, subtask);
    }

    private static byte[] exchange(SharedAppendTopNFixture fixture, int parallelism) {
        return NativeExchangePlanSerializer.hash(fixture.top.input, new int[] {0}, 16, parallelism, true);
    }

    private static void compare(
            SharedAppendTopNFixture fixture,
            List<KeyedNativeMetricHarness> targets,
            List<FlinkStageMetricOracle> oracles,
            RootAllocator allocator,
            int seed,
            int phase)
            throws Exception {
        var random = new Random(seed * 17L + phase);
        var groups = new java.util.HashSet<Integer>();
        var tasks = new java.util.HashSet<Integer>();
        var serializer = new RowDataSerializer(fixture.top.input);
        for (int key = 0; key < 65; key++) {
            var rows = new ArrayList<GenericRowData>();
            boolean[] present = {true, false, true, true, false, true, true, false};
            long[] timestamps = {Long.MIN_VALUE, 0, phase == 3 ? Long.MAX_VALUE : phase * 1000L + key, 1, 0, -1, 7, 0};
            var expected = new DataOutputSerializer(128);
            for (int arrival = 0; arrival < present.length; arrival++) {
                var row = GenericRowData.of(
                        key == 64 ? null : (long) key,
                        random.nextInt(5) == 0 ? null : (long) random.nextInt(17) - 2,
                        random.nextBoolean() ? null : TimestampData.fromEpochMillis(random.nextInt(4)),
                        StringData.fromString("é-" + seed + "-" + phase + "-" + key + "-" + arrival));
                rows.add(row);
                var copy = serializer.toBinaryRow(row).copy();
                List<StreamElement> events = List.of(
                        present[arrival] ? new StreamRecord<>(copy, timestamps[arrival]) : new StreamRecord<>(copy));
                for (var oracle : oracles) {
                    for (var event : events) oracle.accept(event);
                    events = oracle.drain();
                }
                for (var event : events) StageEventBytes.encode(fixture.top.output, event, expected);
            }
            try (var batch = ArrowRowDataBatch.transpose(rows, fixture.top.input, allocator)
                            .withEnvelope(
                                    java.util.Collections.nCopies(present.length, RowKind.INSERT)
                                            .toArray(new RowKind[0]),
                                    present,
                                    timestamps);
                    var envelope = tech.streamfusion.flink.exchange.ArrowExchangeBatch.withEnvelope(
                            batch, fixture.top.input)) {
                for (var frame : ArrowExchangeCDataBridge.route(
                        exchange(fixture, targets.size()), envelope.batch(), allocator, targets.get(0).memory)) {
                    int subtask = KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(
                            16, targets.size(), frame.keyGroup());
                    groups.add(frame.keyGroup());
                    tasks.add(subtask);
                    targets.get(subtask).processElement(0, new StreamRecord<>(frame));
                }
            }
            var actual = new DataOutputSerializer(128);
            for (var target : targets) {
                actual.write(target.output.getCopyOfBuffer());
                target.output.clear();
            }
            assertThat(actual.getCopyOfBuffer())
                    .as("seed=%s phase=%s key=%s", seed, phase, key)
                    .containsExactly(expected.getCopyOfBuffer());
        }
        assertThat(tasks).hasSize(targets.size());
        assertThat(groups.size()).isGreaterThan(8);
    }
}
