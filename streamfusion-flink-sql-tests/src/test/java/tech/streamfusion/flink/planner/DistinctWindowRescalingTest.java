/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.flink.arrow.ArrowExchangeCDataBridge;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.exchange.ArrowExchangeBatch;
import tech.streamfusion.flink.exchange.NativeExchangePlanSerializer;
import tech.streamfusion.flink.operator.StreamFusionNativeRegionOperatorFactory;

/** Live DISTINCT keys and timer namespaces move 1 -> 2 -> 1 through real key-group routing. */
class DistinctWindowRescalingTest {
    @ParameterizedTest
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    void liveCompositePresenceSurvivesRepartitionAndBackendSwitch(boolean strings, boolean rocks) throws Exception {
        var fixture = new DistinctWindowFixture(strings);
        for (int mode = 0; mode < 3; mode++) {
            try (var flink = fixture.oracle(rocks, null);
                    var allocator = new RootAllocator(64L << 20)) {
                OperatorSubtaskState first;
                try (var source = region(fixture, rocks, null, 1, 0)) {
                    input(fixture, List.of(source), flink, allocator, 0, 4000);
                    input(fixture, List.of(source), flink, allocator, 1, 6000);
                    DistinctWindowRecoveryTest.watermark(fixture, flink, source, 1999);
                    first = SharedWindowRuntimeRecoveryTest.snapshot(source, mode, 100);
                }
                var assigned0 = AbstractStreamOperatorTestHarness.repartitionOperatorState(first, 16, 1, 2, 0);
                var assigned1 = AbstractStreamOperatorTestHarness.repartitionOperatorState(first, 16, 1, 2, 1);
                OperatorSubtaskState second;
                var scaledRocks = mode == 0 ? !rocks : rocks;
                try (var left = region(fixture, scaledRocks, assigned0, 2, 0);
                        var right = region(fixture, scaledRocks, assigned1, 2, 1)) {
                    input(fixture, List.of(left, right), flink, allocator, 2, 4000);
                    input(fixture, List.of(left, right), flink, allocator, 3, 8000);
                    second = AbstractStreamOperatorTestHarness.repackageState(
                            SharedWindowRuntimeRecoveryTest.snapshot(left, mode, 101),
                            SharedWindowRuntimeRecoveryTest.snapshot(right, mode, 101));
                }
                var assigned = AbstractStreamOperatorTestHarness.repartitionOperatorState(second, 16, 2, 1, 0);
                try (var target = region(fixture, rocks, assigned, 1, 0)) {
                    input(fixture, List.of(target), flink, allocator, 4, 6000);
                    for (long mark : new long[] {999, 3999, 5999, 7999, Long.MAX_VALUE})
                        DistinctWindowRecoveryTest.watermark(fixture, flink, target, mark);
                } finally {
                    first.discardState();
                    second.discardState();
                }
            }
        }
    }

    private static byte[] exchange(DistinctWindowFixture fixture, int parallelism) {
        return NativeExchangePlanSerializer.hash(
                fixture.input, fixture.strings ? new int[] {0, 1} : new int[] {0}, 16, parallelism, true);
    }

    private static KeyedNativeMetricHarness region(
            DistinctWindowFixture fixture, boolean rocks, OperatorSubtaskState state, int parallelism, int subtask)
            throws Exception {
        var factory = new StreamFusionNativeRegionOperatorFactory(
                List.of(fixture.input),
                fixture.output,
                fixture.plan(),
                List.of(3L),
                List.of(exchange(fixture, parallelism)));
        return new KeyedNativeMetricHarness(rocks, factory, 1, fixture.output, state, parallelism, subtask);
    }

    private static void input(
            DistinctWindowFixture fixture,
            List<KeyedNativeMetricHarness> targets,
            KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> flink,
            RootAllocator allocator,
            int phase,
            long end)
            throws Exception {
        var rows = new ArrayList<RowData>();
        var serializer = new RowDataSerializer(fixture.flinkInput);
        for (int i = 0; i < 128; i++) {
            int id = (phase * 7 + i) % 128;
            flink.processElement(new StreamRecord<>(serializer.toBinaryRow(fixture.row(id, end, false)), 123));
            rows.add(fixture.row(id, end, true));
        }
        var subtasks = new HashSet<Integer>();
        var groups = new HashSet<Integer>();
        try (var batch = ArrowRowDataBatch.transpose(rows, fixture.input, allocator);
                var envelope = ArrowExchangeBatch.withEnvelope(batch, fixture.input, null)) {
            for (var frame : ArrowExchangeCDataBridge.route(
                    exchange(fixture, targets.size()), envelope.batch(), allocator, targets.get(0).memory)) {
                int subtask =
                        KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(16, targets.size(), frame.keyGroup());
                subtasks.add(subtask);
                groups.add(frame.keyGroup());
                targets.get(subtask).processElement(0, new StreamRecord<>(frame));
            }
        }
        assertThat(subtasks).hasSize(targets.size());
        assertThat(groups.size()).isGreaterThan(1);
        // These live windows have not fired. Emit only after all keys return to one subtask,
        // so the result comparison preserves the window-end and control-event order.
        assertThat(flink.getOutput()).isEmpty();
        for (var target : targets) {
            assertThat(target.output.length()).isZero();
            assertThat(target.controls).isEmpty();
        }
    }
}
