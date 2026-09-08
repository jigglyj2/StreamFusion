/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.arrow.ArrowExchangeCDataBridge;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.exchange.ArrowExchangeBatch;

/** Real Rust exchange routing and Flink key-group repartitioning around the shared native tree. */
class SharedAggregateRescalingTest {
    @Test
    void oneToTwoToOnePreservesGeneratedSqlChangelogsOnBothBackendsAndCheckpointModes() throws Exception {
        for (boolean rocks : List.of(false, true))
            for (int mode = 0; mode < 3; mode++) {
                var live = new ArrayList<GenericRowData>();
                try (var oracle = SharedAggregateFlinkOracle.create();
                        var allocator = new RootAllocator(64L << 20)) {
                    OperatorSubtaskState initial;
                    try (var source = new SharedAggregateRuntimeHarness(rocks, null, 1, 0, true)) {
                        compare(List.of(source), oracle, allocator, changes(live, 0), 0);
                        initial = SharedAggregateCheckpointTest.snapshot(source, mode, 100);
                    }
                    var packaged = AbstractStreamOperatorTestHarness.repackageState(initial);
                    var assigned0 = AbstractStreamOperatorTestHarness.repartitionOperatorState(packaged, 16, 1, 2, 0);
                    var assigned1 = AbstractStreamOperatorTestHarness.repartitionOperatorState(packaged, 16, 1, 2, 1);
                    boolean scaledRocks = mode == 0 ? !rocks : rocks;
                    OperatorSubtaskState scaled;
                    try (var left = new SharedAggregateRuntimeHarness(scaledRocks, assigned0, 2, 0, true);
                            var right = new SharedAggregateRuntimeHarness(scaledRocks, assigned1, 2, 1, true)) {
                        compare(List.of(left, right), oracle, allocator, changes(live, 1), 1);
                        scaled = AbstractStreamOperatorTestHarness.repackageState(
                                SharedAggregateCheckpointTest.snapshot(left, mode, 101),
                                SharedAggregateCheckpointTest.snapshot(right, mode, 101));
                    }
                    var assignedBack = AbstractStreamOperatorTestHarness.repartitionOperatorState(scaled, 16, 2, 1, 0);
                    try (var target = new SharedAggregateRuntimeHarness(rocks, assignedBack, 1, 0, true)) {
                        compare(List.of(target), oracle, allocator, changes(live, 2), 2);
                    }
                    assertThat(live).isEmpty();
                    assertThat(allocator.getAllocatedMemory()).isZero();
                }
            }
    }

    private static List<RowData> changes(List<GenericRowData> live, int phase) {
        var result = new ArrayList<RowData>();
        var random = new Random(42 + phase);
        int count = phase == 0 ? 384 : phase == 1 ? 256 : live.size();
        for (int i = 0; i < count; i++) {
            GenericRowData row;
            if (phase == 2 || (phase == 1 && !live.isEmpty() && random.nextBoolean())) {
                row = live.remove(random.nextInt(live.size()));
                row.setRowKind(i % 2 == 0 ? RowKind.DELETE : RowKind.UPDATE_BEFORE);
            } else {
                row = GenericRowData.of(
                        i % 17 == 0 ? null : StringData.fromString("é-" + (i % 96)),
                        SharedAggregateRegionParityTest.generatedValue(random, i));
                row.setRowKind(i % 2 == 0 ? RowKind.INSERT : RowKind.UPDATE_AFTER);
                live.add(GenericRowData.of(row.getField(0), row.getField(1)));
            }
            result.add(row);
        }
        return result;
    }

    private static void compare(
            List<SharedAggregateRuntimeHarness> targets,
            KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> oracle,
            RootAllocator allocator,
            List<RowData> rows,
            int phase)
            throws Exception {
        var keyGroups = new HashSet<Integer>();
        var subtasks = new HashSet<Integer>();
        for (int start = 0; start < rows.size(); start += 64) {
            var arrival = rows.subList(start, Math.min(rows.size(), start + 64));
            var kinds = new RowKind[arrival.size()];
            var present = new boolean[arrival.size()];
            var timestamps = new long[arrival.size()];
            for (int i = 0; i < arrival.size(); i++) {
                var row = arrival.get(i);
                kinds[i] = row.getRowKind();
                present[i] = i % 3 != 0;
                timestamps[i] = phase * 10000L + start + i;
                oracle.processElement(present[i] ? new StreamRecord<>(row, timestamps[i]) : new StreamRecord<>(row));
            }
            var expected = new HashMap<String, List<String>>();
            for (var output : oracle.extractOutputStreamRecords())
                SharedAggregateRuntimeHarness.record(
                        expected, output.getValue(), output.hasTimestamp() ? output.getTimestamp() : null);
            oracle.getOutput().clear();
            try (var batch = ArrowRowDataBatch.transpose(arrival, SharedAggregateFlinkOracle.INPUT, allocator)
                            .withEnvelope(kinds, present, timestamps);
                    var envelope = ArrowExchangeBatch.withEnvelope(batch, SharedAggregateFlinkOracle.INPUT, null)) {
                var frames = ArrowExchangeCDataBridge.route(
                        SharedAggregateRuntimeHarness.exchangePlan(targets.size()),
                        envelope.batch(),
                        allocator,
                        targets.get(0).nativeMemory);
                for (var frame : frames) {
                    keyGroups.add(frame.keyGroup());
                    int subtask = KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(
                            16, targets.size(), frame.keyGroup());
                    subtasks.add(subtask);
                    targets.get(subtask).processElement(0, new StreamRecord<>(frame));
                }
            }
            var actual = new HashMap<String, List<String>>();
            for (var target : targets) {
                for (var entry : target.changelog.entrySet()) {
                    assertThat(actual).doesNotContainKey(entry.getKey()); // A SQL key belongs to exactly one subtask.
                    actual.put(entry.getKey(), entry.getValue());
                }
                target.changelog.clear();
                target.captured.clear();
                target.times.clear();
            }
            assertThat(actual).containsExactlyInAnyOrderEntriesOf(expected);
        }
        assertThat(subtasks).hasSize(targets.size());
        if (phase == 0) assertThat(keyGroups).hasSize(16);
    }
}
