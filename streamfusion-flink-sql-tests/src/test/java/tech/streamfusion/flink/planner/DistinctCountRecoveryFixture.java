/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.types.RowKind;
import tech.streamfusion.flink.arrow.ArrowExchangeCDataBridge;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.exchange.ArrowExchangeBatch;
import tech.streamfusion.flink.exchange.NativeExchangePlanSerializer;
import tech.streamfusion.flink.operator.StreamFusionNativeRegionOperatorFactory;

/** Actual exchange frames and per-key ordered event bytes; never sort a key's changelog. */
final class DistinctCountRecoveryFixture {
    private DistinctCountRecoveryFixture() {}

    static KeyedNativeMetricHarness region(boolean rocks, OperatorSubtaskState state, int parallelism, int subtask)
            throws Exception {
        return region(rocks, state, parallelism, subtask, true);
    }

    static KeyedNativeMetricHarness region(
            boolean rocks, OperatorSubtaskState state, int parallelism, int subtask, boolean incremental)
            throws Exception {
        return region(rocks, state, parallelism, subtask, incremental, false);
    }

    static KeyedNativeMetricHarness region(
            boolean rocks,
            OperatorSubtaskState state,
            int parallelism,
            int subtask,
            boolean incremental,
            boolean appendOnly)
            throws Exception {
        var factory = new StreamFusionNativeRegionOperatorFactory(
                List.of(DistinctCountFlinkOracle.INPUT),
                DistinctCountFlinkOracle.OUTPUT,
                DistinctCountFixture.plan(appendOnly),
                List.of(3L),
                List.of(exchange(parallelism)));
        return new KeyedNativeMetricHarness(
                rocks, factory, 1, List.of(DistinctCountFlinkOracle.OUTPUT), state, parallelism, subtask, incremental);
    }

    private static byte[] exchange(int parallelism) {
        return NativeExchangePlanSerializer.hash(DistinctCountFlinkOracle.INPUT, new int[] {0}, 16, parallelism, true);
    }

    static List<RowData> appendRows(int seed, boolean selected) {
        var rows = new ArrayList<RowData>();
        var random = new Random(seed);
        for (int index = 0; index < 2048; index++) {
            rows.add(GenericRowData.of(
                    index % 7 == 0 ? null : StringData.fromString("é\u0000-" + random.nextInt(96)),
                    index % 11 == 0 ? null : (long) random.nextInt(257),
                    index % 13 == 0 ? null : selected && index % 3 != 0));
        }
        return rows;
    }

    static List<RowData> changes(List<GenericRowData> live, int phase) {
        var result = new ArrayList<RowData>();
        // Cancel the unmatched retraction stored before the checkpoint. This row is not a
        // live member; its signed contribution must disappear without incrementing DISTINCT.
        if (phase == 1) result.add(GenericRowData.of(null, Long.MIN_VALUE, true));
        var random = new Random(42 + phase);
        int count = phase == 0 ? 768 : phase == 1 ? 512 : live.size();
        for (int index = 0; index < count; index++) {
            GenericRowData row;
            if (phase == 2 || (phase == 1 && !live.isEmpty() && random.nextBoolean())) {
                row = live.remove(random.nextInt(live.size()));
                row.setRowKind(index % 2 == 0 ? RowKind.DELETE : RowKind.UPDATE_BEFORE);
            } else {
                int id = index % 96;
                row = GenericRowData.of(
                        id == 0 ? null : StringData.fromString("é\u0000-" + id),
                        id % 11 == 0 ? null : (long) (id % 3),
                        index % 13 == 0 ? null : index % 5 != 0);
                row.setRowKind(index % 2 == 0 ? RowKind.INSERT : RowKind.UPDATE_AFTER);
                live.add(GenericRowData.of(row.getField(0), row.getField(1), row.getField(2)));
            }
            result.add(row);
        }
        if (phase == 0) {
            var unmatched = GenericRowData.of(null, Long.MIN_VALUE, true);
            unmatched.setRowKind(RowKind.DELETE);
            result.add(unmatched);
        }
        return result;
    }

    static void input(
            List<KeyedNativeMetricHarness> targets,
            KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> oracle,
            RootAllocator allocator,
            List<RowData> rows,
            int phase)
            throws Exception {
        var groups = new HashSet<Integer>();
        var subtasks = new HashSet<Integer>();
        for (int start = 0; start < rows.size(); start += 64) {
            var arrival = rows.subList(start, Math.min(rows.size(), start + 64));
            var kinds = new RowKind[arrival.size()];
            var present = new boolean[arrival.size()];
            var timestamps = new long[arrival.size()];
            for (int index = 0; index < arrival.size(); index++) {
                var row = arrival.get(index);
                kinds[index] = row.getRowKind();
                present[index] = index % 3 != 0;
                timestamps[index] = phase * 10000L + start + index;
                oracle.processElement(
                        present[index] ? new StreamRecord<>(row, timestamps[index]) : new StreamRecord<>(row));
            }
            var expected = new DataOutputSerializer(128);
            for (var event : oracle.extractOutputStreamRecords())
                StageEventBytes.encode(DistinctCountFlinkOracle.OUTPUT, event, expected);
            oracle.getOutput().clear();
            try (var batch = ArrowRowDataBatch.transpose(arrival, DistinctCountFlinkOracle.INPUT, allocator)
                            .withEnvelope(kinds, present, timestamps);
                    var envelope = ArrowExchangeBatch.withEnvelope(batch, DistinctCountFlinkOracle.INPUT, null)) {
                for (var frame : ArrowExchangeCDataBridge.route(
                        exchange(targets.size()), envelope.batch(), allocator, targets.get(0).memory)) {
                    groups.add(frame.keyGroup());
                    int subtask = KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(
                            16, targets.size(), frame.keyGroup());
                    subtasks.add(subtask);
                    targets.get(subtask).processElement(0, new StreamRecord<>(frame));
                }
            }
            var actual = new HashMap<String, byte[]>();
            for (var target : targets) {
                for (var entry : keyed(target.output.getCopyOfBuffer()).entrySet()) {
                    assertThat(actual).doesNotContainKey(entry.getKey());
                    actual.put(entry.getKey(), entry.getValue());
                }
                assertThat(target.controls).isEmpty();
                target.output.clear();
            }
            var reference = keyed(expected.getCopyOfBuffer());
            assertThat(actual.keySet()).isEqualTo(reference.keySet());
            for (var entry : reference.entrySet())
                assertThat(actual.get(entry.getKey())).containsExactly(entry.getValue());
        }
        assertThat(subtasks).hasSize(targets.size());
        if (phase == 0) assertThat(groups).hasSize(16);
    }

    private static Map<String, byte[]> keyed(byte[] bytes) throws Exception {
        var result = new HashMap<String, DataOutputSerializer>();
        var input = new DataInputDeserializer(bytes);
        var serializer = new RowDataSerializer(DistinctCountFlinkOracle.OUTPUT);
        while (input.available() > 0) {
            int start = input.getPosition();
            assertThat(input.readUnsignedByte()).isZero();
            var row = serializer.deserialize(input);
            String key = row.isNullAt(0) ? null : row.getString(0).toString();
            if (input.readBoolean()) input.readLong();
            result.computeIfAbsent(key, ignored -> new DataOutputSerializer(128))
                    .write(Arrays.copyOfRange(bytes, start, input.getPosition()));
        }
        var encoded = new HashMap<String, byte[]>();
        result.forEach((key, value) -> encoded.put(key, value.getCopyOfBuffer()));
        return encoded;
    }
}
