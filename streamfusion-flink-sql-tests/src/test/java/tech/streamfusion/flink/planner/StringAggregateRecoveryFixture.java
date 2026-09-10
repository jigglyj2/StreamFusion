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
final class StringAggregateRecoveryFixture {
    private StringAggregateRecoveryFixture() {}

    static KeyedNativeMetricHarness region(
            boolean rocks, OperatorSubtaskState state, int parallelism, int subtask, boolean incremental)
            throws Exception {
        var factory = new StreamFusionNativeRegionOperatorFactory(
                List.of(StringAggregateFlinkOracle.INPUT),
                StringAggregateFlinkOracle.OUTPUT,
                StringAggregateFixture.plan(),
                List.of(3L),
                List.of(exchange(parallelism)));
        return new KeyedNativeMetricHarness(
                rocks,
                factory,
                1,
                List.of(StringAggregateFlinkOracle.OUTPUT),
                state,
                parallelism,
                subtask,
                incremental);
    }

    private static byte[] exchange(int parallelism) {
        return NativeExchangePlanSerializer.hash(
                StringAggregateFlinkOracle.INPUT, new int[] {0, 1}, 16, parallelism, true);
    }

    static List<GenericRowData> rows(int seed, int phase) {
        var rows = new ArrayList<GenericRowData>();
        var random = new Random(seed);
        String[] values = {"", "a", "a\u0000", "a ", "é", "e\u0301", "\uE000", "\uD800\uDC00", "z"};
        for (int index = 0; index < 2048; index++) {
            String key = index % 7 == 0 ? null : "é\u0000-" + random.nextInt(96);
            String part = index % 11 == 0 ? null : "part-" + index % 3;
            String value = index % 11 == 0 ? null : values[index % values.length];
            if (index % 47 == 0 && value != null) value = "\uDBFF\uDFFF" + "z".repeat(8192 + phase) + value;
            rows.add(GenericRowData.of(
                    key == null ? null : StringData.fromString(key),
                    part == null ? null : StringData.fromString(part),
                    value == null ? null : StringData.fromString(value),
                    index % 13 == 0 ? null : phase > 0 && index % 3 != 0,
                    index % 17 == 0 ? null : (long) random.nextInt(257)));
        }
        return rows;
    }

    static void input(
            List<KeyedNativeMetricHarness> targets,
            KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> oracle,
            RootAllocator allocator,
            List<? extends RowData> rows,
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
                var transported = StringAggregateFlinkOracle.binaryInput(row);
                oracle.processElement(
                        present[index]
                                ? new StreamRecord<>(transported, timestamps[index])
                                : new StreamRecord<>(transported));
            }
            var expected = new DataOutputSerializer(128);
            for (var event : oracle.extractOutputStreamRecords())
                StageEventBytes.encode(StringAggregateFlinkOracle.OUTPUT, event, expected);
            oracle.getOutput().clear();
            try (var batch = ArrowRowDataBatch.transpose(arrival, StringAggregateFlinkOracle.INPUT, allocator)
                            .withEnvelope(kinds, present, timestamps);
                    var envelope = ArrowExchangeBatch.withEnvelope(batch, StringAggregateFlinkOracle.INPUT, null)) {
                for (var frame : ArrowExchangeCDataBridge.route(
                        exchange(targets.size()), envelope.batch(), allocator, targets.get(0).memory)) {
                    groups.add(frame.keyGroup());
                    int subtask = KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(
                            16, targets.size(), frame.keyGroup());
                    subtasks.add(subtask);
                    targets.get(subtask).processElement(0, new StreamRecord<>(frame));
                }
            }
            var actual = new HashMap<List<String>, byte[]>();
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

    private static Map<List<String>, byte[]> keyed(byte[] bytes) throws Exception {
        var result = new HashMap<List<String>, DataOutputSerializer>();
        var input = new DataInputDeserializer(bytes);
        var serializer = new RowDataSerializer(StringAggregateFlinkOracle.OUTPUT);
        while (input.available() > 0) {
            int start = input.getPosition();
            assertThat(input.readUnsignedByte()).isZero();
            var row = serializer.deserialize(input);
            var key = Arrays.asList(
                    row.isNullAt(0) ? null : row.getString(0).toString(),
                    row.isNullAt(1) ? null : row.getString(1).toString());
            if (input.readBoolean()) input.readLong();
            result.computeIfAbsent(key, ignored -> new DataOutputSerializer(128))
                    .write(Arrays.copyOfRange(bytes, start, input.getPosition()));
        }
        var encoded = new HashMap<List<String>, byte[]>();
        result.forEach((key, value) -> encoded.put(key, value.getCopyOfBuffer()));
        return encoded;
    }
}
