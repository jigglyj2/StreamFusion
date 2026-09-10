/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.operator;

import static org.assertj.core.api.Assertions.assertThat;
import static tech.streamfusion.flink.operator.WindowJoinRegionFixture.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.window.WindowJoinOperatorBuilder;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;

class WindowJoinRegionParityTest {
    @Test
    void restoresTimerQueuesButResetsWatermarkLikeFlinkIncludingEmptySnapshots() throws Exception {
        for (boolean rocks : List.of(false, true)) {
            for (int mode = 0; mode < 3; mode++) {
                for (boolean pending : List.of(false, true)) {
                    OperatorSubtaskState nativeSnapshot;
                    OperatorSubtaskState flinkSnapshot;
                    try (var nativeSource = new WindowJoinRegionFixture(rocks, null);
                            var flinkSource = flink(rocks, null);
                            var allocator = new RootAllocator(64L << 20)) {
                        if (pending) generated(nativeSource, flinkSource, allocator, 0, 2000);
                        watermark(nativeSource, flinkSource, 999);
                        compare(nativeSource, flinkSource);
                        nativeSnapshot = nativeSource.checkpoint(mode);
                        flinkSnapshot = flinkSource.snapshot(1, 1);
                    }
                    try (var target = new WindowJoinRegionFixture(mode == 0 ? !rocks : rocks, nativeSnapshot);
                            var oracle = flink(rocks, flinkSnapshot);
                            var allocator = new RootAllocator(64L << 20)) {
                        // Flink permits these previously-late windows before the first replayed watermark.
                        // Persisting a union clock for WindowJoin would silently drop these rows.
                        generated(target, oracle, allocator, 1, 100);
                        watermark(target, oracle, 99);
                        assertThat(oracle.extractOutputStreamRecords()).isNotEmpty();
                        compare(target, oracle);
                        generated(target, oracle, allocator, 2, 2000);
                        watermark(target, oracle, 1999);
                        compare(target, oracle);
                    }
                }
            }
        }
    }

    @Test
    void generatedDuplicatesNullKeysRowKindsAndLateRowsMatchFlinkAtEveryWatermark() throws Exception {
        for (boolean rocks : List.of(false, true)) {
            try (var target = new WindowJoinRegionFixture(rocks, null);
                    var oracle = flink(rocks, null);
                    var allocator = new RootAllocator(64L << 20)) {
                for (int seed = 0; seed < 5; seed++) {
                    long end = 100L * (seed + 1);
                    generated(target, oracle, allocator, seed, end);
                    compare(target, oracle);
                    watermark(target, oracle, end - 1);
                    compare(target, oracle);
                    // Late retractions must drop before Flink checks their changelog kind.
                    send(
                            target,
                            oracle,
                            allocator,
                            0,
                            List.of(GenericRowData.ofKind(RowKind.DELETE, 9L, end, StringData.fromString("late"))));
                    send(
                            target,
                            oracle,
                            allocator,
                            1,
                            List.of(GenericRowData.ofKind(RowKind.UPDATE_BEFORE, 9L, end, null)));
                    compare(target, oracle);
                }
            }
        }
    }

    private static void generated(
            WindowJoinRegionFixture target,
            KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> oracle,
            RootAllocator allocator,
            int seed,
            long end)
            throws Exception {
        var random = new Random(seed);
        for (int side = 0; side < 2; side++) {
            List<RowData> rows = new ArrayList<>();
            for (int row = 0; row < 12; row++)
                rows.add(GenericRowData.ofKind(
                        row % 3 == 0 ? RowKind.UPDATE_AFTER : RowKind.INSERT,
                        row % 5 == 0 ? null : 9L,
                        end,
                        row % 4 == 0 ? null : StringData.fromString("é-" + random.nextInt(4))));
            send(target, oracle, allocator, side, rows);
        }
    }

    private static void send(
            WindowJoinRegionFixture target,
            KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> oracle,
            RootAllocator allocator,
            int side,
            List<RowData> rows)
            throws Exception {
        var serializer = new RowDataSerializer(INPUT);
        var kinds = new RowKind[rows.size()];
        var times = new long[rows.size()];
        var present = new boolean[rows.size()];
        for (int index = 0; index < rows.size(); index++) {
            kinds[index] = rows.get(index).getRowKind();
            times[index] = 777;
            present[index] = true;
            var record = new StreamRecord<>(serializer.copy(rows.get(index)), times[index]);
            if (side == 0) oracle.processElement1(record);
            else oracle.processElement2(record);
        }
        try (var batch = ArrowRowDataBatch.transpose(rows, INPUT, allocator).withEnvelope(kinds, present, times)) {
            target.processElement(side, new StreamRecord<>(batch));
        }
    }

    private static void watermark(
            WindowJoinRegionFixture target,
            KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> oracle,
            long time)
            throws Exception {
        target.processWatermark(0, new Watermark(time));
        oracle.processWatermark1(new Watermark(time));
        target.processWatermark(1, new Watermark(time));
        oracle.processWatermark2(new Watermark(time));
    }

    private static void compare(
            WindowJoinRegionFixture target,
            KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> oracle)
            throws Exception {
        target.setProcessingTime(12345);
        oracle.setProcessingTime(12345);
        WindowJoinRegionMetrics.compare(target, oracle);
        var expected = new DataOutputSerializer(128);
        var serializer = new RowDataSerializer(OUTPUT);
        List<Long> times = new ArrayList<>();
        for (var record : oracle.extractOutputStreamRecords()) {
            serializer.serialize(record.getValue(), expected);
            times.add(record.hasTimestamp() ? record.getTimestamp() : null);
        }
        assertThat(target.rows.getCopyOfBuffer()).isEqualTo(expected.getCopyOfBuffer());
        assertThat(target.timestamps).isEqualTo(times);
        assertThat(target.controls.toArray())
                .containsExactly(oracle.getOutput().stream()
                        .filter(value -> !(value instanceof StreamRecord))
                        .toArray());
        target.rows.clear();
        target.timestamps.clear();
        target.controls.clear();
        oracle.getOutput().clear();
    }

    private static KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> flink(
            boolean rocks, OperatorSubtaskState restored) throws Exception {
        var selector = KeySelectorUtil.getRowDataSelector(
                WindowJoinRegionParityTest.class.getClassLoader(), new int[] {0}, InternalTypeInfo.of(INPUT));
        String code =
                "public class RegionWindowCondition extends org.apache.flink.api.common.functions.AbstractRichFunction "
                        + "implements org.apache.flink.table.runtime.generated.JoinCondition {"
                        + "public RegionWindowCondition(Object[] references) {} "
                        + "public boolean apply(org.apache.flink.table.data.RowData left, org.apache.flink.table.data.RowData right) { return true; }}";
        var operator = WindowJoinOperatorBuilder.builder()
                .leftSerializer(new RowDataSerializer(INPUT))
                .rightSerializer(new RowDataSerializer(INPUT))
                .generatedJoinCondition(new GeneratedJoinCondition("RegionWindowCondition", code, new Object[0]))
                .leftWindowEndIndex(1)
                .rightWindowEndIndex(1)
                .filterNullKeys(new boolean[] {true})
                .joinType(FlinkJoinType.INNER)
                .build();
        var harness = new KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData>(
                operator, selector, selector, selector.getProducedType(), GROUPS, 1, 0);
        harness.setStateBackend(rocks ? new EmbeddedRocksDBStateBackend(true) : new HashMapStateBackend());
        harness.setup(new RowDataSerializer(OUTPUT));
        if (restored != null) harness.initializeState(restored);
        harness.open();
        return harness;
    }
}
