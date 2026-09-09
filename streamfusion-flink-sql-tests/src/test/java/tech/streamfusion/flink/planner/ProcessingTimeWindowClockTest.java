/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Random;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Flink's controlled-clock oracle for Q12 prerequisites; no native admission is bypassed. */
class ProcessingTimeWindowClockTest {
    private static final RowType INPUT = RowType.of(new BigIntType(), new TimestampType(3));
    private static final RowType OUTPUT = RowType.of(
            new BigIntType(), new BigIntType(false), new TimestampType(false, 3), new TimestampType(false, 3));

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void onlyProcessingTimeFiresWindowsAndTheClockIsReadAtTheConsumer(boolean rocks) throws Exception {
        for (int seed : List.of(3, 19, 71))
            for (int gap : List.of(1000, 10000, 37000)) {
                var random = new Random(seed);
                try (var flink = oracle(rocks, gap, null)) {
                    for (int window = 0; window < 3; window++) {
                        long start = window * (long) gap;
                        Long key = window == 1 ? null : (long) seed;
                        flink.setProcessingTime(start + 1);
                        int first = 1 + random.nextInt(11);
                        input(flink, key, first);
                        flink.setProcessingTime(start + gap - 2);
                        int last = 1 + random.nextInt(11);
                        input(flink, key, last);
                        // PROCTIME's physical input slot is null: the window reads its own clock.
                        // Even terminal event time cannot close a processing-time window.
                        flink.processWatermark(Watermark.MAX_WATERMARK);
                        assertRows(flink);
                        flink.setProcessingTime(start + gap - 1);
                        assertRows(flink, result(key, first + last, start, start + gap));
                    }
                    flink.setProcessingTime(3L * gap + 1);
                    input(flink, 1L, 3);
                    flink.prepareSnapshotPreBarrier(8);
                    flink.getOperator().finish();
                    assertRows(flink); // Finishing the bounded source does not fire the open window.
                }
            }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void restoresPendingProcessingTimeTimers(boolean rocks) throws Exception {
        OperatorSubtaskState snapshot;
        try (var flink = oracle(rocks, 10000, null)) {
            flink.setProcessingTime(10001);
            input(flink, null, 4);
            flink.prepareSnapshotPreBarrier(1);
            snapshot = flink.snapshot(1, 10002);
            assertRows(flink);
        }
        try (var restored = oracle(rocks, 10000, snapshot)) {
            restored.processWatermark(Watermark.MAX_WATERMARK);
            assertRows(restored);
            restored.setProcessingTime(19998);
            input(restored, null, 2);
            assertRows(restored);
            restored.setProcessingTime(19999);
            assertRows(restored, result(null, 6, 10000, 20000));
        } finally {
            snapshot.discardState();
        }
    }

    static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> oracle(
            boolean rocks, int gap, OperatorSubtaskState restored) throws Exception {
        return oracle(rocks, gap, restored, 1.0);
    }

    static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> oracle(
            boolean rocks, int gap, OperatorSubtaskState restored, double operatorFraction) throws Exception {
        String sql = "WITH B AS (SELECT k, PROCTIME() AS pt FROM local_window_input) "
                + "SELECT k, COUNT(*) AS n, window_start, window_end FROM TABLE("
                + "TUMBLE(TABLE B, DESCRIPTOR(pt), INTERVAL '" + gap / 1000 + "' SECOND)) "
                + "GROUP BY k, window_start, window_end";
        return GlobalWindowFlinkOracle.create(
                SlicingWindowFlinkPlan.stage("WindowAggregate", sql, false, java.time.ZoneId.of("UTC")),
                rocks,
                restored,
                operatorFraction);
    }

    static void input(KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> flink, Long key, int count)
            throws Exception {
        var serializer = new RowDataSerializer(INPUT);
        for (int i = 0; i < count; i++)
            flink.processElement(new StreamRecord<>(serializer.toBinaryRow(GenericRowData.of(key, null)), 123));
    }

    static RowData result(Long key, long count, long start, long end) {
        return GenericRowData.of(key, count, TimestampData.fromEpochMillis(start), TimestampData.fromEpochMillis(end));
    }

    static void assertRows(KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> flink, RowData... expected)
            throws Exception {
        var serializer = new RowDataSerializer(OUTPUT);
        var actualBytes = new DataOutputSerializer(128);
        for (var event : flink.getOutput())
            if (event instanceof StreamRecord<?>) {
                var record = (StreamRecord<?>) event;
                assertThat(record.hasTimestamp()).isFalse();
                serializer.serialize((RowData) record.getValue(), actualBytes);
            }
        var expectedBytes = new DataOutputSerializer(128);
        for (var row : expected) serializer.serialize(row, expectedBytes);
        assertThat(actualBytes.getCopyOfBuffer()).containsExactly(expectedBytes.getCopyOfBuffer());
        flink.getOutput().clear();
    }
}
