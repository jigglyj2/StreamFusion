/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.junit.jupiter.api.Test;

/** Pins the upstream lifecycle before admitting the shared native local-window implementation. */
class LocalWindowFlinkControlContractTest {
    @Test
    void flushesEveryBufferedSliceInFirstAppearanceOrderAtApplicableControlBoundaries() throws Exception {
        try (var harness = LocalWindowFlinkOracle.create(8L << 20)) {
            input(harness, 9, 5000);
            input(harness, 2, 1000);
            input(harness, 9, 5001);
            assertThat(drain(harness)).isEmpty();
            harness.processWatermark(new Watermark(999));
            harness.processWatermark(new Watermark(1998));
            assertThat(drain(harness)).isEmpty();
            harness.processWatermark(new Watermark(1999));
            assertThat(drain(harness)).containsExactly("9:2:6000", "2:1:2000");

            input(harness, 2, 1000);
            harness.processWatermark(new Watermark(2000));
            assertThat(drain(harness)).isEmpty();
            harness.prepareSnapshotPreBarrier(1);
            assertThat(drain(harness)).containsExactly("2:1:2000");
            harness.prepareSnapshotPreBarrier(2);
            assertThat(drain(harness)).isEmpty();

            input(harness, 3, -1);
            input(harness, 4, 9000);
            harness.processWatermark(new Watermark(Long.MAX_VALUE));
            assertThat(drain(harness)).containsExactly("3:1:0", "4:1:10000");
        }
    }

    @Test
    void memoryPressureFlushesBeforeWatermarksAndRetainsEveryInputContribution() throws Exception {
        try (var harness = LocalWindowFlinkOracle.create(3L << 20)) {
            int firstFlush = -1;
            for (int row = 0; row < 180000; row++) {
                input(harness, row % 17, 1000);
                if (firstFlush < 0 && !harness.getOutput().isEmpty()) firstFlush = row;
            }
            assertThat(firstFlush).isEqualTo(64529);
            var partials = drain(harness);
            assertThat(partials).isNotEmpty();
            harness.prepareSnapshotPreBarrier(1);
            partials.addAll(drain(harness));
            var counts = new long[17];
            for (var partial : partials) {
                var fields = partial.split(":");
                counts[Integer.parseInt(fields[0])] += Long.parseLong(fields[1]);
                assertThat(fields[2]).isEqualTo("2000");
            }
            for (int key = 0; key < 17; key++) assertThat(counts[key]).isEqualTo((180000L + 16 - key) / 17);
        }
    }

    private static void input(OneInputStreamOperatorTestHarness<RowData, RowData> harness, long key, long time)
            throws Exception {
        harness.processElement(new StreamRecord<>(GenericRowData.of(key, TimestampData.fromEpochMillis(time)), time));
    }

    private static List<String> drain(OneInputStreamOperatorTestHarness<RowData, RowData> harness) {
        var result = new ArrayList<String>();
        for (var event : harness.getOutput()) {
            if (event instanceof StreamRecord<?>) {
                var record = (StreamRecord<?>) event;
                assertThat(record.hasTimestamp()).isFalse();
                var row = (RowData) record.getValue();
                assertThat(row.getRowKind()).isEqualTo(org.apache.flink.types.RowKind.INSERT);
                result.add(row.getLong(0) + ":" + row.getLong(1) + ":" + row.getLong(2));
            }
        }
        harness.getOutput().clear();
        return result;
    }
}
