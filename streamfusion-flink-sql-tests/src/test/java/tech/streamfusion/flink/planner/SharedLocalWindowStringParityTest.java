/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.junit.jupiter.api.Test;

/** Variable UTF-8 keys retain the SQL-generated Flink local buffer/control boundaries. */
class SharedLocalWindowStringParityTest {
    @Test
    void nullableUnicodeKeysMatchGeneratedFlinkControlChangelogs() throws Exception {
        var keys = new String[] {null, "", "1234567", "12345678", "ééé", "éééé", "日本語", "🙂🙂", "x".repeat(257)};
        for (boolean tumble : List.of(false, true))
            for (int batchSize : List.of(7, 31))
                try (var run = new SharedLocalWindowParityTest.Comparison(tumble, true)) {
                    var random = new Random(83);
                    for (int phase = 0; phase < 12; phase++) {
                        var rows = new ArrayList<RowData>();
                        for (int row = 0; row < 31; row++) {
                            var key = keys[random.nextInt(keys.length)];
                            rows.add(GenericRowData.of(
                                    key == null ? null : StringData.fromString(key),
                                    TimestampData.fromEpochMillis(random.nextInt(16001) - 8000)));
                        }
                        for (int offset = 0; offset < rows.size(); offset += batchSize)
                            run.process(rows.subList(offset, Math.min(rows.size(), offset + batchSize)));
                        if (phase % 3 == 2) run.watermark(phase * 1000L - 3000);
                        if (phase == 6) run.checkpoint(7);
                    }
                    run.watermark(Long.MAX_VALUE);
                    run.checkpoint(8);
                }
    }

    @Test
    void variableStringPressureBoundariesMatchFlinkAcrossArrowBatchSizes() throws Exception {
        var keys = new StringData[17];
        var small = new String[] {null, "", "1234567", "12345678", "ééé", "éééé", "日本語", "🙂🙂"};
        for (int i = 0; i < keys.length; i++) {
            var key = i < small.length ? small[i] : "é".repeat(i * 17);
            keys[i] = key == null ? null : StringData.fromString(key);
        }
        for (boolean tumble : List.of(false, true))
            for (int batchSize : List.of(4096, 16384))
                try (var run = new SharedLocalWindowParityTest.Comparison(tumble, true)) {
                    for (int start = 0; start < 40000; start += batchSize) {
                        var rows = new ArrayList<RowData>();
                        for (int row = start; row < Math.min(40000, start + batchSize); row++)
                            rows.add(GenericRowData.of(keys[row % keys.length], TimestampData.fromEpochMillis(1000)));
                        run.process(rows);
                    }
                    assertThat(run.outputs).isGreaterThan(17);
                    run.checkpoint(1);
                }
    }
}
