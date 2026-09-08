/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;

class DistinctWindowEventBytesTest {
    private final DistinctWindowFixture fixture = new DistinctWindowFixture(false);

    @Test
    void onlyEqualWindowEndKeyOrderIsNormalized() throws Exception {
        assertThat(canonical()).isEmpty();
        assertThat(canonical(1, 2, -1, 3)).isEqualTo(canonical(2, 1, -1, 3));
        assertThat(canonical(1, 2, -1, 3)).isNotEqualTo(canonical(1, 2, 3, -1));
        assertThat(canonical(1, 2)).isNotEqualTo(canonical(1, 2, 2));
        assertThat(canonical(1, 2)).isNotEqualTo(canonical(1, 3));
        assertThat(canonical(1, 20)).isNotEqualTo(canonical(20, 1));
    }

    @Test
    void recordKindsRemainPartOfTheContract() throws Exception {
        var bytes = new DataOutputSerializer(128);
        var row = GenericRowData.of(1L, TimestampData.fromEpochMillis(0), TimestampData.fromEpochMillis(2000));
        row.setRowKind(RowKind.DELETE);
        StageEventBytes.row(fixture.output, row, false, 0, bytes);
        assertThatThrownBy(() -> DistinctWindowEventBytes.canonical(fixture, bytes.getCopyOfBuffer()))
                .isInstanceOf(AssertionError.class);
    }

    private byte[] canonical(int... ids) throws Exception {
        var bytes = new DataOutputSerializer(128);
        for (int id : ids) {
            if (id == -1) StageEventBytes.encode(fixture.output, new Watermark(1999), bytes);
            else {
                long end = id >= 20 ? 4000 : 2000;
                StageEventBytes.row(
                        fixture.output,
                        GenericRowData.of(
                                (long) id,
                                TimestampData.fromEpochMillis(end - 2000),
                                TimestampData.fromEpochMillis(end)),
                        false,
                        0,
                        bytes);
            }
        }
        return DistinctWindowEventBytes.canonical(fixture, bytes.getCopyOfBuffer());
    }
}
