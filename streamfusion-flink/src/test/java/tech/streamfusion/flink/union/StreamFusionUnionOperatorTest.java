/*
 * Copyright 2026 StreamFusion Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package tech.streamfusion.flink.union;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.MultiInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;

class StreamFusionUnionOperatorTest {
    @Test
    void flushesInterleavedChangelogBeforeCombinedWatermark() throws Exception {
        RowType rowType = RowType.of(new IntType(false));
        try (MultiInputStreamOperatorTestHarness<RowData> harness =
                new MultiInputStreamOperatorTestHarness<>(new StreamFusionUnionOperatorFactory(2, rowType))) {
            harness.setup(new RowDataSerializer(rowType));
            harness.open();
            harness.processElement(0, record(10, RowKind.INSERT, 100));
            harness.processElement(1, record(20, RowKind.UPDATE_AFTER, 200));
            harness.processElement(0, record(30, RowKind.DELETE, 300));

            harness.processWatermark(0, new Watermark(50));

            assertThat(records(harness.getOutput()))
                    .extracting(record -> List.of(
                            record.getValue().getInt(0), record.getValue().getRowKind(), record.getTimestamp()))
                    .containsExactly(
                            List.of(10, RowKind.INSERT, 100L),
                            List.of(20, RowKind.UPDATE_AFTER, 200L),
                            List.of(30, RowKind.DELETE, 300L));
            assertThat(harness.getOutput()).noneMatch(Watermark.class::isInstance);

            harness.processWatermark(1, new Watermark(60));

            assertThat(harness.getOutput())
                    .filteredOn(Watermark.class::isInstance)
                    .containsExactly(new Watermark(50));
        }
    }

    private static StreamRecord<RowData> record(int value, RowKind kind, long timestamp) {
        GenericRowData row = GenericRowData.of(value);
        row.setRowKind(kind);
        return new StreamRecord<>(row, timestamp);
    }

    @SuppressWarnings("unchecked")
    private static List<StreamRecord<RowData>> records(Queue<Object> output) {
        List<StreamRecord<RowData>> records = new ArrayList<>();
        for (Object event : output) {
            if (event instanceof StreamRecord<?>) {
                records.add((StreamRecord<RowData>) event);
            }
        }
        return records;
    }
}
