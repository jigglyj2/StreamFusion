/*
 * Copyright 2026 StreamFusion Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package tech.streamfusion.flink.changelog;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamFilter;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.misc.DropUpdateBeforeFunction;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;

class StreamFusionDropUpdateBeforeOperatorTest {
    @Test
    void generatedChangelogMatchesFlinksFilterEventForEvent() throws Exception {
        List<StreamRecord<RowData>> changelog = new ArrayList<>();
        int value = 0;
        for (RowKind kind : RowKind.values()) {
            changelog.add(record(value++, kind, 100 + value));
        }

        assertThat(run(new StreamFusionDropUpdateBeforeOperator(), changelog))
                .containsExactlyElementsOf(run(new StreamFilter<>(new DropUpdateBeforeFunction()), changelog));
    }

    @Test
    void dropsOnlyUpdateBeforeAndPreservesRecordsAndWatermarks() throws Exception {
        RowType rowType = RowType.of(new IntType(false));
        try (OneInputStreamOperatorTestHarness<RowData, RowData> harness =
                new OneInputStreamOperatorTestHarness<>(new StreamFusionDropUpdateBeforeOperator())) {
            harness.setup(new RowDataSerializer(rowType));
            harness.open();
            harness.processElement(record(1, RowKind.INSERT, 10));
            harness.processElement(record(2, RowKind.UPDATE_BEFORE, 20));
            harness.processElement(record(3, RowKind.UPDATE_AFTER, 30));
            harness.processElement(record(4, RowKind.DELETE, 40));
            harness.processWatermark(new Watermark(50));

            assertThat(harness.getOutput())
                    .extracting(StreamFusionDropUpdateBeforeOperatorTest::eventSummary)
                    .containsExactly("+I:1@10", "+U:3@30", "-D:4@40", "watermark:50");
        }
    }

    private static StreamRecord<RowData> record(int value, RowKind kind, long timestamp) {
        GenericRowData row = GenericRowData.of(value);
        row.setRowKind(kind);
        return new StreamRecord<>(row, timestamp);
    }

    private static List<String> run(
            OneInputStreamOperator<RowData, RowData> operator, List<StreamRecord<RowData>> changelog) throws Exception {
        RowType rowType = RowType.of(new IntType(false));
        try (OneInputStreamOperatorTestHarness<RowData, RowData> harness =
                new OneInputStreamOperatorTestHarness<>(operator)) {
            harness.setup(new RowDataSerializer(rowType));
            harness.open();
            for (StreamRecord<RowData> record : changelog) {
                harness.processElement(record);
            }
            harness.processWatermark(new Watermark(200));
            return harness.getOutput().stream()
                    .map(StreamFusionDropUpdateBeforeOperatorTest::eventSummary)
                    .collect(Collectors.toList());
        }
    }

    private static String eventSummary(Object event) {
        if (event instanceof Watermark) {
            return "watermark:" + ((Watermark) event).getTimestamp();
        }
        @SuppressWarnings("unchecked")
        StreamRecord<RowData> record = (StreamRecord<RowData>) event;
        return record.getValue().getRowKind().shortString()
                + ":"
                + record.getValue().getInt(0)
                + "@"
                + record.getTimestamp();
    }
}
