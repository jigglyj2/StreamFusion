/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.sort;

import static org.assertj.core.api.Assertions.assertThat;
import static tech.streamfusion.flink.sort.StreamFusionArrowBoundedSortOperatorTest.ROW_TYPE;
import static tech.streamfusion.flink.sort.StreamFusionArrowBoundedSortOperatorTest.SORT_SPEC;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.planner.codegen.sort.SortCodeGenerator;
import org.apache.flink.table.runtime.operators.sort.StreamSortOperator;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;

/** Direct native parity remains required even while SQL admission temporarily gates full sort. */
class GeneratedBoundedSortParityTest {
    @Test
    void generatedChangelogsMatchFlinkBytesAcrossBatchSizesAndBackends() throws Exception {
        for (int seed = 0; seed < 4; seed++) {
            List<GenericRowData> input = input(seed);
            byte[] expected = flink(input);
            for (boolean rocks : new boolean[] {false, true}) {
                try (RootAllocator allocator = new RootAllocator(64L << 20);
                        var harness = StreamFusionArrowBoundedSortOperatorTest.harness(null, rocks)) {
                    int batchSize = new int[] {1, 17, 128, 257}[seed];
                    for (int start = 0; start < input.size(); start += batchSize) {
                        StreamFusionArrowBoundedSortOperatorTest.process(
                                harness,
                                allocator,
                                input.subList(start, Math.min(start + batchSize, input.size()))
                                        .toArray(new GenericRowData[0]));
                    }
                    harness.endInput();
                    assertThat(harness.bytes.getCopyOfBuffer())
                            .as("seed %s, RocksDB %s", seed, rocks)
                            .isEqualTo(expected);
                }
            }
        }
    }

    private static List<GenericRowData> input(int seed) {
        Random random = new Random(3197 + seed);
        List<GenericRowData> rows = new ArrayList<>();
        for (int index = 0; index < 401; index++) {
            Integer number = index % 11 == 0 ? null : random.nextInt(41) - 20;
            StringData label = index % 13 == 0 ? null : StringData.fromString("é-" + random.nextInt(23));
            rows.add(row(number, label, RowKind.INSERT));
            if (index % 3 == 0) {
                rows.add(row(number, label, RowKind.UPDATE_AFTER));
            }
            if (index % 5 == 0) {
                rows.add(row(number, label, RowKind.UPDATE_BEFORE));
            }
            if (index % 15 == 0) {
                rows.add(row(number, label, RowKind.DELETE));
            }
        }
        return rows;
    }

    private static GenericRowData row(Integer number, StringData label, RowKind kind) {
        GenericRowData row = GenericRowData.of(number, label);
        row.setRowKind(kind);
        return row;
    }

    private byte[] flink(List<GenericRowData> input) throws Exception {
        var comparator = new SortCodeGenerator(new Configuration(), getClass().getClassLoader(), ROW_TYPE, SORT_SPEC)
                .generateRecordComparator("GeneratedSortParityComparator");
        var operator = new StreamSortOperator(InternalTypeInfo.of(ROW_TYPE), comparator);
        try (var harness = new OneInputStreamOperatorTestHarness<RowData, RowData>(operator)) {
            harness.setup(new RowDataSerializer(ROW_TYPE));
            harness.open();
            for (GenericRowData row : input) {
                harness.processElement(new StreamRecord<>(row));
            }
            operator.finish();
            DataOutputSerializer bytes = new DataOutputSerializer(1024);
            RowDataSerializer serializer = new RowDataSerializer(ROW_TYPE);
            for (RowData row : harness.extractOutputValues()) {
                serializer.serialize(row, bytes);
            }
            return bytes.getCopyOfBuffer();
        }
    }
}
