/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.arrow;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;

class NestedWriterGrowthTest {
    private static final RowType LEAF = RowType.of(new BigIntType(), new VarCharType());
    private static final RowType PAYLOAD =
            RowType.of(LEAF, new ArrayType(LEAF), new MapType(new VarCharType(false, VarCharType.MAX_LENGTH), LEAF));
    private static final RowType INPUT = RowType.of(PAYLOAD);

    @Test
    void nestedStructListAndMapGrowthPreservesFlinkBytesAcrossWriterReuse() throws Exception {
        try (var allocator = new RootAllocator(64L << 20);
                var writer = new ArrowRowDataBatchWriter(INPUT, allocator)) {
            for (int pass = 0; pass < 2; pass++) {
                var rows = rows(5001, pass);
                for (var row : rows) writer.write(row);
                try (var batch = writer.finishBatch()) {
                    compare(rows, batch);
                }
                writer.reset();
            }
        }
    }

    @Test
    void standaloneTransposeRefreshesNestedBuffersWhenCapacityGrows() throws Exception {
        try (var allocator = new RootAllocator(64L << 20)) {
            var rows = rows(5001, 3);
            try (var batch = ArrowRowDataBatch.transpose(rows, INPUT, allocator)) {
                compare(rows, batch);
            }
            assertThat(allocator.getAllocatedMemory()).isZero();
        }
    }

    private static List<RowData> rows(int count, int pass) {
        var rows = new ArrayList<RowData>();
        for (int index = 0; index < count; index++) {
            var leaf = GenericRowData.of(
                    index % 11 == 0 ? null : (long) index - pass,
                    index % 7 == 0 ? null : StringData.fromString("é".repeat(index % 101 + 1)));
            var map = new LinkedHashMap<StringData, RowData>();
            map.put(StringData.fromString("key-" + index), index % 19 == 0 ? null : leaf);
            var payload = GenericRowData.of(
                    index % 3 == 0 ? null : leaf,
                    index % 13 == 0 ? null : new GenericArrayData(new RowData[] {leaf, null, leaf}),
                    index % 17 == 0 ? null : new GenericMapData(map));
            rows.add(GenericRowData.of(index % 5 == 0 ? null : payload));
        }
        return rows;
    }

    private static void compare(List<RowData> rows, ArrowRowDataBatch batch) throws Exception {
        assertThat(batch.size()).isEqualTo(rows.size());
        var serializer = new RowDataSerializer(INPUT);
        var expected = new DataOutputSerializer(128);
        var actual = new DataOutputSerializer(128);
        for (int index = 0; index < rows.size(); index++) {
            serializer.serialize(rows.get(index), expected);
            serializer.serialize(batch.rowView(index), actual);
            assertThat(actual.getCopyOfBuffer()).as("row %s", index).containsExactly(expected.getCopyOfBuffer());
            actual.clear();
            expected.clear();
        }
    }
}
