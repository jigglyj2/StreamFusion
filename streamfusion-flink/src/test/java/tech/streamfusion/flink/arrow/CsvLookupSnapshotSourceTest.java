/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.arrow;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.table.types.inference.TypeTransformations;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.InstantiationUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@SuppressWarnings("deprecation")
class CsvLookupSnapshotSourceTest {
    @TempDir
    Path directory;

    private final ClassLoader loader = getClass().getClassLoader();

    @Test
    void generatedConfiguredParsingAndProjectionMatchTheActualFlinkLookupFunction() throws Exception {
        for (int seed : new int[] {3, 17, 91}) {
            for (boolean projected : new boolean[] {false, true}) {
                Path path = directory.resolve("side-" + seed + "-" + projected + ".csv");
                CsvTableSource table = CsvTableSource.builder()
                        .path(path.toUri().toString())
                        .field("key", DataTypes.BIGINT())
                        .field("value", DataTypes.STRING())
                        .fieldDelimiter("|")
                        .quoteCharacter('"')
                        .commentPrefix("#")
                        .ignoreFirstLine()
                        .ignoreParseErrors()
                        .emptyColumnAsNull()
                        .build();
                if (projected) table = table.projectFields(new int[] {1, 0});
                var source = InstantiationUtil.clone(CsvLookupSnapshotSource.from(table), loader);
                assertThat(Files.exists(path)).isFalse(); // Description/serialization must not open the file.
                var csv = new StringBuilder("key|value\n# ignored comment\n");
                for (int i = 0; i < 73; i++) {
                    String key = i % 11 == 0 ? "" : Integer.toString((i * seed) % 7 - 3);
                    String value = i % 13 == 0 ? "" : "\"é|" + seed + "-" + i + "\"";
                    csv.append(key).append('|').append(value).append('\n');
                    if (i % 19 == 0) csv.append("bad-long|skip me\n");
                }
                Files.writeString(path, csv.toString());
                int keyColumn = projected ? 1 : 0;
                var oracle = (CsvTableSource.CsvLookupFunction) table.getLookupFunction(new String[] {"key"});
                oracle.open(new FunctionContext(null));
                try {
                    for (int batchRows : new int[] {1, 7, 128}) {
                        List<Row> all = read(source, table, batchRows);
                        // Flink's lenient parser retains malformed numeric fields as null.
                        assertThat(all).hasSize(77);
                        for (Long key : Arrays.asList(null, -3L, -2L, -1L, 0L, 1L, 2L, 3L, 100L)) {
                            var expected = new ArrayList<Row>();
                            oracle.setCollector(new Collector<Row>() {
                                @Override
                                public void collect(Row row) {
                                    expected.add(Row.copy(row));
                                }

                                @Override
                                public void close() {}
                            });
                            oracle.eval(key);
                            var actual = new ArrayList<Row>();
                            for (Row row : all) {
                                if (java.util.Objects.equals(key, row.getField(keyColumn))) actual.add(row);
                            }
                            assertThat(actual)
                                    .as("seed=%s projected=%s batch=%s key=%s", seed, projected, batchRows, key)
                                    .containsExactlyElementsOf(expected);
                            var converter = DataStructureConverters.getConverter(table.getProducedDataType());
                            converter.open(loader);
                            var serializer = new RowDataSerializer(source.rowType());
                            for (int index = 0; index < expected.size(); index++) {
                                byte[] expectedBytes = bytes(
                                        serializer.toBinaryRow((RowData) converter.toInternal(expected.get(index))));
                                byte[] actualBytes = bytes(
                                        serializer.toBinaryRow((RowData) converter.toInternal(actual.get(index))));
                                assertThat(actualBytes).containsExactly(expectedBytes);
                            }
                        }
                    }
                } finally {
                    oracle.close();
                }
            }
        }
    }

    private static byte[] bytes(org.apache.flink.table.data.binary.BinaryRowData row) {
        return org.apache.flink.table.data.binary.BinarySegmentUtils.copyToBytes(
                row.getSegments(), row.getOffset(), row.getSizeInBytes());
    }

    private List<Row> read(CsvLookupSnapshotSource source, CsvTableSource table, int batchRows) throws Exception {
        var converter = DataStructureConverters.getConverter(
                DataTypeUtils.transform(table.getProducedDataType(), TypeTransformations.timeToSqlTypes()));
        converter.open(loader);
        var rows = new ArrayList<Row>();
        var retained = new ArrayList<ArrowRowDataBatch>();
        try (var allocator = new RootAllocator(16L << 20)) {
            try {
                try (var cursor = source.open(allocator, loader, batchRows)) {
                    ArrowRowDataBatch batch;
                    while ((batch = cursor.nextBatch()) != null) {
                        assertThat(batch.size()).isBetween(1, batchRows);
                        retained.add(batch);
                    }
                    assertThat(cursor.nextBatch()).isNull();
                }
                // Retain all batches through later reads and cursor close. Reusing a mutable writer
                // would corrupt earlier values here, especially strings with unequal lengths.
                for (var batch : retained) {
                    for (int i = 0; i < batch.size(); i++) {
                        rows.add(Row.copy((Row) converter.toExternal(batch.rowView(i))));
                    }
                }
            } finally {
                retained.forEach(ArrowRowDataBatch::close);
            }
            assertThat(allocator.getAllocatedMemory()).isZero();
        }
        return rows;
    }

    @Test
    void reopeningReloadsTheFileAndClosingDoesNotInvalidateReturnedBatches() throws Exception {
        Path path = directory.resolve("reload.csv");
        CsvTableSource table = CsvTableSource.builder()
                .path(path.toUri().toString())
                .field("key", DataTypes.BIGINT())
                .field("value", DataTypes.STRING())
                .build();
        var source = CsvLookupSnapshotSource.from(table);
        Files.writeString(path, "1,first\n2,second\n");
        assertThat(read(source, table, 1)).containsExactly(Row.of(1L, "first"), Row.of(2L, "second"));
        Files.writeString(path, "1,reloaded\n");
        assertThat(read(source, table, 1)).containsExactly(Row.of(1L, "reloaded"));
    }

    @Test
    void strictParseFailureClosesTheCursorAndReleasesArrowBuffers() throws Exception {
        Path path = directory.resolve("invalid.csv");
        Files.writeString(path, "1,valid\nnot-a-long,invalid\n");
        var table = CsvTableSource.builder()
                .path(path.toUri().toString())
                .field("key", DataTypes.BIGINT())
                .field("value", DataTypes.STRING())
                .build();
        var source = CsvLookupSnapshotSource.from(table);
        try (var allocator = new RootAllocator(1L << 20);
                var cursor = source.open(allocator, loader, 17)) {
            assertThatThrownBy(cursor::nextBatch).isInstanceOf(org.apache.flink.api.common.io.ParseException.class);
            assertThat(allocator.getAllocatedMemory()).isZero();
            assertThatThrownBy(cursor::nextBatch).isInstanceOf(IllegalStateException.class);
        }
    }

    @Test
    void allocationDenialAndEarlyCloseReleaseResources() throws Exception {
        Path path = directory.resolve("budget.csv");
        Files.writeString(path, "1,first\n2,second\n");
        var table = CsvTableSource.builder()
                .path(path.toUri().toString())
                .field("key", DataTypes.BIGINT())
                .field("value", DataTypes.STRING())
                .build();
        var source = CsvLookupSnapshotSource.from(table);
        try (var allocator = new RootAllocator(1);
                var cursor = source.open(allocator, loader, 17)) {
            assertThatThrownBy(cursor::nextBatch).isInstanceOf(org.apache.arrow.memory.OutOfMemoryException.class);
            assertThat(allocator.getAllocatedMemory()).isZero();
        }
        try (var allocator = new RootAllocator(1L << 20)) {
            var cursor = source.open(allocator, loader, 1);
            var batch = cursor.nextBatch();
            cursor.close();
            cursor.close();
            assertThat(batch.rowView(0).getString(1).toString()).isEqualTo("first");
            batch.close();
            assertThat(allocator.getAllocatedMemory()).isZero();
        }
    }

    @Test
    void temporalAndDecimalPayloadsUseTheLegacyReadersActualConversionClasses() throws Exception {
        Path path = directory.resolve("types.csv");
        Files.writeString(path, "1,1969-12-31,12:34:56,1969-12-31 23:59:59.123,-17.25\n");
        var table = CsvTableSource.builder()
                .path(path.toUri().toString())
                .field("key", DataTypes.BIGINT())
                .field("day", DataTypes.DATE())
                .field("time", DataTypes.TIME(0))
                .field("stamp", DataTypes.TIMESTAMP(3))
                .field("amount", DataTypes.DECIMAL(10, 2))
                .build();
        var expected = new ArrayList<Row>();
        var oracle = (CsvTableSource.CsvLookupFunction) table.getLookupFunction(new String[] {"key"});
        oracle.setCollector(new Collector<Row>() {
            @Override
            public void collect(Row row) {
                expected.add(Row.copy(row));
            }

            @Override
            public void close() {}
        });
        oracle.open(new FunctionContext(null));
        try {
            oracle.eval(1L);
            assertThat(read(CsvLookupSnapshotSource.from(table), table, 1)).containsExactlyElementsOf(expected);
            assertThat(expected).hasSize(1);
        } finally {
            oracle.close();
        }
    }

    @Test
    void everySplitUsesTheSameDuplicateOrderAsFlink() throws Exception {
        Path files = Files.createDirectory(directory.resolve("splits"));
        Files.writeString(files.resolve("part-a"), "1,first\n1,second\n");
        Files.writeString(files.resolve("part-b"), "1,third\n");
        Files.writeString(files.resolve("part-c"), "");
        var table = CsvTableSource.builder()
                .path(files.toUri().toString())
                .field("key", DataTypes.BIGINT())
                .field("value", DataTypes.STRING())
                .build();
        var expected = new ArrayList<Row>();
        var oracle = (CsvTableSource.CsvLookupFunction) table.getLookupFunction(new String[] {"key"});
        oracle.setCollector(new Collector<Row>() {
            @Override
            public void collect(Row row) {
                expected.add(Row.copy(row));
            }

            @Override
            public void close() {}
        });
        oracle.open(new FunctionContext(null));
        try {
            oracle.eval(1L);
            assertThat(read(CsvLookupSnapshotSource.from(table), table, 2)).containsExactlyElementsOf(expected);
            assertThat(expected).hasSize(3);
        } finally {
            oracle.close();
        }
    }
}
