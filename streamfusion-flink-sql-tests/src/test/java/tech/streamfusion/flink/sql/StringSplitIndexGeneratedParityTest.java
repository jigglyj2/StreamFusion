/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.sql;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

class StringSplitIndexGeneratedParityTest extends SqlParityTestSupport {
    @ParameterizedTest
    @ValueSource(longs = {3, 19, 71})
    void completeGeneratedChangelogsMatchForDelimitersAndIndices(long seed) throws Exception {
        var rows = new ArrayList<Row>();
        var random = new Random(seed);
        String[] edges = {
            null,
            "",
            "a",
            "/",
            "//",
            "a/",
            "/a",
            "a//b/",
            "a::b::::c::",
            "😀界漢界",
            "a\u0000b",
            "é😀".repeat(8192),
            "/".repeat(8192)
        };
        String[] alphabet = {
            "a", "b", "c", "/", "::", "界", "=", "1", " ", "\n", "\r", "\u0085", "\u2028", "\u2029", "\u0000", "😀", "漢",
            "é", "\u0301"
        };
        for (int i = 0; i < 513; i++) {
            String value;
            if (i < edges.length) value = edges[i];
            else {
                var text = new StringBuilder();
                for (int n = 0, length = random.nextInt(64); n < length; n++)
                    text.append(alphabet[random.nextInt(alphabet.length)]);
                value = text.toString();
            }
            int index = i % 11 == 0 ? Integer.MAX_VALUE : i % 13 == 0 ? Integer.MIN_VALUE : random.nextInt(12) - 3;
            for (RowKind kind : RowKind.values())
                rows.add(Row.ofKind(kind, (long) i, value, i % 7 == 0 ? null : index));
        }
        byte[] expected = execute(rows, false);
        assertThat(execute(rows, true)).isEqualTo(expected);
    }

    private static byte[] execute(List<Row> rows, boolean enabled) throws Exception {
        System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        StreamFusionPlannerFactory.resetMetrics();
        if (enabled)
            System.setProperty(
                    StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, StreamFusionPlannerFactory.class.getName());
        var environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        var tables = StreamTableEnvironment.create(environment);
        tables.createTemporaryView(
                "split_input",
                tables.fromChangelogStream(
                        environment.fromCollection(
                                rows,
                                Types.ROW_NAMED(
                                        new String[] {"id", "value", "idx"}, Types.LONG, Types.STRING, Types.INT)),
                        Schema.newBuilder()
                                .column("id", DataTypes.BIGINT())
                                .column("value", DataTypes.STRING())
                                .column("idx", DataTypes.INT())
                                .build(),
                        ChangelogMode.all()));
        String sql = "SELECT id, SPLIT_INDEX(`value`, '/', idx), SPLIT_INDEX(`value`, '::', idx), "
                + "SPLIT_INDEX(`value`, '界', idx), SPLIT_INDEX(`value`, '/', 0), "
                + "SPLIT_INDEX(`value`, '/', 1), SPLIT_INDEX(`value`, '/', 2147483647), "
                + "CASE WHEN id < 8 THEN SPLIT_INDEX(`value`, '/', 2) ELSE SPLIT_INDEX(`value`, '::', 0) END, "
                + "SPLIT_INDEX(SPLIT_INDEX(`value`, '/', 0), '::', 0) "
                + "FROM split_input WHERE id < 16 OR SPLIT_INDEX(`value`, '/', 0) IS NOT NULL";
        var result = tables.executeSql(sql);
        var type = result.getResolvedSchema().toPhysicalRowDataType();
        var converter = org.apache.flink.table.data.conversion.DataStructureConverters.getConverter(type);
        converter.open(StringSplitIndexGeneratedParityTest.class.getClassLoader());
        var serializer = new org.apache.flink.table.runtime.typeutils.RowDataSerializer(
                (org.apache.flink.table.types.logical.RowType) type.getLogicalType());
        var bytes = new org.apache.flink.core.memory.DataOutputSerializer(128);
        try (var output = result.collect()) {
            while (output.hasNext()) {
                var row = output.next();
                var internal = (org.apache.flink.table.data.RowData) converter.toInternal(row);
                internal.setRowKind(row.getKind());
                serializer.serialize(internal, bytes);
            }
        }
        if (enabled) {
            assertThat(StreamFusionPlannerFactory.nativePlanBatchCount())
                    .withFailMessage(StreamFusionPlanningDiagnostics.explain())
                    .isPositive();
            SqlArchitectureAssertions.admission();
        }
        return bytes.getCopyOfBuffer();
    }
}
