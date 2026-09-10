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

class RegexExtractGeneratedParityTest extends SqlParityTestSupport {
    @ParameterizedTest
    @ValueSource(longs = {3, 19, 71})
    void completeGeneratedChangelogsMatchForCaptureAlternativesAndUnicode(long seed) throws Exception {
        var rows = new ArrayList<Row>();
        var random = new Random(seed);
        String[] edges = {
            null,
            "",
            "a",
            "b",
            "ab",
            "abc",
            "channel_id=",
            "xchannel_id=wrong",
            "prefix&channel_id=first&channel_id=second",
            "channel_id=😀漢é\u0301\n\r\u0085\u2028\u2029",
            "\u0000a\u0000",
            "channel_id=" + "é😀".repeat(8192)
        };
        String[] alphabet = {
            "a", "b", "c", "&", "=", "1", " ", "\n", "\r", "\u0085", "\u2028", "\u2029", "\u0000", "😀", "漢", "é",
            "\u0301"
        };
        for (int i = 0; i < 513; i++) {
            String value;
            if (i < edges.length) value = edges[i];
            else {
                var text = new StringBuilder();
                for (int n = 0, length = random.nextInt(64); n < length; n++)
                    text.append(alphabet[random.nextInt(alphabet.length)]);
                value = i % 3 == 0 ? "prefix&channel_id=" + text + "&tail=1" : text.toString();
            }
            for (RowKind kind : RowKind.values()) rows.add(Row.ofKind(kind, (long) i, value));
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
                "regex_input",
                tables.fromChangelogStream(
                        environment.fromCollection(
                                rows, Types.ROW_NAMED(new String[] {"id", "value"}, Types.LONG, Types.STRING)),
                        Schema.newBuilder()
                                .column("id", DataTypes.BIGINT())
                                .column("value", DataTypes.STRING())
                                .build(),
                        ChangelogMode.all()));
        String sql = "SELECT id, REGEXP_EXTRACT(`value`, '(&|^)channel_id=([^&]*)', 2), "
                + "REGEXP_EXTRACT(`value`, '(a)|(b)', 2), REGEXP_EXTRACT(`value`, '((a)|b)(c*)', 2), "
                + "REGEXP_EXTRACT(`value`, '(a|ab)(b*)', 0), REGEXP_EXTRACT(`value`, '(a*)a', 1), "
                + "REGEXP_EXTRACT(`value`, '([^a-c]+)', 1), REGEXP_EXTRACT(`value`, '[A-Z]+'), "
                + "REGEXP_EXTRACT(`value`, '()'), REGEXP_EXTRACT(`value`, ''), "
                + "CASE WHEN id < 8 THEN REGEXP_EXTRACT(`value`, '(a?)(a*)', 1) ELSE REGEXP_EXTRACT(`value`, '([^&]*)', 1) END, "
                + "REGEXP_EXTRACT(REGEXP_EXTRACT(`value`, '([^&]*)', 1), '(a|b)', 1) "
                + "FROM regex_input WHERE id < 12 OR REGEXP_EXTRACT(`value`, '(a)|(b)', 1) IS NOT NULL";
        var result = tables.executeSql(sql);
        var type = result.getResolvedSchema().toPhysicalRowDataType();
        var converter = org.apache.flink.table.data.conversion.DataStructureConverters.getConverter(type);
        converter.open(RegexExtractGeneratedParityTest.class.getClassLoader());
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
