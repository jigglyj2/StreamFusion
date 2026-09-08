/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.sql;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
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

class DateFormatGeneratedParityTest extends SqlParityTestSupport {
    @ParameterizedTest
    @ValueSource(longs = {3, 19, 71})
    void numericFormattingPreservesEveryGeneratedChangelogAcrossFullTimestampRange(long seed) throws Exception {
        var rows = new ArrayList<Row>();
        var random = new Random(seed);
        long[] edges = {
            Long.MIN_VALUE,
            Long.MIN_VALUE + 1,
            Long.MAX_VALUE,
            Long.MAX_VALUE - 1,
            -62167219200000L,
            -62198755200000L,
            253402300800000L,
            -1,
            0,
            1,
            -12219292800000L,
            951782400123L
        };
        for (int i = 0; i < 257; i++) {
            long millis = i < edges.length ? edges[i] : random.nextLong();
            LocalDateTime timestamp =
                    i % 17 == 16 ? null : LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneOffset.UTC);
            for (RowKind kind : RowKind.values()) rows.add(Row.ofKind(kind, (long) i, timestamp));
        }
        byte[] expected = execute(rows, false);
        byte[] actual = execute(rows, true);
        assertThat(actual).isEqualTo(expected);
        assertThat(StreamFusionPlannerFactory.nativePlanBatchCount())
                .withFailMessage(StreamFusionPlanningDiagnostics.explain())
                .isGreaterThan(0);
        SqlArchitectureAssertions.admission();
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
        // TIMESTAMP is independent of the session zone, including around DST gaps/overlaps.
        tables.getConfig().setLocalTimeZone(java.time.ZoneId.of("America/New_York"));
        tables.createTemporaryView(
                "format_input",
                tables.fromChangelogStream(
                        environment.fromCollection(
                                rows, Types.ROW_NAMED(new String[] {"id", "ts"}, Types.LONG, Types.LOCAL_DATE_TIME)),
                        Schema.newBuilder()
                                .column("id", DataTypes.BIGINT())
                                .column("ts", DataTypes.TIMESTAMP(3))
                                .build(),
                        ChangelogMode.all()));
        String sql = "SELECT id, DATE_FORMAT(ts, 'yyyy-MM-dd'), DATE_FORMAT(ts, 'HH:mm'), "
                + "DATE_FORMAT(ts, 'yyyy/MM/dd HH:mm:ss.SSS'), DATE_FORMAT(ts, 'yyyy/yyyy'), "
                + "DATE_FORMAT(ts, '''年''yyyy'' % it''''s ''MM/dd'), DATE_FORMAT(ts, ''), "
                + "CHAR_LENGTH(DATE_FORMAT(ts, 'yyyy-MM-dd')) FROM format_input WHERE id >= 0";
        var result = tables.executeSql(sql);
        var type = result.getResolvedSchema().toPhysicalRowDataType();
        var converter = org.apache.flink.table.data.conversion.DataStructureConverters.getConverter(type);
        converter.open(DateFormatGeneratedParityTest.class.getClassLoader());
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
        return bytes.getCopyOfBuffer();
    }
}
