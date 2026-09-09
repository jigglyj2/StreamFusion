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

class VarcharWideningGeneratedParityTest extends SqlParityTestSupport {
    @ParameterizedTest
    @ValueSource(longs = {3, 19, 71})
    void wideningCastsPreserveGeneratedChangelogWithoutTrimmingOrPadding(long seed) throws Exception {
        var rows = new ArrayList<Row>();
        var random = new Random(seed);
        String[] edges = {null, "", " ", "é", "e\u0301", "😀", "\u0000", "ninechars", "long".repeat(1024)};
        for (int i = 0; i < 257; i++) {
            // Flink widening trusts the declared width even for oversized source values.
            String value = i < edges.length ? edges[i] : "a界😀 ".repeat(random.nextInt(17));
            for (RowKind kind : RowKind.values()) rows.add(Row.ofKind(kind, (long) i, value));
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
        tables.createTemporaryView(
                "varchar_input",
                tables.fromChangelogStream(
                        environment.fromCollection(
                                rows, Types.ROW_NAMED(new String[] {"id", "s"}, Types.LONG, Types.STRING)),
                        Schema.newBuilder()
                                .column("id", DataTypes.BIGINT())
                                .column("s", DataTypes.VARCHAR(9))
                                .build(),
                        ChangelogMode.all()));
        String sql = "SELECT id, CAST(s AS VARCHAR(20)), CAST(s AS STRING), TRY_CAST(s AS VARCHAR(64)), "
                + "CAST(s AS VARCHAR(9)), CAST(CASE WHEN MOD(id, 2) = 0 THEN s "
                + "ELSE CAST('' AS VARCHAR(9)) END AS STRING), "
                + "CHAR_LENGTH(CAST(s AS VARCHAR(20))) FROM varchar_input WHERE id >= 0";
        var result = tables.executeSql(sql);
        var type = result.getResolvedSchema().toPhysicalRowDataType();
        var converter = org.apache.flink.table.data.conversion.DataStructureConverters.getConverter(type);
        converter.open(VarcharWideningGeneratedParityTest.class.getClassLoader());
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
