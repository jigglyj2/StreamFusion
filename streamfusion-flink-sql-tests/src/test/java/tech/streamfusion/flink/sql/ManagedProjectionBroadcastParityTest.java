/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.sql;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

class ManagedProjectionBroadcastParityTest extends SqlParityTestSupport {
    @Test
    void typedNullsAndConstantsAcrossAllPayloadFamiliesPreserveGeneratedChangelogs() throws Exception {
        String nulls = SelectDistinctFallbackParityTest.distinctTypes()
                .map(arguments -> (DataType) arguments.get()[2])
                .map(type -> "CAST(NULL AS " + type.getLogicalType().asSerializableString() + ")")
                .collect(Collectors.joining(", "));
        String sql = "SELECT id, " + nulls + ", '" + "é東京🙂".repeat(32) + "', "
                + "X'0080FF', CAST(123456789.25 AS DECIMAL(25, 2)), "
                + "CAST(7 AS BIGINT) + CAST(11 AS BIGINT) "
                + "FROM broadcast_input WHERE id IS NULL OR id <> 0";
        var rows = new ArrayList<Row>();
        for (int index = -130; index < 130; index++) {
            for (RowKind kind : RowKind.values()) rows.add(Row.ofKind(kind, index % 17 == 0 ? null : index));
        }
        byte[] expected = execute(rows, sql, false);
        byte[] actual = execute(rows, sql, true);
        assertThat(actual).isEqualTo(expected);
        assertThat(StreamFusionPlannerFactory.nativePlanBatchCount())
                .withFailMessage(StreamFusionPlanningDiagnostics.explain())
                .isGreaterThan(0);
        SqlArchitectureAssertions.admission();
    }

    private static byte[] execute(List<Row> rows, String sql, boolean nativeEnabled) throws Exception {
        System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        StreamFusionPlannerFactory.resetMetrics();
        if (nativeEnabled)
            System.setProperty(
                    StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, StreamFusionPlannerFactory.class.getName());
        var environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        var tables = StreamTableEnvironment.create(environment);
        tables.createTemporaryView(
                "broadcast_input",
                tables.fromChangelogStream(
                        environment.fromCollection(rows, Types.ROW_NAMED(new String[] {"id"}, Types.INT)),
                        Schema.newBuilder().column("id", DataTypes.INT()).build(),
                        ChangelogMode.all()));
        return collect(tables.executeSql(sql));
    }
}
