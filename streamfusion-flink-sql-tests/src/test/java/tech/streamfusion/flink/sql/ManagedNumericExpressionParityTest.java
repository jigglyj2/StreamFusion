/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.sql;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
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
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

class ManagedNumericExpressionParityTest extends SqlParityTestSupport {
    @Test
    void composedNumericKernelsPreserveGeneratedChangelogs() throws Exception {
        var rows = new ArrayList<Row>();
        for (int seed = 0; seed < 4; seed++) {
            var random = new Random(seed);
            for (int index = 0; index < 64; index++) {
                Object[] values = {
                    index % 11 == 0 ? null : random.nextInt(65536) - 32768,
                    index % 13 == 0 ? null : random.nextLong(),
                    index % 7 == 0 ? null : (random.nextInt(4096) - 2048) / 8.0f,
                    index % 5 == 0 ? null : (random.nextInt(4096) - 2048) / 16.0,
                    index % 3 == 0 ? null : BigDecimal.valueOf(random.nextInt(1000000) - 500000, 4),
                    index % 17 == 0 ? null : index % 2 == 0
                };
                for (RowKind kind : RowKind.values()) rows.add(Row.ofKind(kind, values));
            }
        }
        byte[] expected = execute(rows, false);
        byte[] actual = execute(rows, true);
        assertThat(actual).isEqualTo(expected);
        assertThat(StreamFusionPlannerFactory.nativePlanBatchCount())
                .withFailMessage(StreamFusionPlanningDiagnostics.explain())
                .isGreaterThan(0);
        SqlArchitectureAssertions.admission();
    }

    private static byte[] execute(List<Row> rows, boolean nativeEnabled) throws Exception {
        System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        StreamFusionPlannerFactory.resetMetrics();
        if (nativeEnabled)
            System.setProperty(
                    StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, StreamFusionPlannerFactory.class.getName());
        var environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        var tables = StreamTableEnvironment.create(environment);
        tables.createTemporaryView(
                "numbers",
                tables.fromChangelogStream(
                        environment.fromCollection(
                                rows,
                                Types.ROW_NAMED(
                                        new String[] {"i", "b", "f", "d", "money", "keep_row"},
                                        Types.INT,
                                        Types.LONG,
                                        Types.FLOAT,
                                        Types.DOUBLE,
                                        Types.BIG_DEC,
                                        Types.BOOLEAN)),
                        Schema.newBuilder()
                                .column("i", DataTypes.INT())
                                .column("b", DataTypes.BIGINT())
                                .column("f", DataTypes.FLOAT())
                                .column("d", DataTypes.DOUBLE())
                                .column("money", DataTypes.DECIMAL(20, 4))
                                .column("keep_row", DataTypes.BOOLEAN())
                                .build(),
                        ChangelogMode.all()));
        return collect(tables.executeSql("SELECT CAST(i + 256 AS SMALLINT), CAST(b AS TINYINT), "
                + "CAST(f + CAST(0.5 AS FLOAT) AS DOUBLE), d * CAST(1.5 AS DOUBLE), "
                + "CAST(money AS DECIMAL(18, 2)), money / CAST(3 AS DECIMAL(2, 0)), "
                + "i IS NULL, b IS NOT NULL, NOT keep_row, i > 7, d <= CAST(0 AS DOUBLE) "
                + "FROM numbers WHERE (i IS NULL OR i <> 0) AND (keep_row OR i < 2)"));
    }
}
