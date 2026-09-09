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

class ScaledIntegerDecimalParityTest extends SqlParityTestSupport {
    @ParameterizedTest
    @ValueSource(longs = {3, 19, 71})
    void decimalScalingAndRangePredicatesPreserveGeneratedChangelogs(long seed) throws Exception {
        var rows = new ArrayList<Row>();
        var random = new Random(seed);
        long[] edges = {
            Long.MIN_VALUE, Long.MAX_VALUE, -1, 0, 1, 1_101_321, 1_101_322, 55_066_079, 55_066_080, 10_000_000
        };
        for (int i = 0; i < 257; i++) {
            Long value = i % 17 == 16
                    ? null
                    : i < edges.length
                            ? edges[i]
                            : i % 2 == 0 ? random.nextLong() : Math.floorMod(random.nextLong(), 60_000_000);
            for (RowKind kind : RowKind.values()) rows.add(Row.ofKind(kind, (long) i, value));
        }
        for (boolean filter : List.of(false, true)) {
            byte[] expected = execute(rows, false, filter);
            byte[] actual = execute(rows, true, filter);
            assertThat(actual).isNotEmpty().isEqualTo(expected);
            assertThat(StreamFusionPlannerFactory.nativePlanBatchCount())
                    .withFailMessage(StreamFusionPlanningDiagnostics.explain())
                    .isGreaterThan(0);
            SqlArchitectureAssertions.admission();
        }
    }

    private static byte[] execute(List<Row> rows, boolean enabled, boolean filter) throws Exception {
        System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        StreamFusionPlannerFactory.resetMetrics();
        if (enabled)
            System.setProperty(
                    StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, StreamFusionPlannerFactory.class.getName());
        var environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        var tables = StreamTableEnvironment.create(environment);
        tables.createTemporaryView(
                "scale_input",
                tables.fromChangelogStream(
                        environment.fromCollection(
                                rows, Types.ROW_NAMED(new String[] {"id", "price"}, Types.LONG, Types.LONG)),
                        Schema.newBuilder()
                                .column("id", DataTypes.BIGINT())
                                .column("price", DataTypes.BIGINT())
                                .build(),
                        ChangelogMode.all()));
        // Original Q14 scaling and range, tested separately from its unresolved Java UDF.
        // Exercise both the full signed input domain and the non-empty selected changelog.
        String sql = "SELECT id, CAST(0.908 * price AS DECIMAL(23, 3)), "
                + "0.908 * price > 1000000, 0.908 * price < 50000000 FROM scale_input WHERE "
                + (filter ? "0.908 * price > 1000000 AND 0.908 * price < 50000000" : "id >= 0");
        var result = tables.executeSql(sql);
        var type = result.getResolvedSchema().toPhysicalRowDataType();
        var converter = org.apache.flink.table.data.conversion.DataStructureConverters.getConverter(type);
        converter.open(ScaledIntegerDecimalParityTest.class.getClassLoader());
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
