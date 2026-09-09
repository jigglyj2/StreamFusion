/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.sql;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

/** Ordinary planner selection with generated multi-argument/filter changelogs on both backends. */
class DistinctCountAdmissionParityTest extends SqlParityTestSupport {
    @ParameterizedTest
    @CsvSource({"false,3", "true,3", "false,29", "true,29", "false,197", "true,197"})
    void keyedBigintDistinctCountsAccelerateAndMatchEveryChangelogRecord(boolean rocks, long seed) throws Exception {
        var rows = generated(seed);
        String sql = "SELECT k, COUNT(*), COUNT(DISTINCT a), COUNT(DISTINCT b), "
                + "COUNT(DISTINCT a) FILTER (WHERE selected), COUNT(DISTINCT a) FILTER (WHERE selected IS NOT TRUE), "
                + "COUNT(DISTINCT b) FILTER (WHERE a >= 0 AND a < 8), SUM(a) "
                + "FROM distinct_input WHERE b IS NULL OR b > -100 GROUP BY k";
        byte[] expected = encoded(tables(rows, false, rocks).executeSql(sql));
        byte[] actual = encoded(tables(rows, true, rocks).executeSql(sql));
        assertThat(actual).isNotEmpty().isEqualTo(expected);
        SqlArchitectureAssertions.admission();
        assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isPositive();
        assertThat(StreamFusionPlannerFactory.nativeGroupAggregateBatchCount()).isZero();
    }

    @ParameterizedTest
    @CsvSource({
        "'SELECT k, SUM(DISTINCT a) FROM distinct_input GROUP BY k', 'non-DISTINCT BIGINT SUM'",
        "'SELECT k, AVG(DISTINCT a) FROM distinct_input GROUP BY k', 'non-DISTINCT BIGINT SUM'",
        "'SELECT COUNT(DISTINCT a) FROM distinct_input', 'singleton/global aggregation'",
        "'SELECT a, COUNT(DISTINCT k) FROM distinct_input GROUP BY a', 'aggregate argument type STRING'"
    })
    void unverifiedDistinctSemanticsRetainWholePlanFallback(String sql, String reason) throws Exception {
        var rows = generated(3);
        byte[] expected = encoded(tables(rows, false, false).executeSql(sql));
        byte[] actual = encoded(tables(rows, true, false).executeSql(sql));
        assertThat(actual).isEqualTo(expected);
        SqlFallbackAssertions.unaccelerated();
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains(reason);
    }

    private static List<Row> generated(long seed) {
        var random = new Random(seed);
        var values = new ArrayList<Row>();
        for (int index = 0; index < 257; index++)
            values.add(Row.of(
                    index % 11 == 0 ? null : "é\u0000-" + random.nextInt(7),
                    index % 13 == 0 ? null : (long) random.nextInt(17) - 8,
                    index % 7 == 0 ? null : (long) random.nextInt(11),
                    index % 17 == 0 ? null : index % 3 != 0));
        var rows = new ArrayList<Row>();
        for (var kind : List.of(RowKind.INSERT, RowKind.UPDATE_AFTER, RowKind.UPDATE_BEFORE, RowKind.DELETE))
            for (var value : values) {
                var copy = Row.copy(value);
                copy.setKind(kind);
                rows.add(copy);
            }
        return rows;
    }

    private static StreamTableEnvironment tables(List<Row> rows, boolean enabled, boolean rocks) {
        System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        StreamFusionPlannerFactory.resetMetrics();
        if (enabled)
            System.setProperty(
                    StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, StreamFusionPlannerFactory.class.getName());
        var environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        var config = new Configuration();
        var ingest = org.apache.flink.state.rocksdb.RocksDBConfigurableOptions.USE_INGEST_DB_RESTORE_MODE;
        config.set(ingest, ingest.defaultValue());
        environment.configure(config);
        var tables = StreamTableEnvironment.create(environment);
        tables.getConfig().set(StateBackendOptions.STATE_BACKEND, rocks ? "rocksdb" : "hashmap");
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, false);
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
        tables.createTemporaryView(
                "distinct_input",
                tables.fromChangelogStream(
                        environment.fromCollection(
                                rows,
                                Types.ROW_NAMED(
                                        new String[] {"k", "a", "b", "selected"},
                                        Types.STRING,
                                        Types.LONG,
                                        Types.LONG,
                                        Types.BOOLEAN)),
                        Schema.newBuilder().build(),
                        ChangelogMode.all()));
        return tables;
    }

    private static byte[] encoded(TableResult result) throws Exception {
        var type = result.getResolvedSchema().toPhysicalRowDataType();
        var converter = DataStructureConverters.getConverter(type);
        converter.open(DistinctCountAdmissionParityTest.class.getClassLoader());
        var serializer = new RowDataSerializer((RowType) type.getLogicalType());
        var rows = new ArrayList<byte[]>();
        try (var output = result.collect()) {
            while (output.hasNext()) {
                var row = output.next();
                var internal = (RowData) converter.toInternal(row);
                internal.setRowKind(row.getKind());
                var bytes = new DataOutputSerializer(128);
                serializer.serialize(internal, bytes);
                rows.add(bytes.getCopyOfBuffer());
            }
        }
        // Independent network jobs may interleave keys; direct-runtime tests verify per-key order.
        rows.sort(Arrays::compareUnsigned);
        var bytes = new DataOutputSerializer(128);
        for (var row : rows) {
            bytes.writeInt(row.length);
            bytes.write(row);
        }
        return bytes.getCopyOfBuffer();
    }
}
