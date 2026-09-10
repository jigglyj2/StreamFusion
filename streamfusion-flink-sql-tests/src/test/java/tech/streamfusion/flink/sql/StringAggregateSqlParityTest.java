/*
 * Copyright 2026 StreamFusion Authors
 * Portions adapted from Apache Flink AggregateITCase, licensed under the Apache License, Version 2.0.
 */
package tech.streamfusion.flink.sql;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.flink.StreamFusionPlannerFactory;

class StringAggregateSqlParityTest extends SqlParityTestSupport {
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void upstreamBigDataOfMinMaxWithBinaryStringMatchesTheCompleteChangelog(boolean rocks) throws Exception {
        // Flink AggregateITCase.testBigDataOfMinMaxWithBinaryString, including its
        // lexicographic minimum ("12" precedes "2"). Keep the SQL and input distribution.
        var rows = new ArrayList<Row>();
        for (int i = 0; i < 100; i++) rows.add(Row.of(i % 10, (long) i, Integer.toString(i)));
        assertTypedParity(
                "SELECT a, min(b), max(c), min(c) FROM T GROUP BY a",
                Types.ROW_NAMED(new String[] {"a", "b", "c"}, Types.INT, Types.LONG, Types.STRING),
                rows,
                rocks);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void unicodeExtremaAfterTheOriginalHashExchangeMatchFlink(boolean rocks) throws Exception {
        var rows = new ArrayList<Row>();
        String[] values = {null, "", "\uE000", "\uD800\uDC00", "é", "e\u0301", "a\u0000", "a ", "z".repeat(8192)};
        for (int pass = 0; pass < 3; pass++) {
            for (int i = 0; i < values.length; i++) {
                rows.add(Row.of(i % 2 == 0 ? null : "k", "part-" + pass % 2, values[i], i % 3 != 0, (long) i));
            }
        }
        assertTypedParity(
                "SELECT k, part, MIN(v), MAX(v), MAX(v) FILTER (WHERE selected), "
                        + "COUNT(DISTINCT member_id), COUNT(*) FROM T GROUP BY k, part",
                Types.ROW_NAMED(
                        new String[] {"k", "part", "v", "selected", "member_id"},
                        Types.STRING,
                        Types.STRING,
                        Types.STRING,
                        Types.BOOLEAN,
                        Types.LONG),
                rows,
                rocks);
    }

    private static void assertTypedParity(String sql, TypeInformation<Row> type, List<Row> rows, boolean rocks)
            throws Exception {
        var expected = executeTyped(sql, type, rows, rocks, false);
        var actual = executeTyped(sql, type, rows, rocks, true);
        SqlArchitectureAssertions.admission();
        assertThat(actual).isEqualTo(expected);
    }

    private static byte[] executeTyped(
            String sql, TypeInformation<Row> type, List<Row> rows, boolean rocks, boolean nativePlan) throws Exception {
        if (nativePlan)
            System.setProperty(
                    StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, StreamFusionPlannerFactory.class.getName());
        else {
            System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
            StreamFusionPlannerFactory.resetMetrics();
        }
        var env = org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        var config = new org.apache.flink.configuration.Configuration();
        // The shared Flink test environment randomizes restore options; use the production
        // default on both engines. Non-defaults retain their existing fallback coverage.
        var ingest = org.apache.flink.state.rocksdb.RocksDBConfigurableOptions.USE_INGEST_DB_RESTORE_MODE;
        config.set(ingest, ingest.defaultValue());
        env.configure(config);
        var tables = org.apache.flink.table.api.bridge.java.StreamTableEnvironment.create(env);
        tables.getConfig()
                .set(
                        org.apache.flink.table.api.config.ExecutionConfigOptions
                                .TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM,
                        1);
        tables.getConfig()
                .set(org.apache.flink.configuration.StateBackendOptions.STATE_BACKEND, rocks ? "rocksdb" : "hashmap");
        tables.createTemporaryView("T", tables.fromDataStream(env.fromCollection(rows, type)));
        return collect(tables.executeSql(sql));
    }
}
