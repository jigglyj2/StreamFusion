/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.sql;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.StreamFusionPlannerFactory;

class SelectedTopOneSqlIntegrationTest extends SqlParityTestSupport {
    @Test
    void generatedTopOneMatchesFlinkThroughOrdinarySelectionOnBothBackends() throws Exception {
        for (boolean rocks : List.of(false, true))
            for (int seed : List.of(3, 19, 71)) {
                var expected = execute(false, rocks, seed);
                var actual = execute(true, rocks, seed);
                assertThat(actual)
                        .as("rocks=%s seed=%s", rocks, seed)
                        .isNotEmpty()
                        .isEqualTo(expected);
                assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isPositive();
                assertThat(StreamFusionPlannerFactory.nativeTopNBatchCount()).isZero();
            }
    }

    private static byte[] execute(boolean selected, boolean rocks, int seed) throws Exception {
        System.clearProperty(StreamFusionPlannerFactory.EXEC_GRAPH_PROCESSOR_PROPERTY);
        if (selected)
            System.setProperty(
                    StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, StreamFusionPlannerFactory.class.getName());
        else System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        StreamFusionPlannerFactory.resetMetrics();
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(seed == 71 ? 2 : 1);
        var config = new Configuration();
        var ingest = org.apache.flink.state.rocksdb.RocksDBConfigurableOptions.USE_INGEST_DB_RESTORE_MODE;
        config.set(ingest, ingest.defaultValue());
        env.configure(config);
        var tables = StreamTableEnvironment.create(env);
        tables.getConfig().set(StateBackendOptions.STATE_BACKEND, rocks ? "rocksdb" : "hashmap");
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ASYNC_STATE_ENABLED, false);
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, false);
        var random = new Random(seed);
        var rows = new ArrayList<Row>();
        for (int row = 0; row < 1025; row++) {
            long millis = random.nextInt(17) - 8;
            var time = LocalDateTime.ofEpochSecond(
                    Math.floorDiv(millis, 1000), (int) Math.floorMod(millis, 1000) * 1_000_000, ZoneOffset.UTC);
            rows.add(Row.of(
                    row % 7 == 0 ? null : (long) random.nextInt(31),
                    row % 13 == 0 ? null : (long) random.nextInt(23) - 11,
                    row % 5 == 0 ? null : time,
                    "界é-" + row + "x".repeat(row % 29)));
        }
        var input = env.fromCollection(
                rows,
                Types.ROW_NAMED(
                        new String[] {"k", "score", "ts", "label"},
                        Types.LONG,
                        Types.LONG,
                        Types.LOCAL_DATE_TIME,
                        Types.STRING));
        tables.createTemporaryView(
                "top_one_input",
                input,
                Schema.newBuilder()
                        .column("k", DataTypes.BIGINT())
                        .column("score", DataTypes.BIGINT())
                        .column("ts", DataTypes.TIMESTAMP(3))
                        .column("label", DataTypes.STRING())
                        .build());
        String sql = "SELECT k,score,ts,label" + (seed == 19 ? ",rn" : "")
                + " FROM (SELECT *,ROW_NUMBER() OVER (PARTITION BY k ORDER BY score " + (seed == 3 ? "ASC" : "DESC")
                + " NULLS LAST,ts ASC NULLS FIRST) rn FROM top_one_input) WHERE rn<=1";
        if (selected) assertThat(tables.explainSql(sql)).contains("Accelerated: yes", "StreamFusionRank");
        return collect(tables.executeSql(sql));
    }
}
