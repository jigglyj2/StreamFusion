/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.sql;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.AggregatePhaseStrategy;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.SelectedLocalWindowSqlProbe;

class SelectedLocalWindowSqlIntegrationTest extends SqlParityTestSupport {
    @Test
    void generatedDirectAndAttachedHopGraphsMatchFlinkOnBothBackends() throws Exception {
        for (boolean rocks : List.of(false, true))
            for (boolean attached : List.of(false, true))
                for (int seed : List.of(3, 19, 71)) {
                    var flink = execute(false, rocks, attached, seed);
                    var nativeResult = execute(true, rocks, attached, seed);
                    assertThat(nativeResult)
                            .as("rocks=%s attached=%s seed=%s", rocks, attached, seed)
                            .isEqualTo(flink);
                    assertThat(StreamFusionPlannerFactory.nativePlanBatchCount())
                            .isPositive();
                    assertThat(StreamFusionPlannerFactory.nativeLocalWindowAggregateBatchCount())
                            .isZero();
                }
    }

    private static byte[] execute(boolean selected, boolean rocks, boolean attached, int seed) throws Exception {
        System.clearProperty(StreamFusionPlannerFactory.EXEC_GRAPH_PROCESSOR_PROPERTY);
        if (selected)
            System.setProperty(
                    StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, StreamFusionPlannerFactory.class.getName());
        else System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        StreamFusionPlannerFactory.resetMetrics();
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        var backend = new Configuration();
        var ingest = org.apache.flink.state.rocksdb.RocksDBConfigurableOptions.USE_INGEST_DB_RESTORE_MODE;
        backend.set(ingest, ingest.defaultValue());
        env.configure(backend);
        var tables = StreamTableEnvironment.create(env);
        if (selected)
            System.setProperty(
                    StreamFusionPlannerFactory.EXEC_GRAPH_PROCESSOR_PROPERTY,
                    SelectedLocalWindowSqlProbe.class.getName());
        tables.getConfig().set(StateBackendOptions.STATE_BACKEND, rocks ? "rocksdb" : "hashmap");
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ASYNC_STATE_ENABLED, false);
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, false);
        tables.getConfig()
                .set(OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY, AggregatePhaseStrategy.TWO_PHASE);
        var random = new java.util.Random(seed);
        var rows = new ArrayList<Row>();
        for (int i = 0; i < 1025; i++) {
            long millis = -9000 + random.nextInt(18000);
            var time = LocalDateTime.ofEpochSecond(
                    Math.floorDiv(millis, 1000), (int) Math.floorMod(millis, 1000) * 1_000_000, ZoneOffset.UTC);
            rows.add(Row.of(i % 11 == 0 ? null : (long) random.nextInt(17), time));
        }
        var input =
                env.fromCollection(rows, Types.ROW_NAMED(new String[] {"k", "ts"}, Types.LONG, Types.LOCAL_DATE_TIME));
        tables.createTemporaryView(
                "window_input",
                input,
                Schema.newBuilder()
                        .column("k", DataTypes.BIGINT())
                        .column("ts", DataTypes.TIMESTAMP(3).notNull())
                        .watermark("ts", "ts")
                        .build());
        String counts = "SELECT k, COUNT(*) n, window_start s, window_end e "
                + "FROM TABLE(HOP(TABLE window_input, DESCRIPTOR(ts), INTERVAL '2' SECOND, INTERVAL '6' SECOND)) "
                + "GROUP BY k, window_start, window_end";
        String sql = attached ? "SELECT MAX(n) m, s, e FROM (" + counts + ") counts GROUP BY s, e" : counts;
        try {
            return collect(tables.executeSql(sql));
        } finally {
            System.clearProperty(StreamFusionPlannerFactory.EXEC_GRAPH_PROCESSOR_PROPERTY);
        }
    }
}
