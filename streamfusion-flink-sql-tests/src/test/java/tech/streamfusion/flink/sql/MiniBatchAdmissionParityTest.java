/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.sql;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.AggregatePhaseStrategy;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

/** Whole-job production selection; no test graph processor or architecture admission override. */
class MiniBatchAdmissionParityTest extends SqlParityTestSupport {
    private static final String QUALIFIED =
            "SELECT k, COUNT(*), COUNT(v), SUM(v), MIN(v), MAX(v), AVG(v) " + "FROM mini_input GROUP BY k";

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void ordinaryMiniBatchSqlPreservesGeneratedCompleteChangelogs(boolean rocks) throws Exception {
        for (boolean retract : List.of(false, true))
            for (long trigger : List.of(1L, 7L, 1000L)) {
                var expected = execute(false, rocks, retract, trigger, AggregatePhaseStrategy.ONE_PHASE, QUALIFIED);
                var actual = execute(true, rocks, retract, trigger, AggregatePhaseStrategy.ONE_PHASE, QUALIFIED);
                assertThat(actual)
                        .as("rocks=%s retract=%s trigger=%s", rocks, retract, trigger)
                        .isEqualTo(expected);
                assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
                assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isPositive();
                assertThat(StreamFusionPlannerFactory.nativeGroupAggregateBatchCount())
                        .isZero();
            }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void unqualifiedMiniBatchShapesRetainCompleteFlinkFallback(boolean rocks) throws Exception {
        for (String sql : List.of(
                "SELECT COUNT(*), SUM(v) FROM mini_input",
                "SELECT k, COUNT(DISTINCT v) FROM mini_input GROUP BY k",
                "SELECT k, SUM(v) FILTER (WHERE v > 0) FROM mini_input GROUP BY k",
                "SELECT v, MIN(k) FROM mini_input GROUP BY v")) {
            var expected = execute(false, rocks, true, 7, AggregatePhaseStrategy.ONE_PHASE, sql);
            var actual = execute(true, rocks, true, 7, AggregatePhaseStrategy.ONE_PHASE, sql);
            assertThat(actual).as(sql).isEqualTo(expected);
            assertThat(StreamFusionPlanningDiagnostics.explain())
                    .contains("Accelerated: no", "aggregate persistent admission");
            assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isZero();
        }
        var expected = execute(false, rocks, true, 7, AggregatePhaseStrategy.TWO_PHASE, QUALIFIED);
        var actual = execute(true, rocks, true, 7, AggregatePhaseStrategy.TWO_PHASE, QUALIFIED);
        assertThat(actual).isEqualTo(expected);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: no");
        assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isZero();
    }

    private static byte[] execute(
            boolean nativeEngine,
            boolean rocks,
            boolean retract,
            long trigger,
            AggregatePhaseStrategy phase,
            String sql)
            throws Exception {
        StreamFusionPlannerFactory.resetMetrics();
        if (nativeEngine)
            System.setProperty(
                    StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, StreamFusionPlannerFactory.class.getName());
        else System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        var options = new Configuration();
        var ingest = org.apache.flink.state.rocksdb.RocksDBConfigurableOptions.USE_INGEST_DB_RESTORE_MODE;
        options.set(ingest, ingest.defaultValue());
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.configure(options);
        var tables = StreamTableEnvironment.create(env);
        tables.getConfig().addConfiguration(options);
        tables.getConfig().set(StateBackendOptions.STATE_BACKEND, rocks ? "rocksdb" : "hashmap");
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ASYNC_STATE_ENABLED, false);
        tables.getConfig().setIdleStateRetention(Duration.ZERO);
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, true);
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE, trigger);
        // Count/end controls are deterministic across separate jobs; clock-marker placement
        // is compared under identical samples by SharedMiniBatchAssignerRegionTest.
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY, Duration.ofDays(1));
        tables.getConfig().set(OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY, phase);
        var rows = new ArrayList<Row>();
        var live = new ArrayList<Row>();
        var random = new Random(61);
        for (int i = 0; i < 83; i++) {
            if (retract && !live.isEmpty() && random.nextBoolean()) {
                var row = live.remove(random.nextInt(live.size()));
                row.setKind(i % 2 == 0 ? RowKind.DELETE : RowKind.UPDATE_BEFORE);
                rows.add(row);
            } else {
                Long value = i % 9 == 0
                        ? null
                        : i % 7 == 0 ? Long.MAX_VALUE : i % 11 == 0 ? Long.MIN_VALUE : (long) random.nextInt(17) - 8;
                var row = Row.of(i % 6 == 0 ? null : "é-" + random.nextInt(7), value);
                live.add(Row.copy(row));
                if (retract && i % 2 == 0) row.setKind(RowKind.UPDATE_AFTER);
                rows.add(row);
            }
        }
        var input = env.fromCollection(rows, Types.ROW_NAMED(new String[] {"k", "v"}, Types.STRING, Types.LONG));
        tables.createTemporaryView(
                "mini_input",
                tables.fromChangelogStream(
                        input,
                        Schema.newBuilder().build(),
                        retract ? ChangelogMode.all() : ChangelogMode.insertOnly()));
        return collect(tables.executeSql(sql));
    }
}
