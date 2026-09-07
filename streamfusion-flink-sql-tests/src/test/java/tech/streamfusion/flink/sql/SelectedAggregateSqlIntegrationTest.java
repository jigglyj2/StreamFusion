/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.sql;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.SelectedAggregateSqlProbe;
import tech.streamfusion.flink.state.StreamFusionStateBackendFactory;

/** Actual SQL source/exchange/region/sink plumbing; production admission is tested separately. */
class SelectedAggregateSqlIntegrationTest extends SqlParityTestSupport {
    @Test
    void sqlGeneratedKeyedGlobalAndDistinctGraphsExecuteOnBothNativeBackends() throws Exception {
        for (boolean rocks : List.of(false, true))
            for (String sql : List.of(
                    "SELECT k, COUNT(*), SUM(v), MIN(v), MAX(v) FROM native_input GROUP BY k",
                    "SELECT COUNT(*), SUM(v), MIN(v), MAX(v) FROM native_input",
                    "SELECT k, COUNT(*) + 1 AS n, SUM(v + 1) AS s FROM native_input "
                            + "WHERE v IS NOT NULL GROUP BY k HAVING COUNT(*) > 0",
                    "SELECT DISTINCT k, v FROM native_input")) {
                var flink = executeGraph(sql, rocks, false);
                var originalGraph = SelectedAggregateSqlProbe.inputGraph;
                var nativeResult = executeGraph(sql, rocks, true);
                assertThat(SelectedAggregateSqlProbe.inputGraph).isEqualTo(originalGraph);
                assertThat(nativeResult).as(sql + " rocks=" + rocks).isEqualTo(flink);
                assertThat(SelectedAggregateSqlProbe.convertedGraphs).isGreaterThan(0);
                assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isGreaterThan(0);
                assertThat(StreamFusionPlannerFactory.nativeGroupAggregateBatchCount())
                        .isZero();
                SelectedAggregateSqlProbe.verifyTranslatedArrowTopology();
            }
    }

    @Test
    void sqlValuesSourcesAlsoComposeWithKeyedAndGlobalRegions() throws Exception {
        // Literal tuples remain one ordered Flink VALUES source. Expressions inside VALUES
        // can instead become independent UNION sources with nondeterministic interleaving.
        String values = "(VALUES ('é', 10), ('b', 3), ('é', 20)) AS t(k, v)";
        for (boolean rocks : List.of(false, true))
            for (String sql : List.of(
                    "SELECT k, COUNT(*), SUM(v) FROM " + values + " GROUP BY k",
                    "SELECT COUNT(*), SUM(v) FROM " + values)) {
                var flink = executeGraph(sql, rocks, false);
                var originalGraph = SelectedAggregateSqlProbe.inputGraph;
                var nativeResult = executeGraph(sql, rocks, true);
                assertThat(SelectedAggregateSqlProbe.inputGraph).isEqualTo(originalGraph);
                assertThat(nativeResult)
                        .as(sql + " rocks=" + rocks + " graph=" + originalGraph)
                        .isEqualTo(flink);
                assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isGreaterThan(0);
                assertThat(StreamFusionPlannerFactory.nativeGroupAggregateBatchCount())
                        .isZero();
                SelectedAggregateSqlProbe.verifyTranslatedArrowTopology();
            }
    }

    private static byte[] executeGraph(String sql, boolean rocks, boolean selected) throws Exception {
        return executeGraph(sql, rocks, selected, 0);
    }

    @Test
    void sqlRawMiniBatchGraphsUseConfiguredBundlesThroughTheSharedRegion() throws Exception {
        for (boolean rocks : List.of(false, true))
            for (String sql : List.of(
                    "SELECT k, COUNT(*), SUM(v), MIN(v), MAX(v) FROM native_input GROUP BY k",
                    "SELECT COUNT(*), SUM(v) FROM native_input")) {
                var flink = executeGraph(sql, rocks, false, 3);
                var graph = SelectedAggregateSqlProbe.inputGraph;
                var nativeResult = executeGraph(sql, rocks, true, 3);
                assertThat(SelectedAggregateSqlProbe.inputGraph).isEqualTo(graph);
                assertThat(nativeResult).as(sql + " rocks=" + rocks).isEqualTo(flink);
                assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isGreaterThan(0);
                assertThat(StreamFusionPlannerFactory.nativeGroupAggregateBatchCount())
                        .isZero();
                SelectedAggregateSqlProbe.verifyTranslatedArrowTopology();
            }
    }

    private static byte[] executeGraph(String sql, boolean rocks, boolean selected, long bundleSize) throws Exception {
        return executeGraph(sql, rocks, selected, bundleSize, false);
    }

    static byte[] executeGraph(String sql, boolean rocks, boolean selected, long bundleSize, boolean twoPhase)
            throws Exception {
        return executeGraph(sql, rocks, selected, bundleSize, twoPhase, null);
    }

    static byte[] executeGraph(
            String sql, boolean rocks, boolean selected, long bundleSize, boolean twoPhase, List<Row> inputRows)
            throws Exception {
        System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        StreamFusionPlannerFactory.resetMetrics();
        SelectedAggregateSqlProbe.convertedGraphs = 0;
        var environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        var tables = StreamTableEnvironment.create(environment);
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, bundleSize > 0);
        if (bundleSize > 0) {
            tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE, bundleSize);
            tables.getConfig()
                    .set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY, java.time.Duration.ofHours(1));
            tables.getConfig()
                    .set(
                            org.apache.flink.table.api.config.OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY,
                            twoPhase
                                    ? org.apache.flink.table.api.config.AggregatePhaseStrategy.TWO_PHASE
                                    : org.apache.flink.table.api.config.AggregatePhaseStrategy.ONE_PHASE);
        }
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ASYNC_STATE_ENABLED, false);
        tables.getConfig().setIdleStateRetention(java.time.Duration.ZERO);
        tables.getConfig().set(StateBackendOptions.STATE_BACKEND, rocks ? "rocksdb" : "hashmap");
        if (selected) {
            StreamFusionStateBackendFactory.install(tables.getConfig().getConfiguration());
        }
        SelectedAggregateSqlProbe.recordOnly = !selected;
        System.setProperty(
                StreamFusionPlannerFactory.EXEC_GRAPH_PROCESSOR_PROPERTY, SelectedAggregateSqlProbe.class.getName());
        var input = environment.fromCollection(
                inputRows != null
                        ? inputRows
                        : List.of(
                                Row.ofKind(RowKind.INSERT, "é", 10L),
                                Row.ofKind(RowKind.UPDATE_AFTER, "é", 20L),
                                Row.ofKind(RowKind.INSERT, null, null),
                                Row.ofKind(RowKind.INSERT, "b", 5L),
                                Row.ofKind(RowKind.UPDATE_BEFORE, "é", 20L),
                                Row.ofKind(RowKind.UPDATE_AFTER, "é", 7L),
                                Row.ofKind(RowKind.DELETE, "b", 5L),
                                Row.ofKind(RowKind.DELETE, "é", 10L),
                                Row.ofKind(RowKind.DELETE, "é", 7L),
                                Row.ofKind(RowKind.DELETE, null, null)),
                Types.ROW_NAMED(new String[] {"k", "v"}, Types.STRING, Types.LONG));
        tables.createTemporaryView(
                "native_input",
                tables.fromChangelogStream(input, Schema.newBuilder().build(), ChangelogMode.all()));
        try {
            return collect(tables.executeSql(sql));
        } finally {
            System.clearProperty(StreamFusionPlannerFactory.EXEC_GRAPH_PROCESSOR_PROPERTY);
        }
    }
}
