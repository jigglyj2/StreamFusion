/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.runner;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.AggregatePhaseStrategy;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.types.Row;
import tech.streamfusion.flink.StreamFusionPlannerFactory;

/** Exercises shared HOP/attached-MAX ownership through the distribution's isolated planner loader. */
final class SharedWindowRunnerFixture {
    private static final String SQL = "WITH counts AS (SELECT MOD(k, 5) k, COUNT(*) n, window_start s, window_end e "
            + "FROM TABLE(HOP(TABLE shared_window_input, DESCRIPTOR(ts), INTERVAL '2' SECOND, INTERVAL '10' SECOND)) "
            + "GROUP BY MOD(k, 5), window_start, window_end) "
            + "SELECT a.k, a.n, a.s, a.e FROM counts a JOIN "
            + "(SELECT MAX(n) m, s, e FROM counts GROUP BY s, e) b "
            + "ON a.s=b.s AND a.e=b.e AND a.n>=b.m";

    private SharedWindowRunnerFixture() {}

    static void verify(TableEnvironment accelerated) throws Exception {
        configure(accelerated);
        for (String backend : List.of("hashmap", "rocksdb")) {
            List<Row> expected;
            long before = StreamFusionPlannerFactory.nativePlanBatchCount();
            String processor = System.getProperty(StreamFusionPlannerFactory.EXEC_GRAPH_PROCESSOR_PROPERTY);
            System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
            System.clearProperty(StreamFusionPlannerFactory.EXEC_GRAPH_PROCESSOR_PROPERTY);
            try {
                var flink = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
                configure(flink);
                flink.getConfig().set(StateBackendOptions.STATE_BACKEND, backend);
                expected = collect(flink);
            } finally {
                if (processor == null) System.clearProperty(StreamFusionPlannerFactory.EXEC_GRAPH_PROCESSOR_PROPERTY);
                else System.setProperty(StreamFusionPlannerFactory.EXEC_GRAPH_PROCESSOR_PROPERTY, processor);
                System.setProperty(
                        StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, StreamFusionPlannerFactory.class.getName());
            }
            if (StreamFusionPlannerFactory.nativePlanBatchCount() != before)
                throw new IllegalStateException("The shared-window Flink baseline executed native work");
            accelerated.getConfig().set(StateBackendOptions.STATE_BACKEND, backend);
            String explain = accelerated.explainSql(SQL);
            if (!explain.contains("Accelerated: yes"))
                throw new IllegalStateException("Packaged shared-window plan fell back on " + backend + "\n" + explain);
            long localBefore = StreamFusionPlannerFactory.nativeLocalWindowAggregateBatchCount();
            var actual = collect(accelerated);
            if (expected.isEmpty() || !actual.equals(expected))
                throw new IllegalStateException("Packaged shared-window parity failed on " + backend + ": expected="
                        + expected + ", actual=" + actual);
            if (StreamFusionPlannerFactory.nativePlanBatchCount() <= before
                    || StreamFusionPlannerFactory.nativeLocalWindowAggregateBatchCount() != localBefore)
                throw new IllegalStateException("Shared windows did not use the common native runtime on " + backend);
        }
    }

    private static void configure(TableEnvironment tables) {
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 2);
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ASYNC_STATE_ENABLED, false);
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, false);
        tables.getConfig().set(OptimizerConfigOptions.TABLE_OPTIMIZER_MULTI_JOIN_ENABLED, true);
        tables.getConfig()
                .set(OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY, AggregatePhaseStrategy.TWO_PHASE);
        tables.executeSql("CREATE TABLE shared_window_input (k BIGINT, "
                + "ts AS CASE WHEN k < 16 THEN TIMESTAMP '2026-01-01 00:00:00.001' ELSE TIMESTAMP '2026-01-01 00:00:05.001' END, WATERMARK FOR ts AS ts) "
                + "WITH ('connector'='datagen', 'number-of-rows'='32', "
                + "'fields.k.kind'='sequence', 'fields.k.start'='0', 'fields.k.end'='31')");
    }

    private static List<Row> collect(TableEnvironment tables) throws Exception {
        var result = new ArrayList<Row>();
        try (var rows = tables.executeSql(SQL).collect()) {
            while (rows.hasNext()) result.add(rows.next());
        }
        result.sort(Comparator.comparing(Row::toString));
        return result;
    }
}
