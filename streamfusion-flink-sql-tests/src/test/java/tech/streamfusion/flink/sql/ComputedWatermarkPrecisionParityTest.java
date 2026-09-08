/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.sql;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.AggregatePhaseStrategy;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.flink.StreamFusionPlannerFactory;

class ComputedWatermarkPrecisionParityTest extends SqlParityTestSupport {
    private static final String SQL = "SELECT MOD(k, 5), COUNT(*), window_start, window_end FROM TABLE("
            + "HOP(TABLE computed_time, DESCRIPTOR(ts), INTERVAL '2' SECOND, INTERVAL '10' SECOND)) "
            + "GROUP BY MOD(k, 5), window_start, window_end";

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 3})
    void computedTimestampPrecisionMatchesFlinkOrFallsBackBeforeExecution(int precision) throws Exception {
        for (int seed : new int[] {3, 19, 71}) {
            byte[] expected = execute(false, precision, seed);
            byte[] actual = execute(true, precision, seed);
            assertThat(actual).as("precision=%s seed=%s", precision, seed).isEqualTo(expected);
        }
    }

    private static byte[] execute(boolean accelerated, int precision, int seed) throws Exception {
        System.clearProperty(StreamFusionPlannerFactory.EXEC_GRAPH_PROCESSOR_PROPERTY);
        if (accelerated)
            System.setProperty(
                    StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, StreamFusionPlannerFactory.class.getName());
        else System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        StreamFusionPlannerFactory.resetMetrics();
        var tables = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ASYNC_STATE_ENABLED, false);
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, false);
        tables.getConfig()
                .set(OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY, AggregatePhaseStrategy.TWO_PHASE);
        String fraction = precision == 0 ? "" : precision == 1 ? ".1" : ".123";
        tables.executeSql("CREATE TABLE computed_time (k BIGINT, ts AS CASE WHEN MOD(k, " + seed
                + ") < 2 THEN TIMESTAMP '2026-01-01 00:00:00" + fraction
                + "' ELSE TIMESTAMP '2026-01-01 00:00:05" + fraction
                + "' END, WATERMARK FOR ts AS ts) WITH ('connector'='datagen', 'number-of-rows'='32', "
                + "'fields.k.kind'='sequence', 'fields.k.start'='0', 'fields.k.end'='31')");
        String explain = tables.explainSql(SQL);
        if (accelerated) {
            if (precision == 3) assertThat(explain).contains("Accelerated: yes");
            else assertThat(explain).contains("Accelerated: no", "timestamp precision conversion is not supported");
        }
        byte[] result = collect(tables.executeSql(SQL));
        if (accelerated && precision == 3)
            assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isPositive();
        else assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isZero();
        return result;
    }
}
