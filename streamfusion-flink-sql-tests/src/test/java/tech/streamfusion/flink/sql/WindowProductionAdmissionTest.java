/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.sql;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.AggregatePhaseStrategy;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.StreamFusionPlannerFactory;

class WindowProductionAdmissionTest extends SqlParityTestSupport {
    @Test
    void verifiedWindowFamiliesAreSelectedAndUnverifiedSemanticsRetainWholePlanFallback() {
        var tables = tables();
        assertThat(tables.explainSql(counts("k", "COUNT(*)")))
                .contains("Accelerated: yes", "StreamFusionGlobalWindowAggregate");
        assertThat(tables.explainSql(counts("k", "SUM(v)")))
                .contains(
                        "Accelerated: no",
                        "verified calls are non-DISTINCT BIGINT COUNT/MIN/MAX",
                        "StreamExecLocalWindowAggregate",
                        "StreamExecGlobalWindowAggregate");
        assertThat(tables.explainSql(counts("label", "COUNT(*)"))).contains("Accelerated: no", "grouping type VARCHAR");
        assertThat(tables.explainSql(counts("k", "MAX(CAST(v AS DOUBLE))")))
                .contains("Accelerated: no", "verified calls are non-DISTINCT BIGINT COUNT/MIN/MAX");
        assertThat(tables.explainSql(counts("k", "COUNT(*)")
                        .replace("HOP(TABLE", "TUMBLE(TABLE")
                        .replace("INTERVAL '2' SECOND, ", "")))
                .contains("Accelerated: no", "integral HOP size/slide");
    }

    @Test
    void sharedWindowLatencyTrackingRejectsTheCompletePlanBeforeRuntimeConstruction() {
        var tables = tables();
        String query = "WITH counts AS (" + counts("k", "COUNT(*)")
                + ") SELECT a.k, a.n FROM counts a JOIN (SELECT MAX(n) m, s, e FROM counts GROUP BY s,e) b "
                + "ON a.s=b.s AND a.e=b.e AND a.n>=b.m";
        assertThat(tables.explainSql(query)).contains("Accelerated: yes");
        tables.getConfig().set(MetricOptions.LATENCY_INTERVAL, Duration.ofSeconds(1));
        assertThat(tables.explainSql(query))
                .contains("Accelerated: no", "Shared native regions do not yet support Flink sampled latency routing")
                .doesNotContain("StreamFusionGlobalWindowAggregate");
    }

    private static String counts(String key, String call) {
        return "SELECT " + key + ", " + call + " n, window_start s, window_end e "
                + "FROM TABLE(HOP(TABLE admission_input, DESCRIPTOR(ts), INTERVAL '2' SECOND, INTERVAL '6' SECOND)) "
                + "GROUP BY " + key + ", window_start, window_end";
    }

    private static StreamTableEnvironment tables() {
        System.setProperty(
                StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, StreamFusionPlannerFactory.class.getName());
        var tables = StreamTableEnvironment.create(StreamExecutionEnvironment.getExecutionEnvironment());
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ASYNC_STATE_ENABLED, false);
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, false);
        tables.getConfig().set(OptimizerConfigOptions.TABLE_OPTIMIZER_MULTI_JOIN_ENABLED, true);
        tables.getConfig()
                .set(OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY, AggregatePhaseStrategy.TWO_PHASE);
        tables.executeSql("CREATE TABLE admission_input (k BIGINT, v BIGINT, label STRING, ts TIMESTAMP(3) NOT NULL, "
                + "WATERMARK FOR ts AS ts) WITH ('connector'='datagen', 'number-of-rows'='1')");
        return tables;
    }
}
