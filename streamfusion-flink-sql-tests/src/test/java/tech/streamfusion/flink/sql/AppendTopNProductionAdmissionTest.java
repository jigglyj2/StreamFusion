/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.sql;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.StreamFusionPlannerFactory;

class AppendTopNProductionAdmissionTest extends SqlParityTestSupport {
    @Test
    void admitsPartitionedConstantTopNAndExplainsEveryUnverifiedSubset() {
        var tables = tables();
        String sql = query("k", "score DESC NULLS LAST, ts ASC NULLS FIRST", 1);
        assertThat(tables.explainSql(sql)).contains("Accelerated: yes", "StreamFusionRank");
        for (int end : new int[] {2, 10, 64}) {
            assertThat(tables.explainSql(query("k", "score DESC", end)))
                    .contains("Accelerated: yes", "StreamFusionRank");
        }
        assertThat(tables.explainSql(query("k", "score DESC", 5) + " AND rn>=2"))
                .contains("Accelerated: yes", "StreamFusionRank");
        assertThat(tables.explainSql(query("label", "score DESC", 1)))
                .contains("Accelerated: no", "partition key type VARCHAR");
        assertThat(tables.explainSql(query("k", "floating DESC", 1))).contains("Accelerated: no", "input type DOUBLE");
        assertThat(tables.explainSql(query("", "score DESC", 1))).contains("Accelerated: no", "global rank and LIMIT");
        tables.getConfig().setIdleStateRetention(Duration.ofSeconds(1));
        assertThat(tables.explainSql(sql)).contains("Accelerated: no", "state TTL");
        tables.getConfig().setIdleStateRetention(Duration.ZERO);
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ASYNC_STATE_ENABLED, true);
        assertThat(tables.explainSql(sql)).contains("Accelerated: no", "synchronous state");
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ASYNC_STATE_ENABLED, false);
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, true);
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY, Duration.ofMillis(100));
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE, 10L);
        assertThat(tables.explainSql(sql)).contains("Accelerated: no", "mini-batching");
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, false);
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RANK_TOPN_CACHE_SIZE, 17L);
        assertThat(tables.explainSql(sql)).contains("Accelerated: no", "topn-cache-size");
    }

    private static String query(String partition, String order, int end) {
        return "SELECT k,score,ts,label FROM (SELECT k,score,ts,label,ROW_NUMBER() OVER ("
                + (partition.isEmpty() ? "" : "PARTITION BY " + partition + " ")
                + "ORDER BY " + order + ") rn FROM append_top_n_admission) WHERE rn<=" + end;
    }

    private static StreamTableEnvironment tables() {
        System.setProperty(
                StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, StreamFusionPlannerFactory.class.getName());
        var tables = StreamTableEnvironment.create(StreamExecutionEnvironment.getExecutionEnvironment());
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ASYNC_STATE_ENABLED, false);
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, false);
        tables.executeSql(
                "CREATE TABLE append_top_n_admission (k BIGINT, score BIGINT, ts TIMESTAMP(3), label STRING, floating DOUBLE) "
                        + "WITH ('connector'='datagen', 'number-of-rows'='1')");
        return tables;
    }
}
