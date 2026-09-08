/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.sql;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

class GroupAggregateAdmissionParityTest extends SqlParityTestSupport {
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void ordinaryKeyedIntegerFiltersAndRetractionsMatchFlinkByteForByte(boolean rocks) throws Exception {
        byte[] flink = executeFilteredRetractions(false, rocks);
        byte[] streamFusion = executeFilteredRetractions(true, rocks);
        assertThat(streamFusion).isEqualTo(flink);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
        assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isPositive();
    }

    private static byte[] executeFilteredRetractions(boolean streamFusion, boolean rocks) throws Exception {
        configurePlanner(streamFusion);
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        // Flink's TestStreamEnvironment randomizes this restore option. Exercise the supported
        // production default on both engines; configured non-defaults have separate fallback tests.
        var config = new org.apache.flink.configuration.Configuration();
        var ingest = org.apache.flink.state.rocksdb.RocksDBConfigurableOptions.USE_INGEST_DB_RESTORE_MODE;
        config.set(ingest, ingest.defaultValue());
        environment.configure(config);
        StreamTableEnvironment tables = StreamTableEnvironment.create(
                environment, EnvironmentSettings.newInstance().inStreamingMode().build());
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
        tables.getConfig()
                .set(org.apache.flink.configuration.StateBackendOptions.STATE_BACKEND, rocks ? "rocksdb" : "hashmap");
        Row selected = Row.of("a", 5L, true);
        Row rejected = Row.of("a", 7L, false);
        Row selectedNull = Row.of("a", null, true);
        Row nullPredicate = Row.of("a", 2L, null);
        DataStream<Row> changes = environment.fromCollection(
                List.of(
                        withKind(selected, RowKind.INSERT),
                        withKind(rejected, RowKind.INSERT),
                        withKind(selectedNull, RowKind.INSERT),
                        withKind(nullPredicate, RowKind.INSERT),
                        withKind(selected, RowKind.DELETE),
                        withKind(rejected, RowKind.DELETE),
                        withKind(selectedNull, RowKind.DELETE),
                        withKind(nullPredicate, RowKind.DELETE)),
                Types.ROW_NAMED(
                        new String[] {"category", "amount", "selected"}, Types.STRING, Types.LONG, Types.BOOLEAN));
        tables.createTemporaryView(
                "filtered_aggregate_input",
                tables.fromChangelogStream(
                        changes,
                        Schema.newBuilder()
                                .column("category", "STRING NOT NULL")
                                .column("amount", "BIGINT")
                                .column("selected", "BOOLEAN")
                                .build()));
        return collect(tables.executeSql("SELECT category, "
                + "COUNT(*) FILTER (WHERE selected), COUNT(amount) FILTER (WHERE selected), "
                + "SUM(amount) FILTER (WHERE selected), AVG(amount) FILTER (WHERE selected), "
                + "MIN(amount) FILTER (WHERE selected), "
                + "MAX(amount) FILTER (WHERE selected) "
                + "FROM filtered_aggregate_input GROUP BY category"));
    }

    private static void configurePlanner(boolean streamFusion) {
        if (streamFusion) {
            System.setProperty(
                    StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, StreamFusionPlannerFactory.class.getName());
        } else {
            System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
            StreamFusionPlannerFactory.resetMetrics();
        }
    }

    private static Row withKind(Row source, RowKind kind) {
        Row copy = Row.copy(source);
        copy.setKind(kind);
        return copy;
    }
}
