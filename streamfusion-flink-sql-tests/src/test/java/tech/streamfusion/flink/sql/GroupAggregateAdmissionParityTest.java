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

    @ParameterizedTest
    @org.junit.jupiter.params.provider.CsvSource({"0.2,0.2", "0.5,0.1", "0.7,0.2"})
    void configuredRocksDbGeometryPreservesCompleteChangelog(double write, double high) throws Exception {
        for (boolean rocks : new boolean[] {false, true}) {
            byte[] flink = executeFilteredRetractions(false, rocks, write, high);
            byte[] nativeBytes = executeFilteredRetractions(true, rocks, write, high);
            assertThat(nativeBytes).isEqualTo(flink);
            assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
            assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isPositive();
        }
    }

    private static byte[] executeFilteredRetractions(boolean streamFusion, boolean rocks) throws Exception {
        return executeFilteredRetractions(streamFusion, rocks, 0.5, 0.1);
    }

    private static byte[] executeFilteredRetractions(boolean streamFusion, boolean rocks, double write, double high)
            throws Exception {
        return executeFilteredRetractions(
                streamFusion,
                rocks,
                write,
                high,
                tech.streamfusion.flink.planner.RocksDbConfigurationProfiles.databaseOptions(write < 0.5 ? 1 : 2));
    }

    @ParameterizedTest
    @org.junit.jupiter.params.provider.CsvSource({
        "FLASH_SSD_OPTIMIZED,false",
        "SPINNING_DISK_OPTIMIZED,false",
        "SPINNING_DISK_OPTIMIZED,true",
        "SPINNING_DISK_OPTIMIZED_HIGH_MEM,false",
        "SPINNING_DISK_OPTIMIZED_HIGH_MEM,true"
    })
    void configuredPresetsPreserveTheCompleteChangelog(String preset, boolean override) throws Exception {
        var config = tech.streamfusion.flink.planner.RocksDbConfigurationProfiles.presetOptions(preset, override);
        var expected = executeFilteredRetractions(false, true, 0.7, 0.2, config);
        var actual = executeFilteredRetractions(true, true, 0.7, 0.2, config);
        assertThat(actual).isEqualTo(expected);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
        assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isPositive();
    }

    @ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(ints = {0, 1, 2, 3})
    void configuredTableOptionsPreserveGeneratedRetractionsOnBothBackends(int profile) throws Exception {
        for (boolean rocks : new boolean[] {false, true}) {
            var config = tech.streamfusion.flink.planner.RocksDbConfigurationProfiles.tableOptions(profile);
            var expected = executeFilteredRetractions(false, rocks, 0.7, 0.2, config);
            var actual = executeFilteredRetractions(true, rocks, 0.7, 0.2, config);
            assertThat(actual).isEqualTo(expected);
            assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
            assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isPositive();
        }
    }

    @ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(ints = {0, 1, 2, 3, 4})
    void partitionedIndexesAndCompactionStylesPreserveGeneratedRetractions(int profile) throws Exception {
        for (boolean rocks : new boolean[] {false, true}) {
            var config = tech.streamfusion.flink.planner.RocksDbConfigurationProfiles.indexOptions(profile);
            var expected = executeFilteredRetractions(false, rocks, 0.7, 0.2, config);
            var actual = executeFilteredRetractions(true, rocks, 0.7, 0.2, config);
            assertThat(actual).isEqualTo(expected);
            assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
            assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isPositive();
        }
    }

    @org.junit.jupiter.api.io.TempDir
    java.nio.file.Path logRoot;

    @ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(ints = {0, 1, 2})
    void explicitLogDirectoriesPreserveGeneratedRetractionsAndRetainNativeLogs(int profile) throws Exception {
        for (boolean rocks : new boolean[] {false, true}) {
            var logs = logRoot.resolve("日志-" + profile + "-" + rocks);
            if (profile == 0) java.nio.file.Files.createDirectory(logs);
            var config = tech.streamfusion.flink.planner.RocksDbConfigurationProfiles.indexOptions(profile);
            config.set(org.apache.flink.state.rocksdb.RocksDBConfigurableOptions.LOG_DIR, logs.toString());
            var stateParent = logRoot;
            if (profile == 2) {
                var physical = java.nio.file.Files.createDirectories(
                        logRoot.resolve("physical-" + rocks).resolve("child"));
                var alias = java.nio.file.Files.createSymbolicLink(logRoot.resolve("alias-" + rocks), physical);
                stateParent = alias.resolve("..");
            }
            var stateA = stateParent.resolve("state-a-" + profile + "-" + rocks);
            var stateB = stateParent.resolve("state-b-" + profile + "-" + rocks);
            var unusable =
                    java.nio.file.Files.writeString(logRoot.resolve("unusable-" + profile + "-" + rocks), "user data");
            config.set(
                    org.apache.flink.state.rocksdb.RocksDBOptions.LOCAL_DIRECTORIES,
                    profile == 0
                            ? stateA.toString()
                            : stateA
                                    + (profile == 1 ? "," : java.io.File.pathSeparator)
                                    + unusable
                                    + (profile == 1 ? "," : java.io.File.pathSeparator)
                                    + stateB);
            var expected = executeFilteredRetractions(false, rocks, 0.7, 0.2, config);
            long flinkLogs = rocks ? logCount(logs) : 0;
            var actual = executeFilteredRetractions(true, rocks, 0.7, 0.2, config);
            assertThat(actual).isEqualTo(expected);
            assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
            assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isPositive();
            if (rocks) {
                assertThat(logCount(logs)).isGreaterThan(flinkLogs);
                try (var files = java.nio.file.Files.list(logs)) {
                    var nativeLogs = files.map(path -> path.getFileName().toString())
                            .filter(name -> name.contains("streamfusion-region-state-"))
                            .collect(java.util.stream.Collectors.toList());
                    assertThat(nativeLogs).isNotEmpty();
                    assertThat(nativeLogs).allMatch(name -> name.contains("_state-a-") || name.contains("_state-b-"));
                }
                for (var stateRoot : java.util.List.of(stateA, stateB)) {
                    if (java.nio.file.Files.exists(stateRoot)) {
                        try (var files = java.nio.file.Files.list(stateRoot)) {
                            assertThat(files.count()).isZero();
                        }
                    }
                }
            }
            assertThat(java.nio.file.Files.readString(unusable)).isEqualTo("user data");
        }
    }

    private static long logCount(java.nio.file.Path directory) throws Exception {
        try (var files = java.nio.file.Files.list(directory)) {
            return files.filter(path -> path.getFileName().toString().endsWith("_LOG"))
                    .count();
        }
    }

    @ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(ints = {-2, -1, 0, 1, 2, 4})
    void configuredCheckpointTransferExecutorsPreserveGeneratedChangelogs(int threads) throws Exception {
        var config = tech.streamfusion.flink.planner.RocksDbConfigurationProfiles.indexOptions(Math.abs(threads) % 4);
        config.set(org.apache.flink.state.rocksdb.RocksDBOptions.CHECKPOINT_TRANSFER_THREAD_NUM, threads);
        for (boolean rocks : new boolean[] {false, true}) {
            var expected = executeFilteredRetractions(false, rocks, 0.7, 0.2, config);
            var actual = executeFilteredRetractions(true, rocks, 0.7, 0.2, config);
            assertThat(actual).isEqualTo(expected);
            assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
            assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isPositive();
        }
    }

    @org.junit.jupiter.api.Test
    void fifoCompactionRetainsTheCompleteFlinkPlanUntilEvictionParityIsVerified() throws Exception {
        var config = new org.apache.flink.configuration.Configuration();
        config.set(
                org.apache.flink.state.rocksdb.RocksDBConfigurableOptions.COMPACTION_STYLE,
                org.rocksdb.CompactionStyle.FIFO);
        var expected = executeFilteredRetractions(false, true, 0.5, 0.1, config);
        var actual = executeFilteredRetractions(true, true, 0.5, 0.1, config);
        assertThat(actual).isEqualTo(expected);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: no", "compaction.style=FIFO");
        assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isZero();
    }

    static byte[] executeFilteredRetractions(
            boolean streamFusion,
            boolean rocks,
            double write,
            double high,
            org.apache.flink.configuration.Configuration config)
            throws Exception {
        configurePlanner(streamFusion);
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        // Flink's TestStreamEnvironment randomizes this restore option. Exercise the supported
        // production default unless a configuration-parity case supplies an explicit value.
        var ingest = org.apache.flink.state.rocksdb.RocksDBConfigurableOptions.USE_INGEST_DB_RESTORE_MODE;
        if (config.getOptional(ingest).isEmpty()) config.set(ingest, ingest.defaultValue());
        config.set(org.apache.flink.state.rocksdb.RocksDBOptions.WRITE_BUFFER_RATIO, write);
        config.set(org.apache.flink.state.rocksdb.RocksDBOptions.HIGH_PRIORITY_POOL_RATIO, high);
        environment.configure(config);
        StreamTableEnvironment tables = StreamTableEnvironment.create(
                environment, EnvironmentSettings.newInstance().inStreamingMode().build());
        tables.getConfig().addConfiguration(config);
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
        tables.getConfig()
                .set(org.apache.flink.configuration.StateBackendOptions.STATE_BACKEND, rocks ? "rocksdb" : "hashmap");
        tables.getConfig().set(org.apache.flink.state.rocksdb.RocksDBOptions.WRITE_BUFFER_RATIO, write);
        tables.getConfig().set(org.apache.flink.state.rocksdb.RocksDBOptions.HIGH_PRIORITY_POOL_RATIO, high);
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
