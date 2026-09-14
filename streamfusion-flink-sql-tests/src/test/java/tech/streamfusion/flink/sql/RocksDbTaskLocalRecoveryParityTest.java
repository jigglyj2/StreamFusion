/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.sql;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

/** Real tasks and TaskManager local stores, with ordinary SQL selection and source/sink recovery. */
class RocksDbTaskLocalRecoveryParityTest {
    @TempDir
    Path temporary;

    @ParameterizedTest
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    @org.junit.jupiter.api.Timeout(180)
    void taskRestartUsesLocalStateAndRetriesRemoteWithoutChangingTheChangelog(boolean incremental, boolean unaligned)
            throws Exception {
        var options = tech.streamfusion.flink.planner.RocksDbDormantCompactionProfiles.disabled(unaligned ? 999999 : 0);
        options.set(StateBackendOptions.STATE_BACKEND, "rocksdb");
        options.set(StateRecoveryOptions.LOCAL_RECOVERY, true);
        options.set(org.apache.flink.configuration.RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        options.set(org.apache.flink.configuration.RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 1);
        options.set(
                org.apache.flink.configuration.RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY,
                Duration.ofMillis(10));
        options.set(
                CheckpointingOptions.LOCAL_RECOVERY_TASK_MANAGER_STATE_ROOT_DIRS,
                temporary.resolve("backup").toString());
        options.set(
                CheckpointingOptions.CHECKPOINTS_DIRECTORY,
                temporary.resolve("remote").toUri().toString());
        options.set(
                org.apache.flink.configuration.ClusterOptions.PROCESS_WORKING_DIR_BASE,
                temporary.resolve("working").toString());
        options.set(
                org.apache.flink.state.rocksdb.RocksDBOptions.LOCAL_DIRECTORIES,
                temporary.resolve("state").toString());
        options.set(org.apache.flink.state.rocksdb.RocksDBConfigurableOptions.MAX_BACKGROUND_THREADS, 3);
        options.set(
                org.apache.flink.state.rocksdb.RocksDBConfigurableOptions.USE_INGEST_DB_RESTORE_MODE,
                org.apache.flink.state.rocksdb.RocksDBConfigurableOptions.USE_INGEST_DB_RESTORE_MODE.defaultValue());
        options.set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, false);
        options.set(ExecutionConfigOptions.TABLE_EXEC_ASYNC_STATE_ENABLED, false);
        var cluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
                .setConfiguration(options)
                .setNumberTaskManagers(1)
                .setNumberSlotsPerTaskManager(2)
                .build());
        cluster.before();
        String prior = System.getProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        try {
            var expected = execute(options, false, false, incremental, unaligned);
            compare(expected, execute(options, true, false, incremental, unaligned));
            compare(expected, execute(options, true, true, incremental, unaligned));
        } finally {
            if (prior == null) System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
            else System.setProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, prior);
            StreamFusionPlannerFactory.resetMetrics();
            cluster.after();
        }
    }

    private Result execute(
            Configuration options, boolean nativeEngine, boolean corrupt, boolean incremental, boolean unaligned)
            throws Exception {
        if (nativeEngine)
            System.setProperty(
                    StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, StreamFusionPlannerFactory.class.getName());
        else System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        StreamFusionPlannerFactory.resetMetrics();
        String run = UUID.randomUUID().toString();
        var control = new RocksDbTaskRecoveryControl(temporary.resolve("backup"), corrupt, nativeEngine);
        RocksDbTaskRecoveryControl.RUNS.put(run, control);
        try {
            var environment = StreamExecutionEnvironment.getExecutionEnvironment();
            environment.configure(options);
            environment.setParallelism(1);
            environment.enableCheckpointing(100);
            environment.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
            environment.getCheckpointConfig().setMinPauseBetweenCheckpoints(100);
            if (unaligned) environment.getCheckpointConfig().enableUnalignedCheckpoints();
            var tables = StreamTableEnvironment.create(
                    environment,
                    EnvironmentSettings.newInstance().inStreamingMode().build());
            tables.getConfig().addConfiguration(options);
            tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
            var input = environment.addSource(
                    new RocksDbTaskRecoveryFunctions.Source(run),
                    "checkpointed-input",
                    RocksDbTaskRecoveryFunctions.INPUT);
            tables.createTemporaryView(
                    "recovery_input",
                    tables.fromChangelogStream(
                            input,
                            Schema.newBuilder()
                                    .column("category", "STRING")
                                    .column("amount", "BIGINT")
                                    .build()));
            var table = tables.sqlQuery("SELECT category, COUNT(*), SUM(amount), MIN(amount), MAX(amount) "
                    + "FROM recovery_input GROUP BY category");
            tables.toChangelogStream(table)
                    .addSink(new RocksDbTaskRecoveryFunctions.Sink(run))
                    .name("checkpointed-changelog");
            var graph = environment.getStreamGraph();
            graph.setJobName("rocksdb-local-recovery-" + run);
            graph.setStateBackend(
                    new RocksDbTaskRecoveryControl.ObservedBackend(run, options, nativeEngine, incremental));
            graph.createJobCheckpointingSettings();
            var job = environment.executeAsync(graph);
            try {
                job.getJobExecutionResult().get(90, TimeUnit.SECONDS);
            } finally {
                if (control.result == null) job.cancel().get(10, TimeUnit.SECONDS);
            }
            assertThat(control.completedCheckpointFailures).isOne();
            assertThat(control.restoredSourceOffsets).containsExactly(80);
            assertThat(control.result).isNotEmpty();
            assertThat(control.localAttempts).containsExactlyElementsOf(corrupt ? List.of(true, false) : List.of(true));
            if (nativeEngine) {
                assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
                assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isPositive();
            }
            assertThat(control.corruptions).isEqualTo(corrupt ? 1 : 0);
            return new Result(
                    control.result,
                    SqlChangelogCapture.encode(
                            table.getResolvedSchema().toPhysicalRowDataType(),
                            control.result.iterator(),
                            SqlChangelogCapture.Order.CHANGELOG));
        } finally {
            RocksDbTaskRecoveryControl.RUNS.remove(run);
        }
    }

    private static void compare(Result expected, Result actual) {
        int at = 0;
        while (at < Math.min(expected.rows.size(), actual.rows.size())
                && expected.rows.get(at).equals(actual.rows.get(at))) at++;
        final int mismatch = at;
        assertThat(actual.bytes)
                .withFailMessage(() -> "Changelog differs at row " + mismatch
                        + " (sizes " + expected.rows.size() + "/" + actual.rows.size() + "); expected "
                        + expected.rows.subList(Math.max(0, mismatch - 2), Math.min(expected.rows.size(), mismatch + 4))
                        + "; actual "
                        + actual.rows.subList(Math.max(0, mismatch - 2), Math.min(actual.rows.size(), mismatch + 4)))
                .isEqualTo(expected.bytes);
    }

    private static final class Result {
        final List<org.apache.flink.types.Row> rows;
        final byte[] bytes;

        Result(List<org.apache.flink.types.Row> rows, byte[] bytes) {
            this.rows = rows;
            this.bytes = bytes;
        }
    }
}
