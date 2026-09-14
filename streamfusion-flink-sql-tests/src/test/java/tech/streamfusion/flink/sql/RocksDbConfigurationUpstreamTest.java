/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.sql;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.apache.flink.state.rocksdb.RocksDBConfigurableOptions;
import org.apache.flink.state.rocksdb.RocksDBOptions;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.planner.runtime.stream.sql.AggregateITCase;
import org.apache.flink.table.planner.runtime.utils.StreamingWithAggTestBase.AggMode;
import org.apache.flink.table.planner.runtime.utils.StreamingWithMiniBatchTestBase.MiniBatchMode;
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

/** Executes the published Flink SQL test body and its assertions with nondefault database options and cache geometry. */
@org.junit.jupiter.api.extension.ExtendWith(
        org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension.class)
public class RocksDbConfigurationUpstreamTest extends StreamingWithStateTestBase {
    @org.junit.jupiter.api.io.TempDir
    java.nio.file.Path logRoot;

    private final StateBackendMode backend;
    private static String priorFactory;

    public RocksDbConfigurationUpstreamTest(StateBackendMode backend) {
        super(backend);
        this.backend = backend;
    }

    @BeforeAll
    static void installPlanner() {
        priorFactory = System.getProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        System.setProperty(
                StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, UpstreamNativePlannerFactory.class.getName());
    }

    @TestTemplate
    void upstreamBinaryStringAggregateRunsWithConfiguredRocksDbMemory() {
        StreamFusionPlannerFactory.resetMetrics();
        System.setProperty(
                StreamFusionPlannerFactory.EXEC_GRAPH_PROCESSOR_PROPERTY,
                "tech.streamfusion.flink.planner.StreamFusionExecGraphProcessor");
        var options = tech.streamfusion.flink.planner.RocksDbConfigurationProfiles.indexOptions(1);
        options.addAll(tech.streamfusion.flink.planner.RocksDbStatisticsProfiles.allTickers());
        options.addAll(tech.streamfusion.flink.planner.RocksDbDormantCompactionProfiles.disabled(999999));
        options.set(org.apache.flink.state.rocksdb.RocksDBOptions.CHECKPOINT_TRANSFER_THREAD_NUM, -2);
        options.set(
                org.apache.flink.state.rocksdb.RocksDBOptions.LOCAL_DIRECTORIES,
                logRoot.resolve("state-a") + "," + logRoot.resolve("state-b"));
        options.set(
                org.apache.flink.state.rocksdb.RocksDBConfigurableOptions.LOG_DIR,
                logRoot.resolve("rocks-logs").toString());
        options.set(
                RocksDBConfigurableOptions.COMPRESSION_PER_LEVEL,
                java.util.List.of(
                        org.rocksdb.CompressionType.ZSTD_COMPRESSION, org.rocksdb.CompressionType.LZ4_COMPRESSION));
        options.set(RocksDBConfigurableOptions.WRITE_BATCH_SIZE, new org.apache.flink.configuration.MemorySize(4096));
        options.set(org.apache.flink.configuration.CheckpointingOptions.LOCAL_BACKUP_ENABLED, true);
        options.set(org.apache.flink.configuration.StateRecoveryOptions.LOCAL_RECOVERY, true);
        options.set(RocksDBOptions.WRITE_BUFFER_RATIO, 0.7);
        options.set(RocksDBOptions.HIGH_PRIORITY_POOL_RATIO, 0.2);
        options.set(
                RocksDBConfigurableOptions.USE_INGEST_DB_RESTORE_MODE,
                RocksDBConfigurableOptions.USE_INGEST_DB_RESTORE_MODE.defaultValue());
        env().configure(options, getClass().getClassLoader());
        tEnv().getConfig().addConfiguration(options);
        tEnv().getConfig().setIdleStateRetention(Duration.ZERO);
        tEnv().getConfig().set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, false);
        tEnv().getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ASYNC_STATE_ENABLED, false);
        var upstream = new AggregateITCase(new AggMode(false), new MiniBatchMode(false), backend, false);
        upstream.env_$eq(env());
        upstream.tEnv_$eq(tEnv());
        upstream.tempFolder_$eq(tempFolder());
        upstream.testBigDataOfMinMaxWithBinaryString();
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
        assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isPositive();
    }

    @AfterAll
    static void restorePlanner() {
        StreamFusionPlannerFactory.resetMetrics();
        if (priorFactory == null) System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        else System.setProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, priorFactory);
    }
}
