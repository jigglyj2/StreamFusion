/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.sql;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.apache.flink.table.api.config.AggregatePhaseStrategy;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.planner.runtime.stream.sql.AggregateITCase;
import org.apache.flink.table.planner.runtime.utils.StreamingWithAggTestBase.AggMode;
import org.apache.flink.table.planner.runtime.utils.StreamingWithMiniBatchTestBase.MiniBatchMode;
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.SelectedAggregateSqlProbe;

/** Upstream SQL/recovery coverage of the retained shared mini-batch implementation, not production admission. */
@org.junit.jupiter.api.extension.ExtendWith(
        org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension.class)
public class SelectedMiniBatchAggregateUpstreamTest extends StreamingWithStateTestBase {
    private final StateBackendMode backend;
    private static String priorFactory;
    private static String priorProcessor;

    public SelectedMiniBatchAggregateUpstreamTest(StateBackendMode backend) {
        super(backend);
        this.backend = backend;
    }

    @BeforeAll
    static void installPlanner() {
        priorFactory = System.getProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        priorProcessor = System.getProperty(StreamFusionPlannerFactory.EXEC_GRAPH_PROCESSOR_PROPERTY);
        System.setProperty(
                StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, UpstreamNativePlannerFactory.class.getName());
        System.setProperty(
                StreamFusionPlannerFactory.EXEC_GRAPH_PROCESSOR_PROPERTY, SelectedAggregateSqlProbe.class.getName());
    }

    @TestTemplate
    void upstreamGroupByUsesNativeBundlesAndRecoversAfterSourceFailure() {
        StreamFusionPlannerFactory.resetMetrics();
        System.setProperty(
                StreamFusionPlannerFactory.EXEC_GRAPH_PROCESSOR_PROPERTY, SelectedAggregateSqlProbe.class.getName());
        SelectedAggregateSqlProbe.recordOnly = false;
        SelectedAggregateSqlProbe.convertedGraphs = 0;
        var options = new org.apache.flink.configuration.Configuration();
        var ingest = org.apache.flink.state.rocksdb.RocksDBConfigurableOptions.USE_INGEST_DB_RESTORE_MODE;
        options.set(ingest, ingest.defaultValue());
        env().configure(options, getClass().getClassLoader());
        tEnv().getConfig().addConfiguration(options);
        tEnv().getConfig().setIdleStateRetention(Duration.ZERO);
        tEnv().getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ASYNC_STATE_ENABLED, false);
        tEnv().getConfig().set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, true);
        tEnv().getConfig().set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE, 3L);
        tEnv().getConfig().set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY, Duration.ofHours(1));
        tEnv().getConfig()
                .set(OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY, AggregatePhaseStrategy.ONE_PHASE);
        var upstream = new AggregateITCase(new AggMode(false), new MiniBatchMode(true), backend, false);
        upstream.env_$eq(env());
        upstream.tEnv_$eq(tEnv());
        upstream.tempFolder_$eq(tempFolder());
        upstream.testGroupByAgg();
        assertThat(SelectedAggregateSqlProbe.convertedGraphs).isPositive();
        assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isPositive();
        assertThat(StreamFusionPlannerFactory.nativeGroupAggregateBatchCount()).isZero();
        SelectedAggregateSqlProbe.verifyTranslatedArrowTopology();
    }

    @AfterAll
    static void restorePlanner() {
        StreamFusionPlannerFactory.resetMetrics();
        restore(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, priorFactory);
        restore(StreamFusionPlannerFactory.EXEC_GRAPH_PROCESSOR_PROPERTY, priorProcessor);
    }

    private static void restore(String property, String value) {
        if (value == null) System.clearProperty(property);
        else System.setProperty(property, value);
    }
}
