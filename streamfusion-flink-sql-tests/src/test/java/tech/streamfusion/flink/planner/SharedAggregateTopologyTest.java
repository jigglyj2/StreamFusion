/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.transformations.KeyedMultipleInputTransformation;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecGroupAggregate;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchTypeInfo;
import tech.streamfusion.flink.operator.StreamFusionNativeRegionOperatorFactory;
import tech.streamfusion.flink.planner.aggregate.StreamFusionGroupAggregateTranslator;
import tech.streamfusion.proto.plan.v1.NativePlan;

/** Selected physical nodes, not a bypass of the separate all-or-nothing admission gate. */
class SharedAggregateTopologyTest {
    private static final RowType INPUT = RowType.of(new BigIntType(false));

    @Test
    void aggregationAndDistinctComposeThroughTheSharedCollectorWithOriginalStageIdentity() throws Exception {
        String previous = System.getProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        try {
            for (boolean singleton : List.of(false, true))
                for (boolean distinct : List.of(false, true)) {
                    if (singleton && distinct) continue; // SQL DISTINCT has grouping keys.
                    var environment =
                            (TableEnvironmentImpl) TableEnvironment.create(EnvironmentSettings.inStreamingMode());
                    var planner = (PlannerBase) environment.getPlanner();
                    var config = new Configuration();
                    var source = new NativeRegionTopologyTest.ArrowSource(INPUT);
                    var distribution = InputProperty.builder()
                            .requiredDistribution(
                                    singleton
                                            ? InputProperty.SINGLETON_DISTRIBUTION
                                            : InputProperty.hashDistribution(new int[] {0}))
                            .build();
                    var exchange = unary(new StreamFusionExecExchange(config, distribution, INPUT, "exchange"), source);
                    var types = new FlinkTypeFactory(getClass().getClassLoader(), RelDataTypeSystem.DEFAULT);
                    var rex = new RexBuilder(types);
                    var key = rex.makeInputRef(types.createFieldTypeFromLogicalType(INPUT.getTypeAt(0)), 0);
                    var before = unary(
                            new StreamFusionExecCalc(
                                    config, List.of(key), null, InputProperty.DEFAULT, INPUT, "before"),
                            exchange);
                    var calls = distinct ? new AggregateCall[0] : calls();
                    var retractable = distinct ? new boolean[0] : new boolean[] {true};
                    var grouping = singleton ? new int[0] : new int[] {0};
                    var output =
                            singleton || distinct ? INPUT : RowType.of(new BigIntType(false), new BigIntType(false));
                    var aggregate = unary(
                            new StreamFusionExecGroupAggregate(
                                    config,
                                    grouping,
                                    calls,
                                    retractable,
                                    true,
                                    true,
                                    null,
                                    InputProperty.DEFAULT,
                                    output,
                                    "aggregate"),
                            before);
                    var original = new StreamExecGroupAggregate(
                            config,
                            grouping,
                            calls,
                            retractable,
                            true,
                            true,
                            null,
                            InputProperty.DEFAULT,
                            output,
                            "original aggregate");
                    original.setCompiled(true);
                    new StreamFusionGraphRewrite().convert(original, ignored -> aggregate);
                    var after = unary(
                            new StreamFusionExecCalc(config, List.of(key), null, InputProperty.DEFAULT, INPUT, "after"),
                            aggregate);
                    var result = after.translateToPlan(planner);
                    assertThat(result).isInstanceOf(KeyedMultipleInputTransformation.class);
                    var owner = (KeyedMultipleInputTransformation<?>) result;
                    assertThat(owner.getOperatorFactory()).isInstanceOf(StreamFusionNativeRegionOperatorFactory.class);
                    assertThat(owner.getStateKeySelectors()).hasSize(1);
                    assertThat(owner.getOutputType()).isSameAs(ArrowRowDataBatchTypeInfo.INSTANCE);
                    assertThat(owner.getTransitivePredecessors().stream()
                                    .filter(node -> node instanceof OneInputTransformation)
                                    .count())
                            .isEqualTo(1);
                    if (singleton) assertThat(owner.getParallelism()).isOne();
                    var field = StreamFusionNativeRegionOperatorFactory.class.getDeclaredField("plan");
                    field.setAccessible(true);
                    var plan = NativePlan.parseFrom((byte[]) field.get(owner.getOperatorFactory()));
                    var stage = plan.getRoot().getCalc().getInput();
                    assertThat(plan.getProtocolVersion()).isEqualTo(3);
                    assertThat(stage.hasGroupAggregate()).isTrue();
                    assertThat(stage.getPlanNodeId()).isEqualTo((1L << 32) | original.getId());
                    assertThat(stage.getMetricName())
                            .isEqualTo(aggregate.nativeMetadata().metricName(aggregate, planner.getTableConfig()));
                    assertThat(stage.getMetricUid()).isEqualTo(original.getId() + "_group-aggregate");
                    assertThat(stage.getGroupAggregate().getAggregateCallsCount())
                            .isEqualTo(calls.length);
                    assertThat(stage.getGroupAggregate().getInput().getCalc().getPreserveInputEnvelope())
                            .isTrue();
                    assertThat(stage.getGroupAggregate()
                                    .getInput()
                                    .getCalc()
                                    .getInput()
                                    .hasInput())
                            .isTrue();
                    assertThat(original.getTransformation()).isNull();
                    assertThat(before.getTransformation()).isNull();
                    assertThat(aggregate.getTransformation()).isNull();
                    assertThat(source.translations).isOne();
                }
        } finally {
            if (previous == null) System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
            else System.setProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, previous);
        }
    }

    @Test
    void selectedFragmentUsesTableConfigurationAndHonorsPersistedOverrides() throws Exception {
        String previous = System.getProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        try {
            var environment = (TableEnvironmentImpl) TableEnvironment.create(EnvironmentSettings.inStreamingMode());
            var planner = (PlannerBase) environment.getPlanner();
            environment.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, true);
            environment.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE, 10L);
            var config = new Configuration();
            var output = RowType.of(new BigIntType(false), new BigIntType(false));
            var aggregate = unary(
                    new StreamFusionExecGroupAggregate(
                            config,
                            new int[] {0},
                            calls(),
                            new boolean[] {true},
                            true,
                            true,
                            null,
                            InputProperty.DEFAULT,
                            output,
                            "aggregate"),
                    new NativeRegionTopologyTest.ArrowSource(INPUT));
            assertThat(NativePlan.parseFrom(aggregate.nativePlanFragment(planner))
                            .getRoot()
                            .getGroupAggregate()
                            .getMiniBatchSize())
                    .isEqualTo(10);
            config.set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, false);
            var overridden = unary(
                    new StreamFusionExecGroupAggregate(
                            config,
                            new int[] {0},
                            calls(),
                            new boolean[] {true},
                            true,
                            true,
                            null,
                            InputProperty.DEFAULT,
                            output,
                            "aggregate"),
                    new NativeRegionTopologyTest.ArrowSource(INPUT));
            assertThat(NativePlan.parseFrom(overridden.nativePlanFragment(planner))
                            .getRoot()
                            .getGroupAggregate()
                            .getMiniBatchSize())
                    .isZero();
            environment.getConfig().setIdleStateRetention(java.time.Duration.ofSeconds(1));
            assertThatThrownBy(() -> overridden.nativePlanFragment(planner))
                    .hasRootCauseMessage("state: native group aggregate TTL is not implemented");
        } finally {
            if (previous == null) System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
            else System.setProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, previous);
        }
    }

    @Test
    void fragmentPreservesBundleControlsAndRejectsInvalidRetractionContracts() throws Exception {
        var config = new Configuration();
        var output = RowType.of(new BigIntType(false), new BigIntType(false));
        assertThat(StreamFusionGroupAggregateTranslator.unsupportedStageReason(
                        INPUT, output, new int[] {0}, calls(), new boolean[] {true}, true, true, 0L, config))
                .isNull();
        config.set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, true);
        config.set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE, 10L);
        var mini = NativePlan.parseFrom(StreamFusionGroupAggregateTranslator.createStagePlan(
                        INPUT, output, new int[] {0}, calls(), new boolean[] {true}, true, true, 0L, config))
                .getRoot()
                .getGroupAggregate();
        assertThat(mini.getMiniBatchSize()).isEqualTo(10);
        assertThat(mini.hasInputSchema()).isTrue();
        assertThat(mini.hasOutputSchema()).isTrue();
        config.set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, false);
        assertThat(StreamFusionGroupAggregateTranslator.unsupportedStageReason(
                        INPUT, output, new int[] {0}, calls(), new boolean[] {true}, true, true, 100L, config))
                .contains("TTL");
        assertThat(StreamFusionGroupAggregateTranslator.unsupportedStageReason(
                        INPUT, output, new int[] {0}, calls(), new boolean[] {false}, true, true, 0L, config))
                .containsIgnoringCase("retract");
    }

    @Test
    void optionalStateMetricRequestsRejectSharedFragmentsInsteadOfSilentlyOmittingMetrics() {
        var output = RowType.of(new BigIntType(false), new BigIntType(false));
        for (boolean mini : List.of(false, true)) {
            var config = new Configuration();
            config.set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, mini);
            config.set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE, 10L);
            assertThat(stageReason(config, output)).isNull();
            config.set(org.apache.flink.configuration.StateLatencyTrackOptions.LATENCY_TRACK_ENABLED, true);
            assertThat(stageReason(config, output)).contains("keyed-state latency histograms");
            config.removeConfig(org.apache.flink.configuration.StateLatencyTrackOptions.LATENCY_TRACK_ENABLED);
            config.setString("state.backend.latency-track.keyed-state-enabled", "true");
            assertThat(stageReason(config, output)).contains("keyed-state latency histograms");
            config.removeKey("state.backend.latency-track.keyed-state-enabled");
            for (var option : List.of(
                    org.apache.flink.state.rocksdb.RocksDBNativeMetricOptions.ESTIMATE_NUM_KEYS,
                    org.apache.flink.state.rocksdb.RocksDBNativeMetricOptions.MONITOR_BYTES_READ)) {
                config.set(option, true);
                assertThat(stageReason(config, output)).contains("RocksDB native metrics");
                config.set(option, false);
                assertThat(stageReason(config, output)).isNull();
            }
        }
    }

    private static String stageReason(Configuration config, RowType output) {
        return StreamFusionGroupAggregateTranslator.unsupportedStageReason(
                INPUT, output, new int[] {0}, calls(), new boolean[] {true}, true, true, 0L, config);
    }

    private static AggregateCall[] calls() {
        var types = new FlinkTypeFactory(SharedAggregateTopologyTest.class.getClassLoader(), RelDataTypeSystem.DEFAULT);
        return new AggregateCall[] {
            AggregateCall.create(
                    SqlStdOperatorTable.COUNT,
                    false,
                    List.of(),
                    -1,
                    types.createFieldTypeFromLogicalType(new BigIntType(false)),
                    "n")
        };
    }

    private static <T extends ExecNode<?>> T unary(T node, ExecNode<?> input) {
        node.setInputEdges(List.of(ExecEdge.builder().source(input).target(node).build()));
        return node;
    }
}
