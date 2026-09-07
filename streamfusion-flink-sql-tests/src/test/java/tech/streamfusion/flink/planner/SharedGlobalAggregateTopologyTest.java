/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.stream.IntStream;
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
import org.apache.flink.table.planner.plan.nodes.exec.processor.ProcessorContext;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecGlobalGroupAggregate;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchTypeInfo;
import tech.streamfusion.flink.operator.StreamFusionNativeRegionOperatorFactory;
import tech.streamfusion.proto.plan.v1.NativePlan;

/** Selected global fragments exercise the normal collector; production all-or-nothing gates remain. */
class SharedGlobalAggregateTopologyTest {
    private static final RowType RAW = RowType.of(new BigIntType(false), new BigIntType(), new BigIntType());
    private static final RowType OUTPUT = RowType.of(new BigIntType(false), new BigIntType(false), new BigIntType());
    private static final RowType PARTIAL =
            RowType.of(new BigIntType(false), new VarBinaryType(false, Integer.MAX_VALUE));

    @Test
    void globalConsumerUsesOneCommonArrowRuntimeAndKeepsOriginalStageIdentity() throws Exception {
        String previous = System.getProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        try {
            var env = (TableEnvironmentImpl) TableEnvironment.create(EnvironmentSettings.inStreamingMode());
            var planner = (PlannerBase) env.getPlanner();
            var config = config();
            var source = new NativeRegionTopologyTest.ArrowSource(PARTIAL);
            var distribution = InputProperty.builder()
                    .requiredDistribution(InputProperty.hashDistribution(new int[] {0}))
                    .build();
            var exchange = unary(new StreamFusionExecExchange(config, distribution, PARTIAL, "exchange"), source);
            var before = calc(planner, config, exchange, PARTIAL);
            var global = unary(selected(config), before);
            var original = original(config);
            original.setCompiled(true);
            new StreamFusionGraphRewrite().convert(original, ignored -> global);
            var after = calc(planner, config, global, OUTPUT);
            var result = after.translateToPlan(planner);
            assertThat(result).isInstanceOf(KeyedMultipleInputTransformation.class);
            var owner = (KeyedMultipleInputTransformation<?>) result;
            assertThat(owner.getOperatorFactory()).isInstanceOf(StreamFusionNativeRegionOperatorFactory.class);
            assertThat(owner.getOutputType()).isSameAs(ArrowRowDataBatchTypeInfo.INSTANCE);
            assertThat(owner.getTransitivePredecessors().stream()
                            .filter(node -> node instanceof OneInputTransformation)
                            .count())
                    .isOne();
            var field = StreamFusionNativeRegionOperatorFactory.class.getDeclaredField("plan");
            field.setAccessible(true);
            var plan = NativePlan.parseFrom((byte[]) field.get(owner.getOperatorFactory()));
            var stage = plan.getRoot().getCalc().getInput();
            assertThat(plan.getProtocolVersion()).isEqualTo(3);
            assertThat(stage.hasGlobalGroupAggregate()).isTrue();
            assertThat(stage.getGlobalGroupAggregate().getMiniBatchSize()).isEqualTo(7);
            assertThat(stage.getGlobalGroupAggregate().getAggregateCalls(1).getInputIndex())
                    .isEqualTo(2);
            assertThat(stage.getGlobalGroupAggregate()
                            .getInput()
                            .getCalc()
                            .getInput()
                            .hasInput())
                    .isTrue();
            assertThat(stage.getPlanNodeId()).isEqualTo((1L << 32) | original.getId());
            assertThat(stage.getMetricName()).isEqualTo(global.nativeMetadata().metricName(global, env.getConfig()));
            assertThat(stage.getMetricUid()).isEqualTo(global.nativeMetadata().metricUid(env.getConfig()));
            var ids = StreamFusionNativeRegionOperatorFactory.class.getDeclaredField("stateIds");
            ids.setAccessible(true);
            assertThat(ids.get(owner.getOperatorFactory())).isEqualTo(List.of(stage.getPlanNodeId()));
            assertThat(original.getTransformation()).isNull();
            assertThat(before.getTransformation()).isNull();
            assertThat(global.getTransformation()).isNull();
            assertThat(source.translations).isOne();
        } finally {
            if (previous == null) System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
            else System.setProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, previous);
        }
    }

    @Test
    void fragmentAndPlanningHonorEffectiveConfigurationAndStateMetricRequirements() throws Exception {
        String previous = System.getProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        try {
            var env = (TableEnvironmentImpl) TableEnvironment.create(EnvironmentSettings.inStreamingMode());
            var planner = (PlannerBase) env.getPlanner();
            env.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, true);
            env.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE, 11L);
            var selected = unary(selected(new Configuration()), new NativeRegionTopologyTest.ArrowSource(PARTIAL));
            assertThat(NativePlan.parseFrom(selected.nativePlanFragment(planner))
                            .getRoot()
                            .getGlobalGroupAggregate()
                            .getMiniBatchSize())
                    .isEqualTo(11);
            var overridden = unary(selected(config()), new NativeRegionTopologyTest.ArrowSource(PARTIAL));
            assertThat(NativePlan.parseFrom(overridden.nativePlanFragment(planner))
                            .getRoot()
                            .getGlobalGroupAggregate()
                            .getMiniBatchSize())
                    .isEqualTo(7);
            for (var option : List.of(
                    "state.backend.latency-track.keyed-state-enabled",
                    "state.backend.rocksdb.metrics.estimate-num-keys")) {
                env.getConfig().getConfiguration().setString(option, "true");
                assertThatThrownBy(() -> overridden.nativePlanFragment(planner))
                        .hasRootCauseMessage(
                                option.contains("latency")
                                        ? "metrics: keyed-state latency histograms are not yet published by shared native state"
                                        : "metrics: enabled RocksDB native metrics are not yet published by shared native state");
                assertThat(StreamFusionGlobalGroupAggregateSupport.unsupportedReason(
                                original(config()), new ProcessorContext(planner)))
                        .startsWith("metrics:");
                env.getConfig().getConfiguration().removeKey(option);
            }
            env.getConfig().setIdleStateRetention(java.time.Duration.ofSeconds(1));
            assertThatThrownBy(() -> overridden.nativePlanFragment(planner))
                    .hasRootCauseMessage("state: native group aggregate TTL is not implemented");
            env.getConfig().setIdleStateRetention(java.time.Duration.ZERO);
            env.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, false);
            assertThatThrownBy(() -> selected.nativePlanFragment(planner))
                    .hasRootCauseMessage("mini-batch: global aggregate requires enabled mini-batching");
        } finally {
            if (previous == null) System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
            else System.setProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, previous);
        }
    }

    @Test
    void globalCompositionAdmissionIsSeparateFromTheRemainingMemoryGate() {
        var config = config();
        var types = new FlinkTypeFactory(getClass().getClassLoader(), RelDataTypeSystem.DEFAULT);
        var rex = new RexBuilder(types);
        var before = unary(
                new org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecCalc(
                        config,
                        IntStream.range(0, PARTIAL.getFieldCount())
                                .mapToObj(index -> rex.makeInputRef(
                                        types.createFieldTypeFromLogicalType(PARTIAL.getTypeAt(index)), index))
                                .collect(java.util.stream.Collectors.toList()),
                        null,
                        InputProperty.DEFAULT,
                        PARTIAL,
                        "before"),
                new NativeRegionTopologyTest.ArrowSource(PARTIAL));
        var global = unary(original(config), before);
        var after = unary(
                new org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecCalc(
                        config,
                        IntStream.range(0, OUTPUT.getFieldCount())
                                .mapToObj(index -> rex.makeInputRef(
                                        types.createFieldTypeFromLogicalType(OUTPUT.getTypeAt(index)), index))
                                .collect(java.util.stream.Collectors.toList()),
                        null,
                        InputProperty.DEFAULT,
                        OUTPUT,
                        "after"),
                global);
        var reasons = new java.util.ArrayList<String>();
        StreamFusionArchitectureSupport.collect(
                new org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph(List.of(after)), reasons);
        assertThat(reasons).hasSize(1).allMatch(reason -> reason.contains("retained-state/buffer admission"));
        assertThat(String.join("\n", reasons)).doesNotContain("intermediate JNI", "fused native ExecutionPlan");
    }

    @Test
    void globalFragmentRejectsMalformedAccumulatorAndUnsupportedStateContracts() {
        var config = config();
        var nullable = RowType.of(new BigIntType(false), new VarBinaryType());
        assertThat(tech.streamfusion.flink.aggregate.StreamFusionGlobalGroupAggregateTranslator.unsupportedStageReason(
                        RAW, nullable, OUTPUT, 1, calls(), new boolean[] {true, true}, true, 0, config))
                .contains("non-null VARBINARY");
        assertThat(tech.streamfusion.flink.aggregate.StreamFusionGlobalGroupAggregateTranslator.unsupportedStageReason(
                        RAW, PARTIAL, OUTPUT, 1, calls(), new boolean[] {true, false}, true, 0, config))
                .contains("retractable accumulator");
        config.set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE, 0L);
        assertThat(reason(config)).contains("size must be positive");
        config.set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE, 7L);
        config.set(ExecutionConfigOptions.TABLE_EXEC_ASYNC_STATE_ENABLED, true);
        assertThat(reason(config)).contains("async-state");
        config.set(ExecutionConfigOptions.TABLE_EXEC_ASYNC_STATE_ENABLED, false);
        config.set(org.apache.flink.configuration.StateChangelogOptions.ENABLE_STATE_CHANGE_LOG, true);
        assertThat(reason(config)).contains("changelog-state wrapping");
    }

    private static String reason(Configuration config) {
        return tech.streamfusion.flink.aggregate.StreamFusionGlobalGroupAggregateTranslator.unsupportedStageReason(
                RAW, PARTIAL, OUTPUT, 1, calls(), new boolean[] {true, true}, true, 0, config);
    }

    private static Configuration config() {
        var config = new Configuration();
        config.set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, true);
        config.set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE, 7L);
        return config;
    }

    private static AggregateCall[] calls() {
        var types = new FlinkTypeFactory(
                SharedGlobalAggregateTopologyTest.class.getClassLoader(), RelDataTypeSystem.DEFAULT);
        return new AggregateCall[] {
            AggregateCall.create(
                    SqlStdOperatorTable.COUNT,
                    false,
                    List.of(),
                    -1,
                    types.createFieldTypeFromLogicalType(new BigIntType(false)),
                    "n"),
            AggregateCall.create(
                    SqlStdOperatorTable.SUM,
                    false,
                    List.of(2),
                    -1,
                    types.createFieldTypeFromLogicalType(new BigIntType()),
                    "s")
        };
    }

    private static StreamFusionExecGlobalGroupAggregate selected(Configuration config) {
        return new StreamFusionExecGlobalGroupAggregate(
                config, RAW, 1, calls(), new boolean[] {true, true}, true, true, null, InputProperty.DEFAULT, OUTPUT);
    }

    private static StreamExecGlobalGroupAggregate original(Configuration config) {
        return new StreamExecGlobalGroupAggregate(
                config,
                new int[] {0},
                calls(),
                new boolean[] {true, true},
                RAW,
                true,
                true,
                0,
                null,
                InputProperty.DEFAULT,
                OUTPUT,
                "original global");
    }

    private static StreamFusionExecCalc calc(
            PlannerBase planner, Configuration config, ExecNode<?> child, RowType type) {
        var types = planner.getTypeFactory();
        var rex = new RexBuilder(types);
        return unary(
                new StreamFusionExecCalc(
                        config,
                        IntStream.range(0, type.getFieldCount())
                                .mapToObj(index -> rex.makeInputRef(
                                        types.createFieldTypeFromLogicalType(type.getTypeAt(index)), index))
                                .collect(java.util.stream.Collectors.toList()),
                        null,
                        InputProperty.DEFAULT,
                        type,
                        "calc"),
                child);
    }

    private static <T extends ExecNode<?>> T unary(T node, ExecNode<?> input) {
        node.setInputEdges(List.of(ExecEdge.builder().source(input).target(node).build()));
        return node;
    }
}
