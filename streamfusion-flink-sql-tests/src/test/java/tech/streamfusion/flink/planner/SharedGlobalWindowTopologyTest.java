/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.transformations.KeyedMultipleInputTransformation;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.logical.HoppingWindowSpec;
import org.apache.flink.table.planner.plan.logical.TimeAttributeWindowingStrategy;
import org.apache.flink.table.planner.plan.logical.WindowAttachedWindowingStrategy;
import org.apache.flink.table.planner.plan.logical.WindowingStrategy;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecGlobalWindowAggregate;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchTypeInfo;
import tech.streamfusion.flink.operator.StreamFusionNativeRegionOperatorFactory;
import tech.streamfusion.flink.planner.window.StreamFusionGlobalWindowAggregateTranslator;
import tech.streamfusion.proto.plan.v1.NativePlan;

/** Global-window nodes compose through the normal selected-region path; no admission bypass is installed. */
class SharedGlobalWindowTopologyTest {
    static final TimestampType ROWTIME = new TimestampType(false, TimestampKind.ROWTIME, 3);
    static final RowType RAW = RowType.of(new BigIntType(), ROWTIME);

    @Test
    void runtimePreflightLoadsWindowBuilderFromPlannerInsteadOfApplicationLoader() {
        var loader = new ClassLoader(getClass().getClassLoader()) {
            @Override
            protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
                if (name.startsWith("tech.streamfusion.flink.planner.") || name.startsWith("org.apache.calcite."))
                    throw new ClassNotFoundException(name);
                return super.loadClass(name, resolve);
            }
        };
        assertThat(StreamFusionExecGraphProcessor.runtimePreflightRejection(loader))
                .isNull();
    }

    @Test
    void sharedAndAttachedGlobalNodesPreserveOriginalIdentityAndCreateOnlyOneArrowRuntime() throws Exception {
        String previous = System.getProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        try {
            var tables = (TableEnvironmentImpl) TableEnvironment.create(EnvironmentSettings.inStreamingMode());
            var planner = (PlannerBase) tables.getPlanner();
            for (boolean attached : List.of(false, true)) {
                var config = SharedSlicingWindowFixture.config();
                var source = new NativeRegionTopologyTest.ArrowSource(SharedSlicingWindowFixture.INPUT);
                var distribution = InputProperty.builder()
                        .requiredDistribution(InputProperty.hashDistribution(new int[] {0}))
                        .build();
                var exchange = unary(
                        new StreamFusionExecExchange(
                                config, distribution, SharedSlicingWindowFixture.INPUT, "exchange"),
                        source);
                var before = calc(planner, config, exchange, SharedSlicingWindowFixture.INPUT);
                var global = unary(selected(config, strategy(attached)), before);
                var original = new StreamExecGlobalWindowAggregate(
                        config,
                        new int[] {0},
                        calls(),
                        strategy(attached),
                        SharedSlicingWindowFixture.properties(),
                        false,
                        InputProperty.DEFAULT,
                        RAW,
                        SharedSlicingWindowFixture.OUTPUT,
                        "original global window");
                original.setCompiled(true);
                new StreamFusionGraphRewrite().convert(original, ignored -> global);
                var after = calc(planner, config, global, SharedSlicingWindowFixture.OUTPUT);
                var result = after.translateToPlan(planner);
                assertThat(result).isInstanceOf(KeyedMultipleInputTransformation.class);
                var owner = (KeyedMultipleInputTransformation<?>) result;
                assertThat(owner.getOutputType()).isSameAs(ArrowRowDataBatchTypeInfo.INSTANCE);
                assertThat(owner.getOperatorFactory()).isInstanceOf(StreamFusionNativeRegionOperatorFactory.class);
                assertThat(owner.getTransitivePredecessors().stream()
                                .filter(node -> node instanceof OneInputTransformation)
                                .count())
                        .isOne();
                var planField = StreamFusionNativeRegionOperatorFactory.class.getDeclaredField("plan");
                planField.setAccessible(true);
                var plan = NativePlan.parseFrom((byte[]) planField.get(owner.getOperatorFactory()));
                var stage = plan.getRoot().getCalc().getInput();
                assertThat(stage.hasWindowAggregate()).isTrue();
                assertThat(stage.getWindowAggregate().getPartialWindowsAreSlices())
                        .isEqualTo(!attached);
                assertThat(stage.getWindowAggregate()
                                .getInput()
                                .getCalc()
                                .getInput()
                                .hasInput())
                        .isTrue();
                assertThat(stage.getPlanNodeId()).isEqualTo((1L << 32) | original.getId());
                assertThat(stage.getMetricName())
                        .isEqualTo(global.nativeMetadata().metricName(global, tables.getConfig()));
                assertThat(stage.getMetricUid())
                        .isEqualTo(global.nativeMetadata().metricUid(tables.getConfig()));
                var ids = StreamFusionNativeRegionOperatorFactory.class.getDeclaredField("stateIds");
                ids.setAccessible(true);
                assertThat(ids.get(owner.getOperatorFactory())).isEqualTo(List.of(stage.getPlanNodeId()));
                assertThat(original.getTransformation()).isNull();
                assertThat(before.getTransformation()).isNull();
                assertThat(global.getTransformation()).isNull();
                assertThat(source.translations).isOne();
            }
        } finally {
            if (previous == null) System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
            else System.setProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, previous);
        }
    }

    @Test
    void effectiveConfigurationAndUnsupportedStateMetricsAreCheckedBeforeNativeLowering() throws Exception {
        String previous = System.getProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        try {
            var tables = (TableEnvironmentImpl) TableEnvironment.create(EnvironmentSettings.inStreamingMode());
            var planner = (PlannerBase) tables.getPlanner();
            var config = SharedSlicingWindowFixture.config();
            var selected = unary(
                    selected(config, strategy(false)),
                    new NativeRegionTopologyTest.ArrowSource(SharedSlicingWindowFixture.INPUT));
            tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ASYNC_STATE_ENABLED, true);
            assertThat(NativePlan.parseFrom(selected.nativePlanFragment(planner))
                            .getRoot()
                            .hasWindowAggregate())
                    .isTrue();
            for (String option : List.of(
                    "state.backend.latency-track.keyed-state-enabled",
                    "state.backend.rocksdb.metrics.estimate-num-keys")) {
                tables.getConfig().getConfiguration().setString(option, "true");
                assertThatThrownBy(() -> selected.nativePlanFragment(planner))
                        .hasRootCauseInstanceOf(IllegalArgumentException.class)
                        .hasStackTraceContaining("metrics:");
                tables.getConfig().getConfiguration().removeKey(option);
            }
        } finally {
            if (previous == null) System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
            else System.setProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, previous);
        }
    }

    @Test
    void globalStageExplainsUnimplementedComputeAndMalformedPartialSchemas() {
        var config = SharedSlicingWindowFixture.config();
        assertThat(reason(config, strategy(false), calls(), SharedSlicingWindowFixture.INPUT, false))
                .isNull();
        assertThat(reason(
                        config,
                        new TimeAttributeWindowingStrategy(
                                new HoppingWindowSpec(Duration.ofSeconds(7), Duration.ofSeconds(2), null), ROWTIME, 1),
                        calls(),
                        SharedSlicingWindowFixture.INPUT,
                        false))
                .contains("integral HOP");
        assertThat(reason(config, strategy(false), calls(), SharedSlicingWindowFixture.INPUT, true))
                .contains("append-only");
        var nullable = RowType.of(new BigIntType(), new VarBinaryType(), new BigIntType(false), new BigIntType(false));
        assertThat(reason(config, strategy(false), calls(), nullable, false)).contains("non-null VARBINARY");
        var sum = new AggregateCall[] {
            SharedSlicingWindowFixture.call(SqlStdOperatorTable.SUM, List.of(0), new BigIntType(false))
        };
        assertThat(reason(config, strategy(false), sum, SharedSlicingWindowFixture.INPUT, false))
                .contains("DataFusion append-only extrema");
        config.set(ExecutionConfigOptions.TABLE_EXEC_ASYNC_STATE_ENABLED, true);
        assertThat(reason(config, strategy(false), calls(), SharedSlicingWindowFixture.INPUT, false))
                .contains("synchronous");
    }

    private static String reason(
            Configuration config,
            WindowingStrategy strategy,
            AggregateCall[] calls,
            RowType input,
            boolean retractable) {
        return StreamFusionGlobalWindowAggregateTranslator.unsupportedStageReason(
                RAW,
                input,
                SharedSlicingWindowFixture.OUTPUT,
                1,
                calls,
                strategy,
                SharedSlicingWindowFixture.properties(),
                retractable,
                config);
    }

    private static AggregateCall[] calls() {
        return new AggregateCall[] {
            SharedSlicingWindowFixture.call(SqlStdOperatorTable.COUNT, List.of(), new BigIntType(false))
        };
    }

    private static WindowingStrategy strategy(boolean attached) {
        return attached
                ? new WindowAttachedWindowingStrategy(SharedSlicingWindowFixture.hop(), ROWTIME, 0, 1)
                : new TimeAttributeWindowingStrategy(SharedSlicingWindowFixture.hop(), ROWTIME, 1);
    }

    private static StreamFusionExecGlobalWindowAggregate selected(Configuration config, WindowingStrategy strategy) {
        return new StreamFusionExecGlobalWindowAggregate(
                config,
                RAW,
                1,
                calls(),
                strategy,
                SharedSlicingWindowFixture.properties(),
                false,
                InputProperty.DEFAULT,
                SharedSlicingWindowFixture.OUTPUT);
    }

    private static StreamFusionExecCalc calc(
            PlannerBase planner, Configuration config, ExecNode<?> child, RowType type) {
        var rex = new RexBuilder(planner.getTypeFactory());
        return unary(
                new StreamFusionExecCalc(
                        config,
                        IntStream.range(0, type.getFieldCount())
                                .mapToObj(index -> rex.makeInputRef(
                                        planner.getTypeFactory().createFieldTypeFromLogicalType(type.getTypeAt(index)),
                                        index))
                                .collect(Collectors.toList()),
                        null,
                        InputProperty.DEFAULT,
                        type,
                        "calc"),
                child);
    }

    private static <T extends ExecNode<?>> T unary(T node, ExecNode<?> child) {
        node.setInputEdges(List.of(ExecEdge.builder().source(child).target(node).build()));
        return node;
    }
}
