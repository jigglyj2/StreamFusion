/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;

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
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.logical.SessionWindowSpec;
import org.apache.flink.table.planner.plan.logical.TimeAttributeWindowingStrategy;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecWindowAggregate;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchTypeInfo;
import tech.streamfusion.flink.operator.StreamFusionNativeRegionOperatorFactory;
import tech.streamfusion.proto.plan.v1.NativePlan;

/** Session windows compose with adjacent Calcs and retain the Flink physical metric identity. */
class SharedSessionWindowTopologyTest {
    static final TimestampType ROWTIME = new TimestampType(false, TimestampKind.ROWTIME, 3);
    static final RowType RAW = RowType.of(new BigIntType(), ROWTIME);

    @Test
    void sessionsAndAdjacentCalcsRetainOriginalIdentityInOneArrowRuntime() throws Exception {
        String previous = System.getProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        try {
            var tables = (TableEnvironmentImpl) TableEnvironment.create(EnvironmentSettings.inStreamingMode());
            var planner = (PlannerBase) tables.getPlanner();
            var config = SharedSlicingWindowFixture.config();
            var source = new NativeRegionTopologyTest.ArrowSource(RAW);
            var distribution = InputProperty.builder()
                    .requiredDistribution(InputProperty.hashDistribution(new int[] {0}))
                    .build();
            var exchange = unary(new StreamFusionExecExchange(config, distribution, RAW, "exchange"), source);
            var before = calc(planner, config, exchange, RAW);
            var strategy = new TimeAttributeWindowingStrategy(
                    new SessionWindowSpec(Duration.ofSeconds(10), new int[] {0}), ROWTIME, 1);
            var calls = new AggregateCall[] {
                SharedSlicingWindowFixture.call(SqlStdOperatorTable.COUNT, List.of(), new BigIntType(false))
            };
            var session = unary(
                    new StreamFusionExecWindowAggregate(
                            config,
                            new int[] {0},
                            calls,
                            strategy,
                            SharedSlicingWindowFixture.properties(),
                            false,
                            InputProperty.DEFAULT,
                            SharedSessionWindowFixture.OUTPUT,
                            "session"),
                    before);
            var original = new StreamExecWindowAggregate(
                    config,
                    new int[] {0},
                    calls,
                    strategy,
                    SharedSlicingWindowFixture.properties(),
                    false,
                    InputProperty.DEFAULT,
                    SharedSessionWindowFixture.OUTPUT,
                    "original session window");
            original.setCompiled(true);
            new StreamFusionGraphRewrite().convert(original, ignored -> session);
            var after = calc(planner, config, session, SharedSessionWindowFixture.OUTPUT);
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
            assertThat(stage.getWindowAggregate().getKind())
                    .isEqualTo(tech.streamfusion.proto.plan.v1.WindowKind.WINDOW_KIND_SESSION);
            assertThat(stage.getWindowAggregate()
                            .getInput()
                            .getCalc()
                            .getInput()
                            .hasInput())
                    .isTrue();
            assertThat(stage.getPlanNodeId()).isEqualTo((1L << 32) | original.getId());
            assertThat(stage.getMetricName())
                    .isEqualTo(session.nativeMetadata().metricName(session, tables.getConfig()));
            assertThat(stage.getMetricUid()).isEqualTo(session.nativeMetadata().metricUid(tables.getConfig()));
            var ids = StreamFusionNativeRegionOperatorFactory.class.getDeclaredField("stateIds");
            ids.setAccessible(true);
            assertThat(ids.get(owner.getOperatorFactory())).isEqualTo(List.of(stage.getPlanNodeId()));
            assertThat(original.getTransformation()).isNull();
            assertThat(before.getTransformation()).isNull();
            assertThat(session.getTransformation()).isNull();
            assertThat(source.translations).isOne();
        } finally {
            if (previous == null) System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
            else System.setProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, previous);
        }
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
