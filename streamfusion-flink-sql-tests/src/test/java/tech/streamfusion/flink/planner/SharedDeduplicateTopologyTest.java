/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.transformations.KeyedMultipleInputTransformation;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchTypeInfo;
import tech.streamfusion.flink.operator.StreamFusionNativeRegionOperatorFactory;
import tech.streamfusion.proto.plan.v1.NativePlan;

/** Checks selected-node plumbing while ordinary deduplication production admission remains separately gated. */
class SharedDeduplicateTopologyTest {
    @Test
    void deduplicateAndAdjacentCalcsHaveOneArrowRuntimeAndOneStateOwner() throws Exception {
        String previous = System.getProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        try {
            var planner = (PlannerBase)
                    ((TableEnvironmentImpl) TableEnvironment.create(EnvironmentSettings.inStreamingMode()))
                            .getPlanner();
            var config = new Configuration();
            var type = SharedDeduplicateMetricFixture.TYPE;
            var source = new NativeRegionTopologyTest.ArrowSource(type);
            var distribution = InputProperty.builder()
                    .requiredDistribution(InputProperty.hashDistribution(new int[] {0}))
                    .build();
            var exchange = unary(new StreamFusionExecExchange(config, distribution, type, "exchange"), source);
            var types = new FlinkTypeFactory(getClass().getClassLoader(), RelDataTypeSystem.DEFAULT);
            var rex = new RexBuilder(types);
            var projections = new java.util.ArrayList<org.apache.calcite.rex.RexNode>();
            for (int field = 0; field < type.getFieldCount(); field++)
                projections.add(rex.makeInputRef(types.createFieldTypeFromLogicalType(type.getTypeAt(field)), field));
            var before = unary(
                    new StreamFusionExecCalc(config, projections, null, InputProperty.DEFAULT, type, "before"),
                    exchange);
            var top = unary(
                    new StreamFusionExecDeduplicate(
                            config,
                            new int[] {0},
                            true,
                            true,
                            false,
                            true,
                            null,
                            InputProperty.DEFAULT,
                            type,
                            "deduplicate"),
                    before);
            var after = unary(
                    new StreamFusionExecCalc(config, projections, null, InputProperty.DEFAULT, type, "after"), top);
            var result = after.translateToPlan(planner);
            assertThat(result).isInstanceOf(KeyedMultipleInputTransformation.class);
            var owner = (KeyedMultipleInputTransformation<?>) result;
            assertThat(owner.getOutputType()).isSameAs(ArrowRowDataBatchTypeInfo.INSTANCE);
            assertThat(owner.getOperatorFactory()).isInstanceOf(StreamFusionNativeRegionOperatorFactory.class);
            assertThat(owner.getTransitivePredecessors().stream()
                            .filter(node -> node instanceof OneInputTransformation)
                            .count())
                    .isOne();
            var field = StreamFusionNativeRegionOperatorFactory.class.getDeclaredField("plan");
            field.setAccessible(true);
            var plan = NativePlan.parseFrom((byte[]) field.get(owner.getOperatorFactory()));
            var stage = plan.getRoot().getCalc().getInput();
            assertThat(plan.getProtocolVersion()).isEqualTo(3);
            assertThat(stage.hasDeduplicate()).isTrue();
            assertThat(stage.getPlanNodeId()).isEqualTo((1L << 32) | top.getId());
            assertThat(stage.getDeduplicate().getInput().hasCalc()).isTrue();
            var ids = StreamFusionNativeRegionOperatorFactory.class.getDeclaredField("stateIds");
            ids.setAccessible(true);
            assertThat(ids.get(owner.getOperatorFactory())).isEqualTo(List.of(stage.getPlanNodeId()));
            assertThat(before.getTransformation()).isNull();
            assertThat(top.getTransformation()).isNull();
            assertThat(source.translations).isOne();
        } finally {
            if (previous == null) System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
            else System.setProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, previous);
        }
    }

    private static <T extends ExecNode<?>> T unary(T node, ExecNode<?> input) {
        node.setInputEdges(List.of(ExecEdge.builder().source(input).target(node).build()));
        return node;
    }
}
