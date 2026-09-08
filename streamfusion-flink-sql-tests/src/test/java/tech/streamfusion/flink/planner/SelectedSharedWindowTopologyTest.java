/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraphGenerator;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.AggregatePhaseStrategy;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.types.Row;
import org.apache.flink.util.InstantiationUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchSerializer;
import tech.streamfusion.flink.operator.StreamFusionNativeRegionOperatorFactory;
import tech.streamfusion.flink.window.NativeLocalWindowResources;
import tech.streamfusion.proto.plan.v1.NativeRegionPlan;

@ResourceLock(Resources.SYSTEM_PROPERTIES)
class SelectedSharedWindowTopologyTest {
    @Test
    void selectedReuseHasOneOwnerTwoArrowExitsAndOriginalStateAndBufferBindings() throws Exception {
        var originalFactory = System.getProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        var originalProcessor = System.getProperty(StreamFusionPlannerFactory.EXEC_GRAPH_PROCESSOR_PROPERTY);
        try {
            for (int parallelism : List.of(1, 2)) {
                System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
                System.setProperty(
                        StreamFusionPlannerFactory.EXEC_GRAPH_PROCESSOR_PROPERTY,
                        SelectedLocalWindowSqlProbe.class.getName());
                var env = StreamExecutionEnvironment.getExecutionEnvironment();
                env.setParallelism(parallelism);
                var source = env.fromCollection(
                        List.of(Row.of(1L, LocalDateTime.of(2026, 1, 1, 0, 0))),
                        Types.ROW_NAMED(new String[] {"k", "ts"}, Types.LONG, Types.LOCAL_DATE_TIME));
                var tables = StreamTableEnvironment.create(env);
                tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ASYNC_STATE_ENABLED, false);
                // The Nexmark benchmark selects Flink's binary MultiJoin path.
                tables.getConfig().set(OptimizerConfigOptions.TABLE_OPTIMIZER_MULTI_JOIN_ENABLED, true);
                tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, false);
                tables.getConfig()
                        .set(
                                OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY,
                                AggregatePhaseStrategy.TWO_PHASE);
                tables.createTemporaryView(
                        "window_input",
                        source,
                        Schema.newBuilder()
                                .column("k", DataTypes.BIGINT())
                                .column("ts", DataTypes.TIMESTAMP(3).notNull())
                                .watermark("ts", "ts")
                                .build());
                var output = tables.toChangelogStream(
                        tables.sqlQuery(
                                "WITH counts AS (" + SelectedLocalWindowTopologyTest.COUNTS
                                        + ") SELECT a.k, a.n FROM counts a JOIN (SELECT MAX(n) m, s, e FROM counts GROUP BY s, e) b ON a.s=b.s AND a.e=b.e AND a.n>=b.m"));
                assertThat(SelectedLocalWindowSqlProbe.originals)
                        .extracting(node -> node.getClass().getSimpleName())
                        .contains("StreamExecMultiJoin")
                        .doesNotContain("StreamExecWindowJoin");
                var sharedStages = SelectedLocalWindowSqlProbe.selected.stream()
                        .filter(node -> node instanceof StreamFusionNativePlanNode
                                && ((StreamFusionNativePlanNode) node)
                                                .nativeMetadata()
                                                .sharedRegion()
                                        != null)
                        .collect(java.util.stream.Collectors.toList());
                assertThat(sharedStages).hasSize(3);
                var sharedOwner = ((StreamFusionNativePlanNode) sharedStages.get(0))
                        .nativeMetadata()
                        .sharedRegion();
                assertThat(sharedStages).allSatisfy(node -> assertThat(((StreamFusionNativePlanNode) node)
                                .nativeMetadata()
                                .sharedRegion())
                        .isSameAs(sharedOwner));
                assertThat(sharedStages.stream()
                                .filter(node -> ((org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase<?>) node)
                                                .getTransformation()
                                        != null))
                        .hasSize(2);
                var sink =
                        output.addSink(new org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction<Row>() {});
                System.setProperty(
                        StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, StreamFusionPlannerFactory.class.getName());
                var graph = new StreamGraphGenerator(
                                List.of(sink.getTransformation()),
                                env.getConfig(),
                                env.getCheckpointConfig(),
                                new Configuration())
                        .generate();
                int sharedOwners = 0;
                var stageIds = new HashSet<Long>();
                for (var node : graph.getStreamNodes()) {
                    if (!(node.getOperatorFactory() instanceof StreamFusionNativeRegionOperatorFactory)) continue;
                    var factory = (StreamFusionNativeRegionOperatorFactory) node.getOperatorFactory();
                    if (!(boolean) field(factory, "sharedRegion")) continue;
                    sharedOwners++;
                    var plan = NativeRegionPlan.parseFrom((byte[]) field(factory, "plan"));
                    assertThat(plan.getStagesCount()).isEqualTo(3);
                    assertThat(plan.getOutputStageIdsCount()).isEqualTo(2);
                    for (var stage : plan.getStagesList())
                        assertThat(stageIds.add(stage.getOperator().getPlanNodeId()))
                                .isTrue();
                    assertThat(node.getTypeSerializerOut()).isInstanceOf(ArrowRowDataBatchSerializer.class);
                    assertThat(node.getOutEdges()).hasSize(2);
                    assertThat(node.getOutEdges().stream().filter(edge -> edge.getOutputTag() != null))
                            .hasSize(1);
                    assertThat(node.getOutEdges().stream()
                                    .filter(edge -> edge.getOutputTag() != null)
                                    .findFirst()
                                    .orElseThrow()
                                    .getOutputTag())
                            .isEqualTo(factory.outputTag(1));
                    assertThat(field(factory, "stateIds"))
                            .isEqualTo(List.of(plan.getStages(0).getOperator().getPlanNodeId()));
                    assertThat(node.getManagedMemoryOperatorScopeUseCaseWeights())
                            .containsEntry(ManagedMemoryUseCase.OPERATOR, 8);
                    var restored = InstantiationUtil.clone(factory);
                    assertThat((boolean) field(restored, "sharedRegion")).isTrue();
                    var resources = (NativeLocalWindowResources) field(restored, "localWindowResources");
                    assertThat(resources.isPending()).isFalse();
                }
                assertThat(sharedOwners).isEqualTo(1);
                assertThat(stageIds).hasSize(3);
                assertThat(graph.getJobGraph().getNumberOfVertices()).isPositive();
            }
        } finally {
            restore(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, originalFactory);
            restore(StreamFusionPlannerFactory.EXEC_GRAPH_PROCESSOR_PROPERTY, originalProcessor);
        }
    }

    private static Object field(Object object, String name) throws Exception {
        var field = object.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(object);
    }

    private static void restore(String name, String value) {
        if (value == null) System.clearProperty(name);
        else System.setProperty(name, value);
    }
}
