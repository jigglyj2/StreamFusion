/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamGraphGenerator;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.AggregatePhaseStrategy;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.InstantiationUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.operator.StreamFusionNativeRegionOperatorFactory;
import tech.streamfusion.flink.window.NativeLocalWindowResources;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.NativeTaskBindings;
import tech.streamfusion.proto.plan.v1.Operator;

@ResourceLock(Resources.SYSTEM_PROPERTIES)
class SelectedLocalWindowTopologyTest {
    static final String COUNTS = "SELECT k, COUNT(*) n, window_start s, window_end e "
            + "FROM TABLE(HOP(TABLE window_input, DESCRIPTOR(ts), INTERVAL '2' SECOND, INTERVAL '6' SECOND)) "
            + "GROUP BY k, window_start, window_end";

    @Test
    void generatedLocalAndAttachedStagesUseOriginalIdentitiesAndCompletePipelineResources() throws Exception {
        String factory = System.getProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        String processor = System.getProperty(StreamFusionPlannerFactory.EXEC_GRAPH_PROCESSOR_PROPERTY);
        try {
            for (boolean attached : List.of(false, true)) {
                System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
                System.setProperty(
                        StreamFusionPlannerFactory.EXEC_GRAPH_PROCESSOR_PROPERTY,
                        SelectedLocalWindowSqlProbe.class.getName());
                var env = StreamExecutionEnvironment.getExecutionEnvironment();
                env.setParallelism(1);
                var source = env.fromCollection(
                        List.of(Row.of(1L, LocalDateTime.of(2026, 1, 1, 0, 0))),
                        Types.ROW_NAMED(new String[] {"k", "ts"}, Types.LONG, Types.LOCAL_DATE_TIME));
                source.getTransformation()
                        .declareManagedMemoryUseCaseAtOperatorScope(ManagedMemoryUseCase.OPERATOR, 11);
                var tables = StreamTableEnvironment.create(env);
                tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ASYNC_STATE_ENABLED, false);
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
                String sql = attached ? "SELECT MAX(n) m, s, e FROM (" + COUNTS + ") counts GROUP BY s, e" : COUNTS;
                var output = tables.toDataStream(tables.sqlQuery(sql));
                var downstream =
                        output.addSink(new org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction<Row>() {});
                downstream
                        .getTransformation()
                        .declareManagedMemoryUseCaseAtOperatorScope(ManagedMemoryUseCase.OPERATOR, 6);
                var originalLocals = new HashSet<Long>();
                var originalGlobals = new HashSet<Long>();
                for (var node : SelectedLocalWindowSqlProbe.originals) {
                    if (node.getClass().getSimpleName().equals("StreamExecLocalWindowAggregate"))
                        originalLocals.add((1L << 32) | node.getId());
                    if (node.getClass().getSimpleName().equals("StreamExecGlobalWindowAggregate"))
                        originalGlobals.add((1L << 32) | node.getId());
                    if (originalLocals.contains((1L << 32) | node.getId())
                            || originalGlobals.contains((1L << 32) | node.getId()))
                        assertThat(((ExecNodeBase<?>) node).getTransformation()).isNull();
                }
                assertThat(originalLocals).hasSize(attached ? 2 : 1);
                assertThat(originalGlobals).hasSameSizeAs(originalLocals);
                System.setProperty(
                        StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, StreamFusionPlannerFactory.class.getName());
                var graph = new StreamGraphGenerator(
                                List.of(downstream.getTransformation()),
                                env.getConfig(),
                                env.getCheckpointConfig(),
                                new Configuration())
                        .generate();
                var localIds = new HashSet<Long>();
                var globalIds = new HashSet<Long>();
                boolean combined = false;
                for (var node : graph.getStreamNodes()) {
                    if (!(node.getOperatorFactory() instanceof StreamFusionNativeRegionOperatorFactory)) continue;
                    var owner = (StreamFusionNativeRegionOperatorFactory) node.getOperatorFactory();
                    var plan = NativePlan.parseFrom((byte[]) field(owner, "plan"));
                    var stages = stages(plan.getRoot());
                    boolean local = false;
                    boolean global = false;
                    for (var stage : stages) {
                        if (stage.hasLocalWindowAggregate()) {
                            assertThat(localIds.add(stage.getPlanNodeId())).isTrue();
                            local = true;
                        }
                        if (stage.hasWindowAggregate()) {
                            assertThat(globalIds.add(stage.getPlanNodeId())).isTrue();
                            global = true;
                        }
                    }
                    if (local && !global) {
                        assertThat(node.getInEdges()).hasSize(1);
                        var sourceNode =
                                graph.getStreamNode(node.getInEdges().get(0).getSourceId());
                        // The local stage's source edge is Arrow C Data, with no synthetic IPC writer.
                        assertThat(sourceNode.getTypeSerializerOut())
                                .isInstanceOf(tech.streamfusion.flink.arrow.ArrowRowDataBatchSerializer.class);
                        assertThat(sourceNode.getOperatorName()).doesNotContain("exchange");
                    }
                    combined |= local && global;
                    assertThat(InstantiationUtil.clone(owner)).isNotSameAs(owner);
                    if (!local) continue;
                    var resources = (NativeLocalWindowResources) field(owner, "localWindowResources");
                    try (var environment = new org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder()
                            .setManagedMemorySize(64L << 20)
                            .build()) {
                        var config = new StreamConfig(new Configuration());
                        config.setStateBackendUsesManagedMemory(false);
                        var bindings = NativeTaskBindings.parseFrom(resources.resolve(environment, config));
                        assertThat(bindings.getBindingsList()).hasSize(1);
                        long capacity =
                                bindings.getBindings(0).getLocalWindowBuffer().getFlinkBufferMemoryBytes();
                        // Original Flink local/global weights are one each, plus source 11 and downstream 6.
                        double fraction =
                                org.apache.flink.runtime.util.config.memory.ManagedMemoryUtils.getFractionRoundedDown(
                                        1, 17 + 2 * originalLocals.size());
                        assertThat(capacity)
                                .isEqualTo(environment.getMemoryManager().computeMemorySize(fraction));
                    }
                }
                assertThat(localIds).isEqualTo(originalLocals);
                assertThat(globalIds).isEqualTo(originalGlobals);
                assertThat(combined).isEqualTo(attached);
                assertThat(graph.getJobGraph().getNumberOfVertices()).isPositive();
            }
        } finally {
            restore(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, factory);
            restore(StreamFusionPlannerFactory.EXEC_GRAPH_PROCESSOR_PROPERTY, processor);
        }
    }

    private static Object field(Object owner, String name) throws Exception {
        var field = owner.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(owner);
    }

    private static List<Operator> stages(Operator root) throws Exception {
        var result = new ArrayList<Operator>();
        result.add(root);
        for (var value : root.getAllFields().values()) {
            if (!(value instanceof com.google.protobuf.Message)) continue;
            var message = (com.google.protobuf.Message) value;
            for (var field : message.getAllFields().values()) {
                if (field instanceof Operator) result.addAll(stages((Operator) field));
                else if (field instanceof List)
                    for (var child : (List<?>) field)
                        if (child instanceof Operator) result.addAll(stages((Operator) child));
            }
        }
        return result;
    }

    private static void restore(String key, String value) {
        if (value == null) System.clearProperty(key);
        else System.setProperty(key, value);
    }
}
