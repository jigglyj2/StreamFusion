/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
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
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph;
import org.apache.flink.table.planner.plan.nodes.exec.processor.ExecNodeGraphProcessor;
import org.apache.flink.table.planner.plan.nodes.exec.processor.ProcessorContext;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.memory.FlinkOperatorMemoryShare;

@ResourceLock(Resources.SYSTEM_PROPERTIES)
public class OriginalMemoryPlanTest {
    @Test
    void originalSharesMatchFlinkJobGraphWithReuseWeightedBoundariesAndSlotGroups() throws Exception {
        for (boolean union : List.of(false, true))
            for (boolean isolatedSink : List.of(false, true))
                for (boolean sinkV2 : List.of(false, true)) {
                    String factory = System.getProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
                    String processor = System.getProperty(StreamFusionPlannerFactory.EXEC_GRAPH_PROCESSOR_PROPERTY);
                    System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
                    System.setProperty(
                            StreamFusionPlannerFactory.EXEC_GRAPH_PROCESSOR_PROPERTY, Capture.class.getName());
                    try {
                        var env = StreamExecutionEnvironment.getExecutionEnvironment();
                        env.setParallelism(2);
                        var type = Types.ROW_NAMED(new String[] {"k", "ts"}, Types.LONG, Types.LOCAL_DATE_TIME);
                        var source = env.fromCollection(List.of(Row.of(1L, LocalDateTime.of(2026, 1, 1, 0, 0))), type);
                        source.slotSharingGroup("ingress-a");
                        source.getTransformation()
                                .declareManagedMemoryUseCaseAtOperatorScope(ManagedMemoryUseCase.OPERATOR, 11);
                        source.getTransformation().declareManagedMemoryUseCaseAtSlotScope(ManagedMemoryUseCase.PYTHON);
                        org.apache.flink.streaming.api.datastream.DataStream<Row> input = source;
                        if (union) {
                            var other =
                                    env.fromCollection(List.of(Row.of(2L, LocalDateTime.of(2026, 1, 1, 0, 0))), type);
                            other.slotSharingGroup("ingress-b");
                            other.getTransformation()
                                    .declareManagedMemoryUseCaseAtOperatorScope(ManagedMemoryUseCase.OPERATOR, 13);
                            input = input.union(other);
                        }
                        var tables = StreamTableEnvironment.create(env);
                        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ASYNC_STATE_ENABLED, false);
                        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, false);
                        tables.getConfig()
                                .set(
                                        OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY,
                                        AggregatePhaseStrategy.TWO_PHASE);
                        tables.createTemporaryView(
                                "weighted_input",
                                tables.fromDataStream(
                                        input,
                                        Schema.newBuilder()
                                                .column("k", DataTypes.BIGINT())
                                                .column("ts", DataTypes.TIMESTAMP(3))
                                                .watermark("ts", "ts")
                                                .build()));
                        var output = tables.toChangelogStream(
                                tables.sqlQuery("WITH counts AS (SELECT k, COUNT(*) n, window_start s, window_end e "
                                        + "FROM TABLE(HOP(TABLE weighted_input, DESCRIPTOR(ts), INTERVAL '2' SECOND, INTERVAL '6' SECOND)) "
                                        + "GROUP BY k, window_start, window_end) "
                                        + "SELECT a.k, a.n FROM counts a JOIN (SELECT MAX(n) m, s, e FROM counts GROUP BY s, e) b "
                                        + "ON a.s=b.s AND a.e=b.e AND a.n>=b.m"));
                        var original = Capture.graph;
                        var resourcePlan = Capture.memory;
                        assertThat(original).isNotNull();
                        // A real DataStream operator added AFTER SQL translation changes the slot total.
                        var sink = sinkV2
                                ? output.sinkTo(
                                        new org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink<Row>())
                                : output.addSink(
                                        new org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction<
                                                Row>() {});
                        sink.getTransformation()
                                .declareManagedMemoryUseCaseAtOperatorScope(ManagedMemoryUseCase.OPERATOR, 7);
                        if (isolatedSink) sink.slotSharingGroup("sink-only");
                        var aliases = new IdentityHashMap<Transformation<?>, ExecNode<?>>();
                        for (var root : original.getRootNodes()) aliases.put(cached(root), root);
                        var shares = resourcePlan.resolve(
                                OriginalMemoryPlanTest::cached, List.of(sink.getTransformation()), aliases);
                        assertThat(shares).hasSize(2);
                        var all = allNodes(original);
                        assertThat(all.stream().filter(node -> node.getClass()
                                        .getSimpleName()
                                        .equals("StreamExecGlobalWindowAggregate")))
                                .hasSize(2);
                        var parents = new IdentityHashMap<ExecNode<?>, Integer>();
                        for (var node : all)
                            for (var edge : node.getInputEdges()) parents.merge(edge.getSource(), 1, Integer::sum);
                        assertThat(parents.values()).anyMatch(count -> count > 1);
                        var config = new Configuration();
                        config.set(StateBackendOptions.STATE_BACKEND, "rocksdb");
                        var graph = new StreamGraphGenerator(
                                        List.of(sink.getTransformation()),
                                        env.getConfig(),
                                        env.getCheckpointConfig(),
                                        config)
                                .generate();
                        var operatorConfigs = new HashMap<Integer, StreamConfig>();
                        for (var vertex : graph.getJobGraph().getVertices()) {
                            var stream = new StreamConfig(vertex.getConfiguration());
                            operatorConfigs.putAll(stream.getTransitiveChainedTaskConfigsWithSelf(
                                    getClass().getClassLoader()));
                        }
                        try (var environment = new org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder()
                                .setManagedMemorySize(64L << 20)
                                .build()) {
                            for (var node : all) {
                                var share = shares.get((1L << 32) | Integer.toUnsignedLong(node.getId()));
                                if (share == null) continue;
                                var flink = operatorConfigs.get(cached(node).getId());
                                assertThat(flink).isNotNull();
                                double expected = flink.getManagedMemoryFractionOperatorUseCaseOfSlot(
                                        ManagedMemoryUseCase.OPERATOR,
                                        config,
                                        environment.getTaskManagerInfo().getConfiguration(),
                                        getClass().getClassLoader());
                                environment.getJobConfiguration().addAll(config);
                                var capacity = new FlinkOperatorMemoryShare(
                                        share.operatorWeight, share.groupOperatorWeight, share.groupUseCases);
                                assertThat(capacity.memoryBytes(environment, flink))
                                        .isEqualTo(
                                                environment.getMemoryManager().computeMemorySize(expected));
                                assertThat(share.operatorWeight).isEqualTo(1);
                                assertThat(share.groupOperatorWeight)
                                        .isEqualTo(4 + (union ? 0 : 11) + (isolatedSink || sinkV2 ? 0 : 7));
                            }
                        }
                        // Replacement buffer weights must not feed back into original capacity geometry.
                        for (var node : all)
                            if (node.getClass().getSimpleName().contains("WindowAggregate"))
                                cached(node)
                                        .declareManagedMemoryUseCaseAtOperatorScope(ManagedMemoryUseCase.OPERATOR, 99);
                        var changed = resourcePlan.resolve(
                                OriginalMemoryPlanTest::cached, List.of(sink.getTransformation()), aliases);
                        for (var entry : shares.entrySet()) {
                            assertThat(changed.get(entry.getKey()).groupOperatorWeight)
                                    .isEqualTo(entry.getValue().groupOperatorWeight);
                            assertThat(changed.get(entry.getKey()).groupUseCases)
                                    .isEqualTo(entry.getValue().groupUseCases);
                        }
                        assertThatThrownBy(() -> resourcePlan.resolve(
                                        OriginalMemoryPlanTest::cached, List.of(source.getTransformation()), Map.of()))
                                .hasMessageContaining("missing an original window-buffer output");
                        var expanding = output.sinkTo(new ExpandingSink());
                        assertThatThrownBy(() -> resourcePlan.resolve(
                                        OriginalMemoryPlanTest::cached,
                                        List.of(expanding.getTransformation()),
                                        aliases))
                                .hasMessageContaining("sink expansion resource contract");
                        assertThatThrownBy(() -> resourcePlan.resolve(
                                        ignored -> null, List.of(sink.getTransformation()), aliases))
                                .hasMessageContaining("has not been translated");
                    } finally {
                        restore(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, factory);
                        restore(StreamFusionPlannerFactory.EXEC_GRAPH_PROCESSOR_PROPERTY, processor);
                        Capture.graph = null;
                        Capture.memory = null;
                    }
                }
    }

    private static final class ExpandingSink
            extends org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink<Row>
            implements org.apache.flink.streaming.api.connector.sink2.SupportsPreWriteTopology<Row> {
        @Override
        public org.apache.flink.streaming.api.datastream.DataStream<Row> addPreWriteTopology(
                org.apache.flink.streaming.api.datastream.DataStream<Row> input) {
            throw new AssertionError("Resource inspection must not invoke connector topology construction");
        }
    }

    public static final class Capture implements ExecNodeGraphProcessor {
        static ExecNodeGraph graph;
        static StreamFusionOriginalMemoryPlan memory;

        @Override
        public ExecNodeGraph process(ExecNodeGraph value, ProcessorContext context) {
            graph = value;
            memory = new StreamFusionOriginalMemoryPlan(value);
            return value;
        }
    }

    static Transformation<?> cached(ExecNode<?> node) {
        try {
            var field = ExecNodeBase.class.getDeclaredField("transformation");
            field.setAccessible(true);
            return (Transformation<?>) field.get(node);
        } catch (ReflectiveOperationException failure) {
            throw new IllegalStateException(failure);
        }
    }

    static List<ExecNode<?>> allNodes(ExecNodeGraph graph) {
        var nodes = new IdentityHashMap<ExecNode<?>, Boolean>();
        var pending = new ArrayList<>(graph.getRootNodes());
        while (!pending.isEmpty()) {
            var node = pending.remove(pending.size() - 1);
            if (nodes.put(node, true) != null) continue;
            for (var edge : node.getInputEdges()) pending.add(edge.getSource());
        }
        return List.copyOf(nodes.keySet());
    }

    static void restore(String key, String value) {
        if (value == null) System.clearProperty(key);
        else System.setProperty(key, value);
    }
}
