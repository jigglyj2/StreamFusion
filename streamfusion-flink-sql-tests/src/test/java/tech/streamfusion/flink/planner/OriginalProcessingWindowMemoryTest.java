/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;
import static tech.streamfusion.flink.planner.OriginalMemoryPlanTest.*;

import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamGraphGenerator;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.window.NativeLocalWindowResources;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.NativeTaskBindings;

/** The original single-stage buffer must receive the same bytes as Flink after complete-pipeline finalization. */
@ResourceLock(Resources.SYSTEM_PROPERTIES)
class OriginalProcessingWindowMemoryTest {
    @ParameterizedTest
    @ValueSource(strings = {"hashmap", "rocksdb"})
    void originalProcessingBufferShareMatchesFlinkWithWeightedBoundaries(String backend) throws Exception {
        for (boolean union : List.of(false, true))
            for (boolean isolatedSink : List.of(false, true)) {
                String factory = System.getProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
                String processor = System.getProperty(StreamFusionPlannerFactory.EXEC_GRAPH_PROCESSOR_PROPERTY);
                System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
                System.setProperty(StreamFusionPlannerFactory.EXEC_GRAPH_PROCESSOR_PROPERTY, Capture.class.getName());
                try {
                    var env = StreamExecutionEnvironment.getExecutionEnvironment();
                    env.setParallelism(2);
                    var type = Types.ROW_NAMED(new String[] {"k"}, Types.LONG);
                    var source = env.fromCollection(List.of(Row.of(1L)), type);
                    source.slotSharingGroup("ingress-a");
                    source.getTransformation()
                            .declareManagedMemoryUseCaseAtOperatorScope(ManagedMemoryUseCase.OPERATOR, 11);
                    source.getTransformation().declareManagedMemoryUseCaseAtSlotScope(ManagedMemoryUseCase.PYTHON);
                    org.apache.flink.streaming.api.datastream.DataStream<Row> input = source;
                    if (union) {
                        var other = env.fromCollection(List.of(Row.of(2L)), type);
                        other.slotSharingGroup("ingress-b");
                        other.getTransformation()
                                .declareManagedMemoryUseCaseAtOperatorScope(ManagedMemoryUseCase.OPERATOR, 13);
                        input = input.union(other);
                    }
                    var tables = StreamTableEnvironment.create(env);
                    tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ASYNC_STATE_ENABLED, false);
                    tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, false);
                    tables.createTemporaryView(
                            "weighted_processing",
                            tables.fromDataStream(
                                    input,
                                    Schema.newBuilder()
                                            .column("k", DataTypes.BIGINT())
                                            .build()));
                    var result = tables.toChangelogStream(
                            tables.sqlQuery("WITH b AS (SELECT k, PROCTIME() AS pt FROM weighted_processing) "
                                    + "SELECT k, COUNT(*), window_start, window_end FROM TABLE("
                                    + "TUMBLE(TABLE b, DESCRIPTOR(pt), INTERVAL '10' SECOND)) "
                                    + "GROUP BY k, window_start, window_end"));
                    var original = Capture.graph;
                    var nodes = allNodes(original);
                    assertThat(nodes.stream()
                                    .filter(node ->
                                            node.getClass().getSimpleName().equals("StreamExecLocalWindowAggregate")))
                            .isEmpty();
                    var windows = nodes.stream()
                            .filter(StreamFusionOriginalMemoryPlan::requiresOriginalBuffer)
                            .collect(java.util.stream.Collectors.toList());
                    assertThat(windows).hasSize(1);
                    var window = windows.get(0);
                    long id = (1L << 32) | Integer.toUnsignedLong(window.getId());
                    var resources = StreamFusionOriginalWindowResources.capture(original.getRootNodes());
                    assertThat(resources).isNotNull();
                    var aliases = new IdentityHashMap<Transformation<?>, ExecNode<?>>();
                    for (var root : original.getRootNodes()) {
                        aliases.put(cached(root), root);
                        resources.recordOutput(root, cached(root));
                    }
                    // This legacy sink is added after SQL translation, so its weight must be
                    // included only when it shares the original window's slot-sharing group.
                    var sink = result.addSink(
                            new org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction<Row>() {});
                    sink.getTransformation()
                            .declareManagedMemoryUseCaseAtOperatorScope(ManagedMemoryUseCase.OPERATOR, 7);
                    if (isolatedSink) sink.slotSharingGroup("sink-only");
                    var roots = List.<Transformation<?>>of(sink.getTransformation());
                    var shares = Capture.memory.resolve(OriginalMemoryPlanTest::cached, roots, aliases);
                    assertThat(shares).containsOnlyKeys(id);
                    var share = shares.get(id);
                    assertThat(share.operatorWeight).isEqualTo(1);
                    assertThat(share.groupOperatorWeight).isEqualTo(1 + (union ? 0 : 11) + (isolatedSink ? 0 : 7));
                    assertThat(share.groupUseCases)
                            .contains(ManagedMemoryUseCase.OPERATOR, ManagedMemoryUseCase.STATE_BACKEND);
                    var bindings = resources.resolver().apply(roots);
                    assertThat(bindings).containsOnlyKeys(id);
                    var config = new Configuration();
                    config.set(StateBackendOptions.STATE_BACKEND, backend);
                    var graph = new StreamGraphGenerator(roots, env.getConfig(), env.getCheckpointConfig(), config)
                            .generate();
                    var operators = new HashMap<Integer, StreamConfig>();
                    for (var vertex : graph.getJobGraph().getVertices())
                        operators.putAll(new StreamConfig(vertex.getConfiguration())
                                .getTransitiveChainedTaskConfigsWithSelf(
                                        getClass().getClassLoader()));
                    var flink = operators.get(cached(window).getId());
                    assertThat(flink).isNotNull();
                    // Match the original physical identity to the task protocol, not the fixture's ID.
                    var fixture = NativePlan.parseFrom(SharedProcessingWindowFixture.plan(10000));
                    var owner =
                            fixture.getRoot().getCalc().getInput().toBuilder().setPlanNodeId(id);
                    var plan = fixture.toBuilder().setRoot(owner).build().toByteArray();
                    var pending = NativeLocalWindowResources.pending(plan);
                    assertThat(pending.isPending()).isTrue();
                    var resolved = pending.resolvedFrom(bindings);
                    resolved.validate(plan);
                    try (var environment = new MockEnvironmentBuilder()
                            .setManagedMemorySize(64L << 20)
                            .build()) {
                        environment.getJobConfiguration().addAll(config);
                        double fraction = flink.getManagedMemoryFractionOperatorUseCaseOfSlot(
                                ManagedMemoryUseCase.OPERATOR,
                                config,
                                environment.getTaskManagerInfo().getConfiguration(),
                                getClass().getClassLoader());
                        long expected = environment.getMemoryManager().computeMemorySize(fraction);
                        assertThat(expected).isPositive();
                        var actual = NativeTaskBindings.parseFrom(resolved.resolve(environment, flink));
                        assertThat(actual.getProtocolVersion()).isEqualTo(2);
                        assertThat(actual.getBindingsCount()).isEqualTo(1);
                        assertThat(actual.getBindings(0).getPlanNodeId()).isEqualTo(id);
                        assertThat(actual.getBindings(0).getLocalWindowBuffer().getFlinkBufferMemoryBytes())
                                .isEqualTo(expected);
                        assertThat(actual.getBindings(0).getLocalWindowBuffer().getFlinkPageBytes())
                                .isEqualTo(environment.getMemoryManager().getPageSize());
                        // Replacement execution weights cannot change Flink's buffer geometry.
                        cached(window).declareManagedMemoryUseCaseAtOperatorScope(ManagedMemoryUseCase.OPERATOR, 99);
                        assertThat(resources.resolver().apply(roots).get(id).memoryBytes(environment, flink))
                                .isEqualTo(expected);
                    }
                } finally {
                    restore(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, factory);
                    restore(StreamFusionPlannerFactory.EXEC_GRAPH_PROCESSOR_PROPERTY, processor);
                    Capture.graph = null;
                    Capture.memory = null;
                }
            }
    }
}
