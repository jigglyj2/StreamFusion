/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static tech.streamfusion.flink.planner.SharedLocalWindowFixture.*;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamGraphGenerator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.transformations.MultipleInputTransformation;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.util.InstantiationUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.memory.FlinkOperatorMemoryShare;
import tech.streamfusion.flink.operator.NativePipelineResourceFinalizer;
import tech.streamfusion.flink.operator.StreamFusionNativeRegionOperatorFactory;
import tech.streamfusion.flink.operator.StreamFusionNativeRegionTranslator;
import tech.streamfusion.flink.window.NativeLocalWindowResources;
import tech.streamfusion.proto.plan.v1.NativeTaskBindings;

@ResourceLock(Resources.SYSTEM_PROPERTIES)
class NativePipelineResourceFinalizerTest {
    @Test
    void patchedPipelineHookBindsFreshFactoriesForEachCompletePipeline() throws Exception {
        String previous = System.getProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        String plannerProcessor = System.getProperty(StreamFusionPlannerFactory.EXEC_GRAPH_PROCESSOR_PROPERTY);
        System.setProperty(
                StreamFusionPlannerFactory.EXEC_GRAPH_PROCESSOR_PROPERTY, "isolated.planner.NotVisibleToRuntime");
        System.setProperty(
                StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, StreamFusionPlannerFactory.class.getName());
        try {
            var env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            var input = input(env);
            var calls = new AtomicInteger();
            var external = Collections.newSetFromMap(new IdentityHashMap<Transformation<?>, Boolean>());
            external.add(input);
            Function<List<Transformation<?>>, Map<Long, FlinkOperatorMemoryShare>> resolver = roots -> {
                calls.incrementAndGet();
                // One original local weight plus the actual external source/downstream weights.
                int originalWeight = 1 + externalWeights(roots, external);
                return Map.of(
                        3L, new FlinkOperatorMemoryShare(1, originalWeight, Set.of(ManagedMemoryUseCase.OPERATOR)));
            };
            var selected = StreamFusionNativeRegionTranslator.translateInputsWithResources(
                    List.of(input), List.of(INPUT), PARTIAL, plan(), resolver);
            var pending = factory(selected);
            pending.setChainingStrategy(ChainingStrategy.NEVER);
            assertThatThrownBy(() -> InstantiationUtil.clone(pending))
                    .hasStackTraceContaining("complete-pipeline finalization");
            var graph1 = graph(env, List.of(selected));
            var first = (StreamFusionNativeRegionOperatorFactory)
                    graph1.getStreamNode(selected.getId()).getOperatorFactory();
            assertThat(first).isNotSameAs(pending);
            assertThat(first.getChainingStrategy()).isEqualTo(ChainingStrategy.NEVER);
            assertThat(capacity(InstantiationUtil.clone(first))).isEqualTo(32L << 20);
            // Add a user-owned operator after the already-selected SQL region.
            var sinkView = tech.streamfusion.flink.arrow.StreamFusionArrowBoundaries.toRowData(selected, PARTIAL);
            var downstream = new OneInputTransformation<>(
                    sinkView,
                    "external downstream",
                    new org.apache.flink.streaming.api.operators.StreamMap<RowData, RowData>(value -> value),
                    InternalTypeInfo.of(PARTIAL),
                    1);
            downstream.declareManagedMemoryUseCaseAtOperatorScope(ManagedMemoryUseCase.OPERATOR, 6);
            external.add(downstream);
            var graph2 = graph(env, List.of(downstream));
            var second = (StreamFusionNativeRegionOperatorFactory)
                    graph2.getStreamNode(selected.getId()).getOperatorFactory();
            assertThat(calls).hasValue(2);
            assertThat(second).isNotSameAs(first).isNotSameAs(pending);
            assertThat(capacity(InstantiationUtil.clone(second))).isEqualTo(8L << 20);
            assertThat(capacity(first)).isEqualTo(32L << 20);
            var graph3 = graph(env, List.of(selected));
            assertThat(capacity((StreamFusionNativeRegionOperatorFactory)
                            graph3.getStreamNode(selected.getId()).getOperatorFactory()))
                    .isEqualTo(32L << 20);
            assertThat(calls).hasValue(3);
            assertThat(factory(selected)).isSameAs(pending);
            assertThatThrownBy(() -> InstantiationUtil.clone(pending))
                    .hasStackTraceContaining("complete-pipeline finalization");
            // Actual Flink JobGraph serialization sees only resolved factory copies.
            assertThat(graph1.getJobGraph().getNumberOfVertices()).isPositive();
            assertThat(graph2.getJobGraph().getNumberOfVertices()).isPositive();
        } finally {
            restore(previous);
            if (plannerProcessor == null)
                System.clearProperty(StreamFusionPlannerFactory.EXEC_GRAPH_PROCESSOR_PROPERTY);
            else System.setProperty(StreamFusionPlannerFactory.EXEC_GRAPH_PROCESSOR_PROPERTY, plannerProcessor);
        }
    }

    @Test
    void resolvesSharedPlannerOnceAndPublishesNothingWhenAnotherOwnerFails() throws Exception {
        String previous = System.getProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        try {
            var env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            var input = input(env);
            var calls = new AtomicInteger();
            Function<List<Transformation<?>>, Map<Long, FlinkOperatorMemoryShare>> resolver = roots -> {
                calls.incrementAndGet();
                return Map.of(3L, new FlinkOperatorMemoryShare(1, 2, Set.of(ManagedMemoryUseCase.OPERATOR)));
            };
            var first = StreamFusionNativeRegionTranslator.translateInputsWithResources(
                    List.of(input), List.of(INPUT), PARTIAL, plan(), resolver);
            var second = StreamFusionNativeRegionTranslator.translateInputsWithResources(
                    List.of(input), List.of(INPUT), PARTIAL, plan(), resolver);
            var shared = graph(env, List.of(first, second));
            NativePipelineResourceFinalizer.finalizePipeline(shared, List.of(first, second));
            assertThat(calls).hasValue(1);
            assertThat(shared.getStreamNode(first.getId()).getOperatorFactory()).isNotSameAs(factory(first));
            assertThat(shared.getStreamNode(second.getId()).getOperatorFactory())
                    .isNotSameAs(factory(second));
            var failing = StreamFusionNativeRegionTranslator.translateInputsWithResources(
                    List.of(input), List.of(INPUT), PARTIAL, plan(), roots -> {
                        throw new IllegalArgumentException("unverified external resources");
                    });
            var atomic = graph(env, List.of(first, failing));
            assertThatThrownBy(() -> NativePipelineResourceFinalizer.finalizePipeline(atomic, List.of(first, failing)))
                    .hasMessageContaining("unverified external resources");
            assertThat(atomic.getStreamNode(first.getId()).getOperatorFactory()).isSameAs(factory(first));
            assertThat(atomic.getStreamNode(failing.getId()).getOperatorFactory())
                    .isSameAs(factory(failing));
        } finally {
            restore(previous);
        }
    }

    private static Transformation<RowData> input(StreamExecutionEnvironment env) {
        var source = env.fromCollection(
                List.<RowData>of(GenericRowData.of(1L, TimestampData.fromEpochMillis(1000))),
                InternalTypeInfo.of(INPUT));
        source.getTransformation().declareManagedMemoryUseCaseAtOperatorScope(ManagedMemoryUseCase.OPERATOR, 1);
        return source.getTransformation();
    }

    private static StreamGraph graph(StreamExecutionEnvironment env, List<Transformation<?>> roots) {
        return new StreamGraphGenerator(roots, env.getConfig(), env.getCheckpointConfig(), new Configuration())
                .generate();
    }

    private static StreamFusionNativeRegionOperatorFactory factory(Transformation<?> value) {
        return (StreamFusionNativeRegionOperatorFactory) ((MultipleInputTransformation<?>) value).getOperatorFactory();
    }

    private static int externalWeights(List<Transformation<?>> roots, Set<Transformation<?>> external) {
        var seen = Collections.newSetFromMap(new IdentityHashMap<Transformation<?>, Boolean>());
        var pending = new java.util.ArrayList<>(roots);
        int sum = 0;
        while (!pending.isEmpty()) {
            var value = pending.remove(pending.size() - 1);
            if (!seen.add(value)) continue;
            pending.addAll(value.getInputs());
            if (!external.contains(value)) continue;
            sum += value.getManagedMemoryOperatorScopeUseCaseWeights().getOrDefault(ManagedMemoryUseCase.OPERATOR, 0);
        }
        return sum;
    }

    private static long capacity(StreamFusionNativeRegionOperatorFactory factory) throws Exception {
        var field = StreamFusionNativeRegionOperatorFactory.class.getDeclaredField("localWindowResources");
        field.setAccessible(true);
        var resources = (NativeLocalWindowResources) field.get(factory);
        try (var environment = new org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder()
                .setManagedMemorySize(64L << 20)
                .build()) {
            var config = new StreamConfig(new Configuration());
            config.setStateBackendUsesManagedMemory(false);
            config.setManagedMemoryFractionOperatorOfUseCase(ManagedMemoryUseCase.OPERATOR, 1.0);
            return NativeTaskBindings.parseFrom(resources.resolve(environment, config))
                    .getBindings(0)
                    .getLocalWindowBuffer()
                    .getFlinkBufferMemoryBytes();
        }
    }

    private static void restore(String previous) {
        if (previous == null) System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        else System.setProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, previous);
    }
}
