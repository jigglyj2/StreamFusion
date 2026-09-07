/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.metrics.groups.InternalOperatorMetricGroup;
import org.apache.flink.runtime.metrics.util.InterceptingOperatorMetricGroup;
import org.apache.flink.runtime.metrics.util.InterceptingTaskMetricGroup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.junit.jupiter.api.Test;
import tech.streamfusion.proto.plan.v1.Calc;
import tech.streamfusion.proto.plan.v1.Input;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;

class StreamFusionNativeUidMetricsTest {
    @Test
    void explicitUidMetricIdsMatchRealFlinkJobGraphsIndependentlyOfRegionOwner() throws Exception {
        for (String uid : List.of("42_calc", "計算/é")) {
            var environment = StreamExecutionEnvironment.getExecutionEnvironment();
            environment.setParallelism(1);
            environment
                    .fromData(1)
                    .map((MapFunction<Integer, Integer>) value -> value)
                    .uid(uid)
                    .name("uid-stage")
                    .disableChaining()
                    .sinkTo(new DiscardingSink<>());
            var graph = environment.getStreamGraph().getJobGraph(getClass().getClassLoader(), new JobID());
            var vertex = java.util.stream.StreamSupport.stream(
                            graph.getVertices().spliterator(), false)
                    .filter(node -> node.getName().equals("uid-stage"))
                    .findFirst()
                    .orElseThrow();
            var expected = vertex.getOperatorIDs().get(0).getGeneratedOperatorID();
            for (OperatorID runtime : List.of(new OperatorID(0, 0), new OperatorID(128, 512))) {
                var seen = new java.util.ArrayList<OperatorID>();
                var task = new InterceptingTaskMetricGroup() {
                    @Override
                    public InternalOperatorMetricGroup getOrAddOperator(
                            OperatorID id, String name, Map<String, String> variables) {
                        assertThat(name).isEqualTo("uid-stage");
                        seen.add(id);
                        return new InterceptingOperatorMetricGroup();
                    }
                };
                try (var tree =
                        StreamFusionNativeMetricTree.forRegion(plan(uid), runtime, task, new Configuration(), 0)) {
                    assertThat(seen).containsExactly(expected);
                    tree.update(new long[] {1, 1, 1, 2, 0, 1});
                }
            }
        }
    }

    @Test
    void invalidExplicitUidsAreRejectedBeforeMetricRegistration() throws Exception {
        var task = new InterceptingTaskMetricGroup() {
            @Override
            public InternalOperatorMetricGroup getOrAddOperator(
                    OperatorID id, String name, Map<String, String> variables) {
                throw new AssertionError("Invalid identity tree must not register metric groups");
            }
        };
        for (String uid : List.of("duplicate", "")) {
            var base = NativePlan.parseFrom(plan(uid));
            var root = base.getRoot().toBuilder()
                    .setPlanNodeId(3)
                    .setCalc(Calc.newBuilder().setInput(base.getRoot()));
            assertThatThrownBy(() -> StreamFusionNativeMetricTree.forRegion(
                            base.toBuilder().setRoot(root).build().toByteArray(),
                            new OperatorID(),
                            task,
                            new Configuration(),
                            0))
                    .hasMessageContaining(uid.isEmpty() ? "Empty string operator uid" : "Duplicate Flink operator UID");
        }
    }

    @Test
    void emptyUidIsInvalidInTheReferenceFlinkJobGraph() {
        var environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.fromData(1).uid("").sinkTo(new DiscardingSink<>());
        assertThatThrownBy(() ->
                        environment.getStreamGraph().getJobGraph(getClass().getClassLoader(), new JobID()))
                .hasMessageContaining("Empty string operator uid is not allowed");
    }

    private static byte[] plan(String uid) {
        return NativePlan.newBuilder()
                .setProtocolVersion(2)
                .setRoot(Operator.newBuilder()
                        .setPlanNodeId(1)
                        .setMetricName("uid-stage")
                        .setMetricUid(uid)
                        .setCalc(Calc.newBuilder()
                                .setInput(Operator.newBuilder().setPlanNodeId(2).setInput(Input.getDefaultInstance()))))
                .build()
                .toByteArray();
    }
}
