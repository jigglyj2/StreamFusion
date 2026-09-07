/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Meter;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.metrics.groups.InternalOperatorMetricGroup;
import org.apache.flink.runtime.metrics.util.InterceptingOperatorMetricGroup;
import org.apache.flink.runtime.metrics.util.InterceptingTaskMetricGroup;
import org.junit.jupiter.api.Test;
import tech.streamfusion.proto.plan.v1.Calc;
import tech.streamfusion.proto.plan.v1.Input;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;

class StreamFusionNativeMetricTreeTest {
    @Test
    void publishesOriginalPlannerNamesForRootAndInternalStagesWithoutRenamingThem() throws Exception {
        var names = new java.util.HashMap<Long, String>();
        var task = new InterceptingTaskMetricGroup() {
            @Override
            public InternalOperatorMetricGroup getOrAddOperator(
                    OperatorID id, String name, Map<String, String> variables) {
                names.put(java.nio.ByteBuffer.wrap(id.getBytes()).getLong(8), name);
                return new InterceptingOperatorMetricGroup();
            }
        };
        var original = NativePlan.parseFrom(plan());
        var root = original.getRoot().toBuilder().setMetricName("Calc(select=[résultat])");
        var child = root.getCalc().getInput().toBuilder().setMetricName("Calc[27]");
        root.setCalc(root.getCalc().toBuilder().setInput(child));
        try (var tree = StreamFusionNativeMetricTree.forRegion(
                original.toBuilder().setRoot(root).build().toByteArray(),
                new OperatorID(0, 0),
                task,
                new Configuration(),
                0)) {
            assertThat(names).containsExactlyInAnyOrderEntriesOf(Map.of(1L, "Calc(select=[résultat])", 2L, "Calc[27]"));
            tree.update(new long[] {1, 8, 5, 2, 12, 8, 3, 0, 12});
        }
    }

    @Test
    void regionRootHasItsOwnScopeAndFailureReportingPreservesTheExecutionCause() {
        var groups = new java.util.HashMap<Long, InterceptingOperatorMetricGroup>();
        var task = new InterceptingTaskMetricGroup() {
            @Override
            public InternalOperatorMetricGroup getOrAddOperator(
                    OperatorID id, String name, Map<String, String> variables) {
                var group = new InterceptingOperatorMetricGroup();
                groups.put(java.nio.ByteBuffer.wrap(id.getBytes()).getLong(8), group);
                return group;
            }
        };
        try (var tree =
                StreamFusionNativeMetricTree.forRegion(plan(), new OperatorID(0, 0), task, new Configuration(), 0)) {
            assertThat(groups).containsOnlyKeys(1L, 2L);
            assertThatThrownBy(tree::ownerInputRecords).hasMessageContaining("external inputs separately");
            var failure = new IllegalStateException("execution failed");
            tree.updateAfterFailure(() -> new long[0], failure);
            assertThat(failure.getSuppressed()).isEmpty();
            tree.updateAfterFailure(() -> new long[] {1, 8, 5, 2, 12, 8, 3, 0, 12}, failure);
            assertThat(groups.get(1L)
                            .getIOMetricGroup()
                            .getNumRecordsInCounter()
                            .getCount())
                    .isEqualTo(8);
            assertThat(groups.get(2L)
                            .getIOMetricGroup()
                            .getNumRecordsInCounter()
                            .getCount())
                    .isEqualTo(12);
            var reporting = new IllegalArgumentException("metric snapshot failed");
            tree.updateAfterFailure(
                    () -> {
                        throw reporting;
                    },
                    failure);
            assertThat(failure.getSuppressed()).containsExactly(reporting);
        }
        assertThatThrownBy(() -> StreamFusionNativeMetricTree.forRegion(
                        new byte[0], new OperatorID(), task, new Configuration(), 0))
                .hasMessageContaining("missing its root");
    }

    @Test
    void validatesPhysicalArityBeforeRegisteringAnyVirtualMetricGroups() {
        var task = new InterceptingTaskMetricGroup() {
            @Override
            public InternalOperatorMetricGroup getOrAddOperator(
                    OperatorID id, String name, Map<String, String> variables) {
                throw new AssertionError("Invalid metric trees must not leak registered groups");
            }
        };
        var bad = Operator.newBuilder()
                .setPlanNodeId(2)
                .setRegularJoin(tech.streamfusion.proto.plan.v1.RegularJoin.getDefaultInstance());
        byte[] bytes = NativePlan.newBuilder()
                .setRoot(Operator.newBuilder()
                        .setPlanNodeId(1)
                        .setCalc(Calc.newBuilder().setInput(bad)))
                .build()
                .toByteArray();
        assertThatThrownBy(
                        () -> new StreamFusionNativeMetricTree(bytes, new OperatorID(), task, new Configuration(), 0))
                .hasMessageContaining("physical inputs");
    }

    @Test
    void unionWiringRetainsNativeCountsWithoutInventingFlinkOperatorScopes() {
        var task = new InterceptingTaskMetricGroup() {
            @Override
            public InternalOperatorMetricGroup getOrAddOperator(
                    OperatorID id, String name, Map<String, String> variables) {
                throw new AssertionError("Flink CommonExecUnion has no operator metric scope: " + name);
            }
        };
        var edge = Operator.newBuilder()
                .setPlanNodeId(3)
                .setInput(Input.getDefaultInstance())
                .build();
        var union = Operator.newBuilder()
                .setPlanNodeId(2)
                .setUnion(tech.streamfusion.proto.plan.v1.Union.newBuilder().addInputs(edge))
                .build();
        byte[] plan = NativePlan.newBuilder()
                .setRoot(Operator.newBuilder()
                        .setPlanNodeId(1)
                        .setCalc(Calc.newBuilder().setInput(union)))
                .build()
                .toByteArray();
        try (var tree = new StreamFusionNativeMetricTree(plan, new OperatorID(), task, new Configuration(), 0)) {
            tree.update(new long[] {1, 4, 2, 2, 4, 4, 3, 0, 4});
            assertThat(tree.ownerInputRecords()).isEqualTo(4);
            tree.watermark(2, 100);
        }
    }

    @Test
    void findsLifecycleOwnerThroughArbitraryProtocolChildrenAndRejectsAmbiguity() {
        Operator owner = Operator.newBuilder()
                .setPlanNodeId(17)
                .setRegularJoin(tech.streamfusion.proto.plan.v1.RegularJoin.getDefaultInstance())
                .build();
        Operator root = Operator.newBuilder()
                .setPlanNodeId(1)
                .setExpand(tech.streamfusion.proto.plan.v1.Expand.newBuilder().setInput(owner))
                .build();
        byte[] bytes = NativePlan.newBuilder().setRoot(root).build().toByteArray();
        assertThat(StreamFusionNativeMetricTree.uniqueNodeId(bytes, Operator.OperatorCase.REGULAR_JOIN))
                .isEqualTo(17);
        assertThatThrownBy(() -> StreamFusionNativeMetricTree.uniqueNodeId(bytes, Operator.OperatorCase.DEDUPLICATE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exactly one");
        byte[] duplicate = NativePlan.newBuilder()
                .setRoot(Operator.newBuilder()
                        .setUnion(tech.streamfusion.proto.plan.v1.Union.newBuilder()
                                .addInputs(owner)
                                .addInputs(owner)))
                .build()
                .toByteArray();
        assertThatThrownBy(
                        () -> StreamFusionNativeMetricTree.uniqueNodeId(duplicate, Operator.OperatorCase.REGULAR_JOIN))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exactly one");
    }

    @Test
    void persistentLifecycleOwnerDoesNotStealTheOutputCalcsMetricScope() {
        InterceptingOperatorMetricGroup outputCalc = new InterceptingOperatorMetricGroup();
        InterceptingTaskMetricGroup task = new InterceptingTaskMetricGroup() {
            @Override
            public InternalOperatorMetricGroup getOrAddOperator(
                    OperatorID id, String name, Map<String, String> variables) {
                assertThat(name).isEqualTo("Calc");
                return outputCalc;
            }
        };
        try (StreamFusionNativeMetricTree tree =
                new StreamFusionNativeMetricTree(plan(), new OperatorID(), task, new Configuration(), 0, 2)) {
            tree.update(new long[] {1, 10, 3, 2, 7, 10, 3, 0, 7});
            assertThat(((Counter) outputCalc.get("numRecordsIn")).getCount()).isEqualTo(10);
            assertThat(((Counter) outputCalc.get("numRecordsOut")).getCount()).isEqualTo(3);
        }
    }

    @Test
    void preservesFlinkIoMetricTypesAndCountsOnlyItsOwnStage() {
        InterceptingOperatorMetricGroup group = new InterceptingOperatorMetricGroup();
        InterceptingTaskMetricGroup task = new InterceptingTaskMetricGroup() {
            @Override
            public InternalOperatorMetricGroup getOrAddOperator(
                    OperatorID id, String name, Map<String, String> variables) {
                assertThat(name).isEqualTo("Calc");
                return group;
            }
        };
        try (StreamFusionNativeMetricTree tree =
                new StreamFusionNativeMetricTree(plan(), new OperatorID(), task, new Configuration(), 0)) {
            for (String name : new String[] {"numRecordsIn", "numRecordsOut", "numBytesIn", "numBytesOut"}) {
                assertThat(group.get(name)).isInstanceOf(Counter.class);
                assertThat(group.get(name + "PerSecond")).isInstanceOf(Meter.class);
            }
            tree.update(new long[] {1, 4, 2, 2, 5, 4, 3, 0, 5});
            tree.update(new long[] {1, 8, 4, 2, 10, 8, 3, 0, 10});
            assertThat(tree.ownerInputRecords()).isEqualTo(8);
            assertThat(((Counter) group.get("numRecordsIn")).getCount()).isEqualTo(10);
            assertThat(((Counter) group.get("numRecordsOut")).getCount()).isEqualTo(8);
            // Flink's chained operator byte counters are zero; network transport owns physical bytes.
            assertThat(((Counter) group.get("numBytesIn")).getCount()).isZero();
            tree.watermark(100);
            tree.watermark(90);
            assertThat(((Gauge<?>) group.get("currentInputWatermark")).getValue())
                    .isEqualTo(100L);
            tree.watermark(3, 900);
            assertThat(((Gauge<?>) group.get("currentOutputWatermark")).getValue())
                    .isEqualTo(100L);
            tree.watermark(2, 150);
            assertThat(((Gauge<?>) group.get("currentOutputWatermark")).getValue())
                    .isEqualTo(150L);
            assertThat(((Gauge<?>) group.get("currentInputWatermark")).getValue())
                    .isEqualTo(100L);
            tree.inputWatermark(2, 0, 120);
            assertThat(((Gauge<?>) group.get("currentInputWatermark")).getValue())
                    .isEqualTo(120L);
            assertThat(((Gauge<?>) group.get("currentOutputWatermark")).getValue())
                    .isEqualTo(150L);
            assertThatThrownBy(() -> tree.inputWatermark(2, 1, 900)).isInstanceOf(IndexOutOfBoundsException.class);
            assertThatThrownBy(() -> tree.watermark(999, 1000)).hasMessageContaining("Unknown");
            assertThat(((Gauge<?>) group.get("currentOutputWatermark")).getValue())
                    .isEqualTo(150L);
            assertThatThrownBy(() -> tree.update(new long[] {2, 11, 9, 2, 12, 10}))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThat(((Counter) group.get("numRecordsIn")).getCount()).isEqualTo(10);
            assertThatThrownBy(() -> tree.update(new long[] {2, 11, 9})).isInstanceOf(IllegalArgumentException.class);
            assertThat(((Counter) group.get("numRecordsIn")).getCount()).isEqualTo(10);
            assertThatThrownBy(() -> tree.update(new long[] {2, 9, 8})).isInstanceOf(IllegalArgumentException.class);
        }
    }

    private static byte[] plan() {
        Operator input = Operator.newBuilder()
                .setPlanNodeId(3)
                .setInput(Input.getDefaultInstance())
                .build();
        Operator child = Operator.newBuilder()
                .setPlanNodeId(2)
                .setCalc(Calc.newBuilder().setInput(input))
                .build();
        return NativePlan.newBuilder()
                .setRoot(Operator.newBuilder()
                        .setPlanNodeId(1)
                        .setCalc(Calc.newBuilder().setInput(child)))
                .build()
                .toByteArray();
    }
}
