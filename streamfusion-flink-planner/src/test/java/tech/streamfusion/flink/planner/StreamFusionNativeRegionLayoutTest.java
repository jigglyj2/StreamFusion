/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecDropUpdateBefore;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

class StreamFusionNativeRegionLayoutTest {
    @Test
    void diamondAndExposedIntermediateHaveOneDefinitionAndOrderedReferences() {
        var source = node("source");
        var shared = node("native-shared", source);
        var left = node("native-left", shared);
        var right = node("native-right", shared);
        var merge = node("native-merge", left, right, left);
        var exit = node("exchange", shared);
        var layout = discover(List.of(merge, exit));
        layout.validateBoundaryGraph();
        assertThat(layout.regions).hasSize(1);
        var region = layout.owner(shared);
        assertThat(region.stages).containsExactly(shared, left, right, merge);
        assertThat(region.inputs).containsExactly(source);
        assertThat(region.outputs).containsExactly(shared, merge);
        assertThat(region.stageInputs.get(0).get(0).external).isTrue();
        assertThat(region.stageInputs.get(1).get(0).external).isFalse();
        assertThat(region.stageInputs.get(1).get(0).index).isZero();
        assertThat(region.stageInputs.get(2).get(0).index).isZero();
        assertThat(region.stageInputs.get(3)).extracting(ref -> ref.index).containsExactly(1, 2, 1);
        assertThat(layout.sharedInternalStages()).containsKeys(shared, left);
        assertThat(layout.sharedInternalStages().get(shared)).hasSize(3);
        // Port bindings are a snapshot, independent of later Flink graph rewrites.
        left.setInputEdges(List.of());
        assertThat(region.stageInputs.get(1).get(0).index).isZero();
    }

    @Test
    void repeatedExternalSourceRetainsDistinctFlinkInputChannels() {
        var source = node("source");
        var left = node("native-left", source);
        var right = node("native-right", source);
        var merge = node("native-merge", left, right);
        var layout = discover(List.of(merge, merge));
        assertThat(layout.regions).hasSize(1);
        var region = layout.owner(merge);
        assertThat(region.inputs).containsExactly(source, source);
        assertThat(region.outputs).containsExactly(merge);
        for (int stage : List.of(0, 1)) {
            assertThat(region.stageInputs.get(stage).get(0).external).isTrue();
            assertThat(region.stageInputs.get(stage).get(0).index).isEqualTo(stage);
        }
        assertThat(layout.sharedInternalStages()).isEmpty();
    }

    @Test
    void keepsFlinkExchangesAndRejectsContractionAcrossAnIndirectSelfDependency() {
        var source = node("source");
        var first = node("native-first", source);
        var exchange = node("exchange", first);
        var last = node("native-last", exchange);
        var split = discover(List.of(last));
        split.validateBoundaryGraph();
        assertThat(split.regions).hasSize(2);
        assertThat(split.owner(last).inputs).containsExactly(exchange);
        assertThat(split.owner(first).outputs).containsExactly(first);
        var joined = node("native-join", first, last);
        var cyclic = discover(List.of(joined));
        assertThatThrownBy(cyclic::validateBoundaryGraph).hasMessageContaining("cycle across a Flink boundary");
        exchange.setInputEdges(List.of());
        assertThatThrownBy(cyclic::validateBoundaryGraph).hasMessageContaining("cycle across a Flink boundary");
        split.validateBoundaryGraph();
    }

    @Test
    void rejectsPhysicalCyclesWithoutTreatingThemAsReuse() {
        var cycle = node("native-cycle");
        cycle.setInputEdges(
                List.of(ExecEdge.builder().source(cycle).target(cycle).build()));
        assertThatThrownBy(() -> discover(List.of(cycle))).hasMessageContaining("physical-plan cycle");
    }

    private static StreamFusionNativeRegionLayout discover(List<ExecNode<?>> roots) {
        return StreamFusionNativeRegionLayout.discover(
                new ExecNodeGraph(roots), node -> node.getDescription().startsWith("native-"));
    }

    private static StreamExecDropUpdateBefore node(String name, ExecNode<?>... inputs) {
        var node = new StreamExecDropUpdateBefore(
                new Configuration(), InputProperty.DEFAULT, RowType.of(new IntType()), name);
        node.setInputEdges(java.util.Arrays.stream(inputs)
                .map(input -> ExecEdge.builder().source(input).target(node).build())
                .collect(java.util.stream.Collectors.toList()));
        return node;
    }
}
