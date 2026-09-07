/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecCalc;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecExpand;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecSort;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.exec.spec.JoinSpec;
import org.apache.flink.table.planner.plan.nodes.exec.spec.SortSpec;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecJoin;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

class StreamFusionArchitectureSupportTest {
    private final Configuration config = new Configuration();
    private final RowType type = RowType.of(new IntType(false));

    @Test
    void rejectsSharedInternalStagesBeforeAnyRootIsReplaced() {
        var source = new BatchExecTableSourceScan(config, null, type, "source");
        source.setInputEdges(List.of());
        var shared = unary(calc("shared"), source);
        var left = unary(calc("left"), shared);
        var right = unary(calc("right"), shared);
        ExecNodeGraph graph = new ExecNodeGraph(List.of(left, right));
        List<String> reasons = new ArrayList<>();
        StreamFusionArchitectureSupport.collect(graph, reasons);
        assertThat(reasons).hasSize(1);
        assertThat(reasons.get(0))
                .contains(
                        "multiple consumers",
                        "BatchExecCalc#" + left.getId(),
                        "BatchExecCalc#" + right.getId(),
                        "duplicate execution and per-stage metrics");
        assertThat(new StreamFusionExecGraphProcessor().process(graph, null)).isSameAs(graph);
        assertThat(left.getInputEdges().get(0).getSource()).isSameAs(shared);
        assertThat(right.getInputEdges().get(0).getSource()).isSameAs(shared);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("multi-output native region ownership");
    }

    @Test
    void countsRootOutputsAsConsumersButAllowsSharedSourcesAndRegionOutputs() {
        var source = new BatchExecTableSourceScan(config, null, type, "source");
        source.setInputEdges(List.of());
        var shared = unary(calc("shared"), source);
        var tail = unary(calc("tail"), shared);
        List<String> reasons = new ArrayList<>();
        StreamFusionArchitectureSupport.collect(new ExecNodeGraph(List.of(shared, tail)), reasons);
        assertThat(reasons).hasSize(1);
        assertThat(reasons.get(0)).contains("root[0]", "multiple consumers");
        reasons.clear();
        StreamFusionArchitectureSupport.collect(new ExecNodeGraph(List.of(shared, shared)), reasons);
        assertThat(reasons).isEmpty();
        StreamFusionArchitectureSupport.collect(
                new ExecNodeGraph(List.of(shared, unary(calc("separate"), source))), reasons);
        assertThat(reasons).isEmpty();
    }

    @Test
    void regularJoinReportsRemainingMemoryRestrictionWithoutObsoleteStorageOrOutputRestrictions() {
        var join = new StreamExecJoin(
                config,
                new JoinSpec(FlinkJoinType.INNER, new int[] {0}, new int[] {0}, new boolean[] {true}, null),
                List.of(),
                List.of(),
                InputProperty.DEFAULT,
                InputProperty.DEFAULT,
                Map.of(),
                RowType.of(new IntType(false), new IntType(false)),
                "join");
        join.setInputEdges(List.of());
        List<String> reasons = new ArrayList<>();
        StreamFusionArchitectureSupport.collect(new ExecNodeGraph(List.of(join)), reasons);
        assertThat(String.join("\n", reasons))
                .contains("retained-state/buffer admission")
                .doesNotContain("dirty-page", "whole-key", "fan-out is not yet drained");
    }

    @Test
    void rejectsTheWholeGraphWithoutMutatingAnyRootAndReportsEveryBlockedOperator() {
        var source = new BatchExecTableSourceScan(config, null, type, "source");
        source.setInputEdges(List.of());
        var calc = unary(calc("safe calc"), source);
        var sortA = unary(sort("first sort"), source);
        var sortB = unary(sort("second sort"), source);
        ExecEdge original = calc.getInputEdges().get(0);
        ExecNodeGraph graph = new ExecNodeGraph(List.of(calc, sortA, sortB));

        assertThat(new StreamFusionExecGraphProcessor().process(graph, null)).isSameAs(graph);
        assertThat(calc.getInputEdges()).containsExactly(original);
        assertThat(StreamFusionPlanningDiagnostics.explain())
                .contains(
                        "the entire plan will use Flink",
                        "root[1]/BatchExecSort",
                        "root[2]/BatchExecSort",
                        "Flink backend configuration parity");
    }

    @Test
    void admitsCalcChainsButRejectsUnverifiedNativeComposition() {
        var source = new BatchExecTableSourceScan(config, null, type, "source");
        source.setInputEdges(List.of());
        var inner = unary(calc("inner"), source);
        var outer = unary(calc("outer"), inner);
        List<String> reasons = new ArrayList<>();
        StreamFusionArchitectureSupport.collect(new ExecNodeGraph(List.of(outer)), reasons);
        assertThat(reasons).isEmpty();

        var expand = unary(
                new BatchExecExpand(config, List.of(List.of(reference())), InputProperty.DEFAULT, type, "expand"),
                inner);
        var root = unary(calc("root"), expand);
        StreamFusionArchitectureSupport.collect(new ExecNodeGraph(List.of(root)), reasons);
        assertThat(reasons).hasSize(2);
        assertThat(String.join("\n", reasons))
                .contains(
                        "BatchExecExpand -> BatchExecCalc",
                        "BatchExecCalc -> BatchExecExpand",
                        "intermediate JNI/Java handoff");
    }

    private BatchExecCalc calc(String name) {
        return new BatchExecCalc(config, List.of(reference()), null, InputProperty.DEFAULT, type, name);
    }

    @Test
    void aggregateCompositionIsAdmittedIndependentlyOfTheRemainingPersistentMemoryGate() {
        var source = new BatchExecTableSourceScan(config, null, type, "source boundary");
        source.setInputEdges(List.of());
        var before = unary(
                new org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecCalc(
                        config, List.of(reference()), null, InputProperty.DEFAULT, type, "before"),
                source);
        ExecNode<?> child = before;
        for (int stage = 0; stage < 2; stage++) {
            child = unary(
                    new org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecGroupAggregate(
                            config,
                            new int[] {0},
                            new org.apache.calcite.rel.core.AggregateCall[0],
                            new boolean[0],
                            true,
                            true,
                            null,
                            InputProperty.DEFAULT,
                            type,
                            "distinct " + stage),
                    child);
        }
        var root = unary(
                new org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecCalc(
                        config, List.of(reference()), null, InputProperty.DEFAULT, type, "after"),
                child);
        var reasons = new ArrayList<String>();
        StreamFusionArchitectureSupport.collect(new ExecNodeGraph(List.of(root)), reasons);
        assertThat(reasons).hasSize(2).allMatch(reason -> reason.contains("retained-state/buffer admission"));
        assertThat(String.join("\n", reasons)).doesNotContain("intermediate JNI", "fused native ExecutionPlan");
    }

    private BatchExecSort sort(String name) {
        return new BatchExecSort(
                config, SortSpec.builder().addField(0, true, false).build(), InputProperty.DEFAULT, type, name);
    }

    @Test
    void deduplicateUsesTheSharedRegionCapabilityWithoutRemovingItsMemoryGate() {
        var source = new BatchExecTableSourceScan(config, null, type, "source boundary");
        source.setInputEdges(List.of());
        var before = unary(
                new org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecCalc(
                        config, List.of(reference()), null, InputProperty.DEFAULT, type, "before"),
                source);
        var dedup = unary(
                new org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecDeduplicate(
                        config, new int[] {0}, false, false, true, false, InputProperty.DEFAULT, type, "dedup"),
                before);
        var after = unary(
                new org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecCalc(
                        config, List.of(reference()), null, InputProperty.DEFAULT, type, "after"),
                dedup);
        var reasons = new ArrayList<String>();
        StreamFusionArchitectureSupport.collect(new ExecNodeGraph(List.of(after)), reasons);
        assertThat(reasons).hasSize(1);
        assertThat(reasons.get(0))
                .contains("StreamExecDeduplicate", "retained-state/buffer admission")
                .doesNotContain("intermediate JNI", "fused native ExecutionPlan");
    }

    private RexInputRef reference() {
        return new RexInputRef(
                0,
                new FlinkTypeFactory(getClass().getClassLoader(), RelDataTypeSystem.DEFAULT)
                        .createSqlType(SqlTypeName.INTEGER));
    }

    private static <T extends ExecNode<?>> T unary(T node, ExecNode<?> input) {
        node.setInputEdges(List.of(ExecEdge.builder().source(input).target(node).build()));
        return node;
    }
}
