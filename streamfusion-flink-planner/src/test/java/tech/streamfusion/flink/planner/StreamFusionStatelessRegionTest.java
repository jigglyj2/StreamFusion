/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecTableSourceScan;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

class StreamFusionStatelessRegionTest {
    @Test
    void discoversArbitraryBranchingContractsButNeverDuplicatesSharedInternalStages() {
        RowType type = RowType.of(new IntType(false));
        var source = new BatchExecTableSourceScan(new Configuration(), null, type, "source");
        source.setInputEdges(List.of());
        var left = unary(new FutureNativeNode(type), source);
        var right = unary(new FutureNativeNode(type), source);
        var branch = new FutureNativeNode(type);
        branch.setInputEdges(List.of(
                ExecEdge.builder().source(left).target(branch).build(),
                ExecEdge.builder().source(right).target(branch).build()));
        var root = unary(new FutureNativeNode(type), branch);
        assertThat(StreamFusionStatelessRegion.stages(root)).containsExactly(left, right, branch, root);
        branch.replaceInputEdge(
                1, ExecEdge.builder().source(left).target(branch).build());
        assertThatThrownBy(() -> StreamFusionStatelessRegion.stages(root)).hasMessageContaining("shared internal");
    }

    @Test
    void discoversNewNativeNodesByContractAndRejectsCycles() {
        RowType type = RowType.of(new IntType(false));
        var source = new BatchExecTableSourceScan(new Configuration(), null, type, "source");
        source.setInputEdges(List.of());
        var first = unary(new FutureNativeNode(type), source);
        var second = unary(new FutureNativeNode(type), first);
        assertThat(StreamFusionStatelessRegion.stages(second)).containsExactly(first, second);
        unary(first, second);
        assertThatThrownBy(() -> StreamFusionStatelessRegion.stages(second))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cycle");
    }

    private static final class FutureNativeNode
            extends org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase<org.apache.flink.table.data.RowData>
            implements org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecNode<
                            org.apache.flink.table.data.RowData>,
                    StreamFusionNativePlanNode {
        private final StreamFusionNativeNodeMetadata nativeMetadata = new StreamFusionNativeNodeMetadata();

        @Override
        public StreamFusionNativeNodeMetadata nativeMetadata() {
            return nativeMetadata;
        }

        FutureNativeNode(RowType type) {
            super(
                    org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext.newNodeId(),
                    new org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext("future-native_1"),
                    new Configuration(),
                    List.of(InputProperty.DEFAULT),
                    type,
                    "future native node");
        }

        @Override
        public byte[] nativePlanFragment(org.apache.flink.table.planner.delegation.PlannerBase planner) {
            throw new AssertionError("Region discovery must not build runtime operators");
        }

        @Override
        protected org.apache.flink.api.dag.Transformation<org.apache.flink.table.data.RowData> translateToPlanInternal(
                org.apache.flink.table.planner.delegation.PlannerBase planner,
                org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig config) {
            throw new AssertionError("Region discovery must not translate internal nodes");
        }
    }

    @Test
    void collectsAllMixedStagesInInputOrderAndStopsAtTheBoundary() {
        var config = new Configuration();
        var type = RowType.of(new IntType(false));
        var ref = new RexInputRef(
                0,
                new FlinkTypeFactory(getClass().getClassLoader(), RelDataTypeSystem.DEFAULT)
                        .createSqlType(SqlTypeName.INTEGER));
        var source = new BatchExecTableSourceScan(config, null, type, "source");
        source.setInputEdges(List.of());
        var first = unary(
                new StreamFusionBatchExecCalc(config, List.of(ref), null, InputProperty.DEFAULT, type, "calc"), source);
        var expand = unary(
                new StreamFusionBatchExecExpand(config, List.of(List.of(ref)), InputProperty.DEFAULT, type, "expand"),
                first);
        var second = unary(
                new StreamFusionBatchExecCalc(config, List.of(ref), null, InputProperty.DEFAULT, type, "calc"), expand);
        var last = unary(
                new StreamFusionBatchExecExpand(config, List.of(List.of(ref)), InputProperty.DEFAULT, type, "expand"),
                second);
        assertThat(StreamFusionStatelessRegion.stages(last)).containsExactly(first, expand, second, last);
        assertThat(StreamFusionStatelessRegion.stages(second)).containsExactly(first, expand, second);
        assertThat(StreamFusionStatelessRegion.stages(first)).containsExactly(first);
        assertThat(first.getInputEdges().get(0).getSource()).isSameAs(source);
    }

    private static <T extends ExecNode<?>> T unary(T node, ExecNode<?> input) {
        node.setInputEdges(List.of(ExecEdge.builder().source(input).target(node).build()));
        return node;
    }
}
