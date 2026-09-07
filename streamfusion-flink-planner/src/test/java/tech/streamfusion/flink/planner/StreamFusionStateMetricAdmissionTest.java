/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateLatencyTrackOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecDropUpdateBefore;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

class StreamFusionStateMetricAdmissionTest {
    @Test
    void allDeclaredStateOwnersUseOneGuardBeforeGraphCommitWithoutAnOperatorNameRegistry() {
        var table = new Configuration();
        table.set(StateLatencyTrackOptions.LATENCY_TRACK_ENABLED, true);
        var override = new Configuration();
        override.set(StateLatencyTrackOptions.LATENCY_TRACK_ENABLED, false);
        var first = original(new Configuration());
        var second = original(new Configuration());
        var exempt = original(override);
        var stateless = original(new Configuration());
        var root = original(new Configuration());
        var edge = ExecEdge.builder().source(first).target(root).build();
        root.setInputEdges(List.of(edge));
        var rewrite = new StreamFusionGraphRewrite(table);
        for (var node : List.of(first, second, exempt, stateless))
            rewrite.convert(node, ignored -> new UnregisteredNativeNode(node != stateless));
        rewrite.replaceInputEdge(
                root, 0, ExecEdge.builder().source(second).target(root).build());
        var checked = new AtomicInteger();
        var reasons = rewrite.stateMetricRejections(config -> {
            checked.incrementAndGet();
            return config.get(StateLatencyTrackOptions.LATENCY_TRACK_ENABLED)
                    ? "metrics: unsupported configured surface"
                    : null;
        });
        assertThat(checked).hasValue(3);
        assertThat(reasons)
                .containsExactly(
                        "StreamExecDropUpdateBefore#" + first.getId() + "\nmetrics: unsupported configured surface",
                        "StreamExecDropUpdateBefore#" + second.getId() + "\nmetrics: unsupported configured surface");
        assertThat(root.getInputEdges()).containsExactly(edge);
        assertThat(rewrite.stateMetricRejections(config -> null)).isEmpty();
        rewrite.commit();
        assertThat(root.getInputEdges().get(0).getSource()).isSameAs(second);
    }

    private static StreamExecDropUpdateBefore original(Configuration config) {
        // Use the restore constructor so the explicit persisted override is not filtered by
        // this synthetic original family's consumed-option list.
        var node = new StreamExecDropUpdateBefore(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(StreamExecDropUpdateBefore.class),
                config,
                List.of(InputProperty.DEFAULT),
                RowType.of(new IntType()),
                "original");
        node.setInputEdges(List.of());
        return node;
    }

    private static final class UnregisteredNativeNode extends ExecNodeBase<RowData>
            implements StreamFusionNativePlanNode {
        private final StreamFusionNativeNodeMetadata metadata = new StreamFusionNativeNodeMetadata();
        private final boolean keyed;

        UnregisteredNativeNode(boolean keyed) {
            super(
                    ExecNodeContext.newNodeId(),
                    new ExecNodeContext("conformance_1"),
                    new Configuration(),
                    List.of(InputProperty.DEFAULT),
                    RowType.of(new IntType()),
                    "unregistered native family");
            this.keyed = keyed;
            setInputEdges(List.of());
        }

        @Override
        public StreamFusionNativeNodeMetadata nativeMetadata() {
            return metadata;
        }

        @Override
        public boolean ownsNativeKeyedState() {
            return keyed;
        }

        @Override
        public byte[] nativePlanFragment(PlannerBase planner) {
            throw new AssertionError("must not lower or execute for metric admission");
        }

        @Override
        protected Transformation<RowData> translateToPlanInternal(PlannerBase planner, ExecNodeConfig config) {
            throw new AssertionError("must not execute for metric admission");
        }
    }
}
