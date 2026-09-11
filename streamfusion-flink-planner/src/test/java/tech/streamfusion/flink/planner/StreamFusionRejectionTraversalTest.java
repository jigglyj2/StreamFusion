/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecUnion;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

class StreamFusionRejectionTraversalTest {
    private final RowType type = RowType.of(new IntType());

    @Test
    void failedShapeStillReportsBothInputsAndOnlyInspectsSharedStagesOnce() {
        var shared = source("shared blocker");
        var left = union(shared, source("left blocker"));
        var right = union(shared, source("right blocker"));
        var root = union(left, right);
        var reasons = new ArrayList<String>();
        var inspected = new ArrayList<ExecNode<?>>();
        var traversal = new StreamFusionRejectionTraversal(reasons, (node, path) -> {
            inspected.add(node);
            // An invalid shape returns without descending to its inputs.
            reasons.add(path + "\nunsupported " + node.getDescription());
        });
        traversal.visit(root, "root[0]");
        traversal.visit(shared, "root[1]");
        assertThat(inspected).hasSize(6).containsOnlyOnce(shared);
        assertThat(String.join("\n", reasons)).contains("left blocker", "right blocker", "shared blocker");
    }

    @Test
    void capabilityFailureDoesNotHideDescendantsOrTheOtherRoot() {
        var child = source("unsupported child");
        var root = union(child, source("sibling"));
        var other = source("other root");
        var reasons = new ArrayList<String>();
        var traversal = new StreamFusionRejectionTraversal(reasons, (node, path) -> {
            if (node == root) throw new NoClassDefFoundError("missing capability");
            reasons.add(path + "\nunsupported " + node.getDescription());
        });
        traversal.visit(root, "root[0]");
        traversal.visit(other, "root[1]");
        assertThat(String.join("\n", reasons))
                .contains("missing capability", "unsupported child", "sibling", "other root", "root[1]");
    }

    @Test
    void acceptedFusedShapeDoesNotReinspectItsConsumedNodesWhenAnUpstreamInputFails() {
        var input = source("unsupported input");
        var folded = union(input, input);
        var root = union(folded, folded);
        var reasons = new ArrayList<String>();
        var inspected = new ArrayList<ExecNode<?>>();
        var holder = new StreamFusionRejectionTraversal[1];
        holder[0] = new StreamFusionRejectionTraversal(reasons, (node, path) -> {
            inspected.add(node);
            if (node == root) holder[0].visit(input, path + "/native-input");
            else reasons.add(node.getDescription());
        });
        holder[0].visit(root, "root[0]");
        assertThat(inspected).containsExactly(root, input);
        assertThat(reasons).containsExactly("unsupported input");
    }

    private BatchExecTableSourceScan source(String name) {
        var node = new BatchExecTableSourceScan(new Configuration(), null, type, name);
        node.setInputEdges(List.of());
        return node;
    }

    private BatchExecUnion union(ExecNode<?> left, ExecNode<?> right) {
        var node = new BatchExecUnion(
                new Configuration(), List.of(InputProperty.DEFAULT, InputProperty.DEFAULT), type, "union");
        node.setInputEdges(List.of(
                ExecEdge.builder().source(left).target(node).build(),
                ExecEdge.builder().source(right).target(node).build()));
        return node;
    }
}
