/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;

/** Keeps a failed shape or capability inspection from hiding independent upstream blockers. */
final class StreamFusionRejectionTraversal {
    private final Set<ExecNode<?>> visited = Collections.newSetFromMap(new IdentityHashMap<>());
    private final List<String> rejections;
    private final BiConsumer<ExecNode<?>, String> inspect;

    StreamFusionRejectionTraversal(List<String> rejections, BiConsumer<ExecNode<?>, String> inspect) {
        this.rejections = rejections;
        this.inspect = inspect;
    }

    private Inspection current;

    private static final class Inspection {
        boolean followedInput;
    }

    void visit(ExecNode<?> node, String path) {
        Inspection parent = current;
        if (parent != null) parent.followedInput = true;
        if (!visited.add(node)) return;
        int before = rejections.size();
        Inspection inspection = new Inspection();
        current = inspection;
        boolean failed = false;
        try {
            inspect.accept(node, path);
        } catch (RuntimeException | LinkageError failure) {
            failed = true;
            rejections.add(path + "/" + node.getClass().getSimpleName()
                    + "\nStreamFusion capability inspection was inconclusive: "
                    + failure.getClass().getSimpleName() + ": " + failure.getMessage());
        } finally {
            current = parent;
        }
        if (!failed && (rejections.size() == before || inspection.followedInput)) return;
        // Successful fused-shape inspection owns its consumed nodes. A rejected shape cannot
        // suppress checks of its original inputs, including when it returned before recursion.
        for (int index = 0; index < node.getInputEdges().size(); index++) {
            visit(
                    node.getInputEdges().get(index).getSource(),
                    path + "/" + node.getClass().getSimpleName() + "/input[" + index + "]");
        }
    }
}
