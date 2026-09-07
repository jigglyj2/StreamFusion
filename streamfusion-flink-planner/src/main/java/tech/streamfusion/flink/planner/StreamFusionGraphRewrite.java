/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;

/** Stages changes to retained Flink nodes until the complete replacement graph is available. */
final class StreamFusionGraphRewrite {
    private final Map<ExecNode<?>, ExecNode<?>> replacements = new IdentityHashMap<>();
    private final Map<ExecNode<?>, List<ExecEdge>> pending = new IdentityHashMap<>();
    private final Map<String, ExecNode<?>> explicitUids = new java.util.HashMap<>();
    private final ReadableConfig tableConfig;

    StreamFusionGraphRewrite() {
        this(new Configuration());
    }

    StreamFusionGraphRewrite(ReadableConfig tableConfig) {
        this.tableConfig = tableConfig == null ? new Configuration() : tableConfig;
    }

    ExecNode<?> convert(ExecNode<?> node, Function<ExecNode<?>, ExecNode<?>> converter) {
        ExecNode<?> replacement = replacements.get(node);
        if (replacement == null) {
            replacement = converter.apply(node);
            if (replacement instanceof StreamFusionNativePlanNode && replacement != node) {
                var metadata = ((StreamFusionNativePlanNode) replacement).nativeMetadata();
                metadata.bindOriginal(node);
                String uid = metadata.metricUid(tableConfig);
                if (uid != null) {
                    ExecNode<?> previous = explicitUids.putIfAbsent(uid, node);
                    if (previous != null)
                        throw new IllegalStateException("Duplicate explicit Flink UID among selected native stages: "
                                + uid
                                + "; node " + previous.getId() + " (" + previous.getDescription() + ") and node "
                                + node.getId() + " (" + node.getDescription() + ")");
                }
            }
            replacements.put(node, replacement);
        }
        return replacement;
    }

    void replaceInputEdge(ExecNode<?> node, int index, ExecEdge edge) {
        pending.computeIfAbsent(node, ignored -> new ArrayList<>(node.getInputEdges()))
                .set(index, edge);
    }

    /** Validate shared resource capabilities before committing any retained Flink edge. */
    List<String> stateMetricRejections(Function<ReadableConfig, String> checker) {
        var rejections = new ArrayList<String>();
        replacements.entrySet().stream()
                .sorted(java.util.Comparator.comparingInt(
                        entry -> entry.getKey().getId()))
                .forEach(entry -> {
                    if (!(entry.getValue() instanceof StreamFusionNativePlanNode)
                            || !((StreamFusionNativePlanNode) entry.getValue()).ownsNativeKeyedState()) return;
                    var config = Configuration.fromMap(tableConfig.toMap());
                    config.addAll(Configuration.fromMap(((ExecNodeBase<?>) entry.getKey())
                            .getPersistedConfig()
                            .toMap()));
                    String reason = checker.apply(config);
                    if (reason != null)
                        rejections.add(entry.getKey().getClass().getSimpleName() + "#"
                                + entry.getKey().getId() + "\n" + reason);
                });
        return rejections;
    }

    void commit() {
        Map<ExecNode<?>, List<ExecEdge>> originals = new IdentityHashMap<>();
        try {
            for (Map.Entry<ExecNode<?>, List<ExecEdge>> entry : pending.entrySet()) {
                ExecNode<?> node = entry.getKey();
                originals.put(node, new ArrayList<>(node.getInputEdges()));
                node.setInputEdges(entry.getValue());
            }
        } catch (RuntimeException | LinkageError failure) {
            for (Map.Entry<ExecNode<?>, List<ExecEdge>> entry : originals.entrySet()) {
                entry.getKey().setInputEdges(entry.getValue());
            }
            throw failure;
        }
    }
}
