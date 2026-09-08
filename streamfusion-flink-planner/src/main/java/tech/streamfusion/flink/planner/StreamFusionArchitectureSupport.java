/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.flink.table.planner.plan.logical.SessionWindowSpec;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecWindowTableFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecMultiJoin;

/** Production admission requirements, separate from implemented semantic coverage. */
final class StreamFusionArchitectureSupport {
    private static final Set<String> PERSISTENT_STATE = Set.of(
            "StreamExecChangelogNormalize",
            "StreamExecDeduplicate",
            "StreamExecGroupAggregate",
            "StreamExecLocalGroupAggregate",
            "StreamExecGlobalGroupAggregate",
            "StreamExecGroupWindowAggregate",
            "StreamExecOverAggregate",
            "StreamExecLocalWindowAggregate",
            "StreamExecGlobalWindowAggregate",
            "StreamExecWindowAggregate",
            "StreamExecWindowDeduplicate",
            "StreamExecRank",
            "StreamExecWindowRank",
            "StreamExecWindowJoin",
            "StreamExecTemporalJoin",
            "StreamExecIntervalJoin",
            "StreamExecMultiJoin",
            "StreamExecJoin",
            "StreamExecMatch",
            "StreamExecTemporalSort",
            "StreamExecSort",
            "BatchExecHashAggregate",
            "BatchExecSortAggregate",
            "BatchExecHashWindowAggregate",
            "BatchExecSortWindowAggregate",
            "BatchExecOverAggregate",
            "BatchExecRank",
            "BatchExecSort",
            "BatchExecSortLimit",
            "BatchExecAdaptiveJoin",
            "BatchExecHashJoin",
            "BatchExecNestedLoopJoin",
            "BatchExecSortMergeJoin");
    private static final Set<String> STATELESS_NATIVE = Set.of(
            "StreamExecCalc",
            "BatchExecCalc",
            "StreamExecUnion",
            "BatchExecUnion",
            "StreamExecCorrelate",
            "BatchExecCorrelate",
            "StreamExecExpand",
            "BatchExecExpand",
            "StreamExecWindowTableFunction",
            "BatchExecWindowTableFunction");

    private StreamFusionArchitectureSupport() {}

    // Admission is a capability of each physical family, never an operator-pair fusion rule.
    // Composition coverage is distinct from the persistent-memory gate and semantic/config checks.
    private static final Set<String> REGION_READY = Set.of(
            "StreamExecCalc",
            "BatchExecCalc",
            "StreamExecUnion",
            "BatchExecUnion",
            "StreamExecDeduplicate",
            "StreamExecGroupAggregate",
            "StreamExecLocalGroupAggregate",
            "StreamExecGlobalGroupAggregate");

    static void collect(ExecNodeGraph graph, List<String> rejections) {
        Set<ExecNode<?>> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        Map<ExecNode<?>, List<String>> shared = StreamFusionNativeRegionOwnership.sharedInternalStages(
                graph, node -> isNative(node.getClass().getSimpleName()));
        for (int index = 0; index < graph.getRootNodes().size(); index++) {
            visit(graph.getRootNodes().get(index), "root[" + index + "]", visited, shared, rejections);
        }
    }

    private static void visit(
            ExecNode<?> node,
            String path,
            Set<ExecNode<?>> visited,
            Map<ExecNode<?>, List<String>> shared,
            List<String> rejections) {
        if (!visited.add(node)) {
            return;
        }
        String name = node.getClass().getSimpleName();
        String nodePath = path + "/" + name;
        if (node.getOutputType() instanceof org.apache.flink.table.types.logical.RowType) {
            for (String field : ((org.apache.flink.table.types.logical.RowType) node.getOutputType()).getFieldNames()) {
                if (field.startsWith("__streamfusion_owned_timestamp_"))
                    rejections.add(nodePath + "\nschema: SQL field '" + field
                            + "' conflicts with the reserved native owned-envelope namespace");
            }
        }
        if (shared.containsKey(node)) {
            rejections.add(nodePath + "\narchitecture: native stage has multiple consumers " + shared.get(node)
                    + "; multi-output native region ownership is not yet implemented; independent fusion would "
                    + "duplicate execution and per-stage metrics");
        }
        boolean persistent = PERSISTENT_STATE.contains(name);
        if (node instanceof CommonExecWindowTableFunction) {
            try {
                persistent |= FlinkExecNodeAccess.windowStrategy((CommonExecWindowTableFunction) node)
                                .getWindow()
                        instanceof SessionWindowSpec;
            } catch (RuntimeException failure) {
                rejections.add(nodePath + "\narchitecture: could not determine Window TVF state ownership: "
                        + failure.getMessage());
            }
        }
        if (persistent) {
            rejections.add(nodePath + "\narchitecture: native persistent state is temporarily disabled; "
                    + "large retained-state/buffer admission, Flink backend configuration parity, "
                    + "and checkpoint/metric conformance are not yet verified for this physical family");
        }
        // Flink also represents two-table joins as MultiJoin. Use the same shape
        // decision as semantic lowering, not the original node's class name.
        if (usesMultiWayJoin(node, nodePath, rejections)) {
            rejections.add(nodePath + "\narchitecture: multi-way join's paged state and bounded output cursor "
                    + "are not yet integrated with the common fused ExecutionPlan, per-stage metrics, "
                    + "and checkpoint/control lifecycle");
        }
        for (int index = 0; index < node.getInputEdges().size(); index++) {
            ExecEdge edge = node.getInputEdges().get(index);
            String child = edge.getSource().getClass().getSimpleName();
            if (isNative(name) && isNative(child) && !sameReadyRegion(node, edge.getSource())) {
                rejections.add(nodePath + "/input[" + index + "]\narchitecture: " + child + " -> " + name
                        + " is not admitted as a general fused native ExecutionPlan region with complete per-stage "
                        + "metric parity; intermediate JNI/Java handoff is not allowed");
            }
            visit(edge.getSource(), nodePath + "/input[" + index + "]", visited, shared, rejections);
        }
    }

    private static boolean isNative(String name) {
        return PERSISTENT_STATE.contains(name) || STATELESS_NATIVE.contains(name);
    }

    private static boolean usesMultiWayJoin(ExecNode<?> node, String path, List<String> rejections) {
        if (!(node instanceof StreamExecMultiJoin)) return false;
        try {
            return FlinkExecNodeAccess.binaryMultiJoinSpec((StreamExecMultiJoin) node) == null;
        } catch (RuntimeException failure) {
            rejections.add(
                    path + "\narchitecture: could not determine native join implementation: " + failure.getMessage());
            return true;
        }
    }

    private static boolean isRegionReady(ExecNode<?> node) {
        if (REGION_READY.contains(node.getClass().getSimpleName())) return true;
        if (!(node instanceof StreamExecMultiJoin)) return false;
        try {
            // Binary INNER MultiJoin is covered by the shared regular-join/Calc metric,
            // changelog, rescaling and channel-recovery tests. Outer and true multi-way
            // shapes use a different algorithm and retain their separate integration gate.
            return FlinkExecNodeAccess.binaryMultiJoinSpec((StreamExecMultiJoin) node) != null;
        } catch (RuntimeException unsupportedShape) {
            return false;
        }
    }

    private static boolean sameReadyRegion(ExecNode<?> parent, ExecNode<?> child) {
        return isRegionReady(parent)
                && isRegionReady(child)
                && (parent instanceof org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecNode)
                        == (child instanceof org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecNode);
    }
}
