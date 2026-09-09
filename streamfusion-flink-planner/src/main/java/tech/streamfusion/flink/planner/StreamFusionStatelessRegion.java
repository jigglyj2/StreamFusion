/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.types.logical.RowType;

/** Discovers native physical trees without translating internal nodes to JVM operators. */
final class StreamFusionStatelessRegion {
    private StreamFusionStatelessRegion() {}

    static List<ExecNode<?>> stages(ExecNode<?> root) {
        List<ExecNode<?>> result = new ArrayList<>();
        boolean bounded = root instanceof org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecNode;
        collect(root, bounded, new java.util.IdentityHashMap<>(), result);
        return result;
    }

    private static void collect(
            ExecNode<?> node, boolean bounded, java.util.Map<ExecNode<?>, Boolean> visited, List<ExecNode<?>> result) {
        if (!(node instanceof StreamFusionNativePlanNode)
                || bounded != (node instanceof org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecNode)) {
            return;
        }
        Boolean complete = visited.putIfAbsent(node, false);
        if (complete != null) {
            throw new IllegalArgumentException(
                    complete
                            ? "A native tree cannot duplicate a shared internal physical stage"
                            : "A native region cannot contain a physical-plan cycle");
        }
        if (node.getInputEdges().isEmpty()) {
            throw new IllegalArgumentException("An arrival-driven native stage must have physical inputs");
        }
        for (ExecEdge edge : node.getInputEdges()) {
            collect(edge.getSource(), bounded, visited, result);
        }
        visited.put(node, true);
        result.add(node);
    }

    static Transformation<RowData> translate(ExecNode<?> root, PlannerBase planner) {
        var owner = ((StreamFusionNativePlanNode) root).nativeMetadata().sharedRegion();
        var result = owner == null ? translateInternal(root, planner) : owner.translate(root, planner);
        ((StreamFusionNativePlanNode) root).nativeMetadata().recordOutput(result);
        return result;
    }

    @SuppressWarnings("unchecked")
    private static Transformation<RowData> translateInternal(ExecNode<?> root, PlannerBase planner) {
        List<ExecNode<?>> stages = stages(root);
        if (stages.isEmpty()) {
            throw new IllegalArgumentException("Native region root does not implement physical-plan lowering");
        }
        ExecEdge edge = stages.get(0).getInputEdges().get(0);
        ClassLoader loader = planner.getFlinkContext().getClassLoader();
        try {
            Class<?> runtime =
                    Class.forName("tech.streamfusion.flink.operator.StreamFusionNativeRegionTranslator", true, loader);
            var identify = runtime.getMethod("identifyStage", byte[].class, int.class, String.class, String.class);
            List<byte[]> plans = new ArrayList<>();
            List<Long> stateIds = new ArrayList<>();
            var lookupSources =
                    new java.util.LinkedHashMap<Long, tech.streamfusion.flink.arrow.CsvLookupSnapshotSource>();
            StreamFusionOriginalWindowResources resources = null;
            for (ExecNode<?> stage : stages) {
                var nativeStage = (StreamFusionNativePlanNode) stage;
                if (nativeStage.ownsWindowBuffer()) {
                    var owner = nativeStage.nativeMetadata().resources();
                    if (owner == null)
                        throw new IllegalStateException("Local-window stage has no original resource graph");
                    if (resources != null && resources != owner)
                        throw new IllegalStateException("A native region cannot mix original window resource graphs");
                    resources = owner;
                }
                int identity = nativeStage.nativeMetadata().physicalNodeId(stage);
                if (nativeStage.lookupSource() != null)
                    lookupSources.put((1L << 32) | Integer.toUnsignedLong(identity), nativeStage.lookupSource());
                plans.add((byte[]) identify.invoke(
                        null,
                        nativeStage.nativePlanFragment(planner),
                        identity,
                        nativeStage.nativeMetadata().metricName(stage, planner.getTableConfig()),
                        nativeStage.nativeMetadata().metricUid(planner.getTableConfig())));
                if (nativeStage.ownsNativeKeyedState()) {
                    stateIds.add((1L << 32) | Integer.toUnsignedLong(identity));
                }
            }
            if (resources != null
                    || !lookupSources.isEmpty()
                    || !stateIds.isEmpty()
                    || stages.stream().anyMatch(stage -> stage.getInputEdges().size() != 1)) {
                return translateTree(root, planner, runtime, stages, plans, stateIds, resources, lookupSources);
            }
            // Only the region's edge is translated. All internal stages are protobuf children.
            return (Transformation<RowData>) runtime.getMethod(
                            "translate", Transformation.class, RowType.class, RowType.class, List.class)
                    .invoke(null, edge.translateToPlan(planner), edge.getOutputType(), root.getOutputType(), plans);
        } catch (InvocationTargetException failure) {
            throw new IllegalStateException("Native region translation failed", failure.getCause());
        } catch (ReflectiveOperationException failure) {
            throw new IllegalStateException("Could not invoke the native region runtime", failure);
        }
    }

    @SuppressWarnings("unchecked")
    private static Transformation<RowData> translateTree(
            ExecNode<?> root,
            PlannerBase planner,
            Class<?> runtime,
            List<ExecNode<?>> stages,
            List<byte[]> fragments,
            List<Long> stateIds,
            StreamFusionOriginalWindowResources resources,
            java.util.Map<Long, tech.streamfusion.flink.arrow.CsvLookupSnapshotSource> lookupSources)
            throws ReflectiveOperationException {
        var inputPlan = runtime.getMethod("inputPlan", int.class);
        var compose = runtime.getMethod("composeWithInputs", byte[].class, List.class);
        java.util.Map<ExecNode<?>, byte[]> trees = new java.util.IdentityHashMap<>();
        List<ExecEdge> boundaries = new ArrayList<>();
        for (int index = 0; index < stages.size(); index++) {
            ExecNode<?> stage = stages.get(index);
            List<byte[]> inputs = new ArrayList<>();
            for (ExecEdge edge : stage.getInputEdges()) {
                byte[] child = trees.get(edge.getSource());
                if (child == null) {
                    child = (byte[]) inputPlan.invoke(null, boundaries.size());
                    boundaries.add(edge);
                }
                inputs.add(child);
            }
            trees.put(stage, (byte[]) compose.invoke(null, fragments.get(index), inputs));
        }
        List<Transformation<RowData>> inputs = new ArrayList<>();
        List<RowType> inputTypes = new ArrayList<>();
        for (ExecEdge edge : boundaries) {
            inputs.add((Transformation<RowData>) edge.translateToPlan(planner));
            inputTypes.add((RowType) edge.getOutputType());
        }
        if (!lookupSources.isEmpty()) {
            if (!stateIds.isEmpty())
                throw new IllegalArgumentException(
                        "Lookup task-open sources cannot yet share a region with keyed state initialization");
            return (Transformation<RowData>) runtime.getMethod(
                            "translateInputsWithLookupSources",
                            List.class,
                            List.class,
                            RowType.class,
                            byte[].class,
                            java.util.function.Function.class,
                            java.util.Map.class)
                    .invoke(
                            null,
                            inputs,
                            inputTypes,
                            root.getOutputType(),
                            trees.get(root),
                            resources == null ? null : resources.resolver(),
                            lookupSources);
        }
        if (resources != null) {
            if (!stateIds.isEmpty()) {
                return (Transformation<RowData>) runtime.getMethod(
                                "translateKeyedInputsWithResources",
                                List.class,
                                List.class,
                                RowType.class,
                                byte[].class,
                                List.class,
                                org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.class,
                                java.util.function.Function.class)
                        .invoke(
                                null,
                                inputs,
                                inputTypes,
                                root.getOutputType(),
                                trees.get(root),
                                stateIds,
                                planner.getExecEnv(),
                                resources.resolver());
            }
            return (Transformation<RowData>) runtime.getMethod(
                            "translateInputsWithResources",
                            List.class,
                            List.class,
                            RowType.class,
                            byte[].class,
                            java.util.function.Function.class)
                    .invoke(null, inputs, inputTypes, root.getOutputType(), trees.get(root), resources.resolver());
        }
        if (!stateIds.isEmpty()) {
            return (Transformation<RowData>) runtime.getMethod(
                            "translateKeyedInputs",
                            List.class,
                            List.class,
                            RowType.class,
                            byte[].class,
                            List.class,
                            org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.class)
                    .invoke(
                            null,
                            inputs,
                            inputTypes,
                            root.getOutputType(),
                            trees.get(root),
                            stateIds,
                            planner.getExecEnv());
        }
        return (Transformation<RowData>)
                runtime.getMethod("translateInputs", List.class, List.class, RowType.class, byte[].class)
                        .invoke(null, inputs, inputTypes, root.getOutputType(), trees.get(root));
    }
}
