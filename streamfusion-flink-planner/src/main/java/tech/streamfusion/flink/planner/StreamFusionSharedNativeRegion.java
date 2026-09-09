/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph;
import org.apache.flink.table.types.logical.RowType;

/** Planner-only identity cache: translating either exit materializes the same native owner once. */
final class StreamFusionSharedNativeRegion {
    private final StreamFusionNativeRegionLayout.Region layout;
    private final byte[] plan;
    private final java.lang.reflect.Method translation;
    private final List<Long> stateIds;
    private final java.util.Map<Long, tech.streamfusion.flink.arrow.CsvLookupSnapshotSource> lookupSources =
            new java.util.LinkedHashMap<>();
    private final StreamFusionOriginalWindowResources resources;
    private List<Transformation<RowData>> outputs;
    private boolean translating;

    static void install(ExecNodeGraph graph, PlannerBase planner) {
        var layout = StreamFusionNativeRegionLayout.discover(graph, node -> node instanceof StreamFusionNativePlanNode);
        layout.validateBoundaryGraph();
        // Validate every connected region, including a single-exit tree, while GraphRewrite
        // can still roll back retained Flink boundary edges. Keyed contexts initialize before
        // open; the CSV lookup snapshot must observe the source at Flink's task-open boundary.
        for (var region : layout.regions) {
            boolean keyed =
                    region.stages.stream().anyMatch(node -> ((StreamFusionNativePlanNode) node).ownsNativeKeyedState());
            if (keyed)
                for (var node : region.stages) {
                    if (((StreamFusionNativePlanNode) node).lookupSource() != null)
                        throw new IllegalArgumentException(
                                node.getDescription()
                                        + ": lookup task-open sources cannot yet share a region with keyed state initialization");
                }
        }
        var owners = new ArrayList<StreamFusionSharedNativeRegion>();
        for (var region : layout.regions)
            if (region.outputs.size() > 1) owners.add(new StreamFusionSharedNativeRegion(region, planner));
        // Preflight every owner before binding any selected stage. GraphRewrite rolls back
        // retained Flink boundary edges if a shared contract cannot be represented exactly.
        for (var owner : owners)
            for (var node : owner.layout.stages)
                ((StreamFusionNativePlanNode) node).nativeMetadata().bindSharedRegion(owner);
    }

    private StreamFusionSharedNativeRegion(StreamFusionNativeRegionLayout.Region layout, PlannerBase planner) {
        if (planner == null)
            throw new IllegalArgumentException("Shared native region selection requires planner preflight");
        this.layout = layout;
        plan = layout.plan(planner);
        var ids = new ArrayList<Long>();
        StreamFusionOriginalWindowResources resourceOwner = null;
        for (var node : layout.stages) {
            var stage = (StreamFusionNativePlanNode) node;
            var config = Configuration.fromMap(
                    planner.getExecEnv().getConfiguration().toMap());
            config.addAll(Configuration.fromMap(planner.getTableConfig().toMap()));
            config.addAll(Configuration.fromMap(
                    ((ExecNodeBase<?>) node).getPersistedConfig().toMap()));
            var execution = planner.getExecEnv().getConfig();
            long latency = execution.isLatencyTrackingConfigured()
                    ? execution.getLatencyTrackingInterval()
                    : config.get(MetricOptions.LATENCY_INTERVAL).toMillis();
            if (latency > 0)
                throw new IllegalArgumentException(
                        "Shared native regions do not yet support Flink sampled latency routing");
            if (stage.lookupSource() != null)
                lookupSources.put(
                        (1L << 32)
                                | Integer.toUnsignedLong(stage.nativeMetadata().physicalNodeId(node)),
                        stage.lookupSource());
            if (stage.ownsNativeKeyedState())
                ids.add((1L << 32)
                        | Integer.toUnsignedLong(stage.nativeMetadata().physicalNodeId(node)));
            if (stage.ownsWindowBuffer()) {
                var original = stage.nativeMetadata().resources();
                if (original == null || (resourceOwner != null && resourceOwner != original))
                    throw new IllegalStateException(
                            "Shared local-window stages require one original Flink resource graph");
                resourceOwner = original;
            }
        }
        stateIds = List.copyOf(ids);
        if (!stateIds.isEmpty() && !lookupSources.isEmpty())
            throw new IllegalArgumentException(
                    "Lookup task-open sources cannot yet share a region with keyed state initialization");
        resources = resourceOwner;
        try {
            var runtime = Class.forName(
                    "tech.streamfusion.flink.operator.NativeSharedRegionTranslation",
                    true,
                    planner.getFlinkContext().getClassLoader());
            runtime.getMethod("validate", byte[].class, int.class).invoke(null, plan, layout.outputs.size());
            translation = runtime.getMethod(
                    "translateWithLookupSources",
                    List.class,
                    List.class,
                    List.class,
                    byte[].class,
                    List.class,
                    org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.class,
                    java.util.function.Function.class,
                    java.util.Map.class);
        } catch (InvocationTargetException failure) {
            throw new IllegalArgumentException(
                    "Unsupported shared native region: " + failure.getCause().getMessage(), failure.getCause());
        } catch (ReflectiveOperationException failure) {
            throw new IllegalStateException("Could not load shared native region runtime", failure);
        }
    }

    @SuppressWarnings("unchecked")
    Transformation<RowData> translate(ExecNode<?> exit, PlannerBase planner) {
        int port = layout.outputs.indexOf(exit);
        if (port < 0)
            throw new IllegalStateException("An internal shared stage cannot create an independent Flink operator");
        if (outputs != null) return outputs.get(port);
        if (translating) throw new IllegalStateException("A shared native owner cannot consume its own output");
        translating = true;
        try {
            var inputs = new ArrayList<Transformation<RowData>>();
            var inputTypes = new ArrayList<RowType>();
            for (var input : layout.inputs) {
                inputs.add((Transformation<RowData>) input.translateToPlan(planner));
                inputTypes.add((RowType) input.getOutputType());
            }
            var outputTypes = layout.outputs.stream()
                    .map(node -> (RowType) node.getOutputType())
                    .collect(java.util.stream.Collectors.toList());
            var result = (List<Transformation<RowData>>) translation.invoke(
                    null,
                    inputs,
                    inputTypes,
                    outputTypes,
                    plan,
                    stateIds,
                    planner.getExecEnv(),
                    resources == null ? null : resources.resolver(),
                    lookupSources);
            if (result.size() != layout.outputs.size())
                throw new IllegalStateException("Native region runtime returned an incorrect output arity");
            for (int index = 0; index < result.size(); index++)
                ((StreamFusionNativePlanNode) layout.outputs.get(index))
                        .nativeMetadata()
                        .recordOutput(result.get(index));
            outputs = List.copyOf(result);
            return outputs.get(port);
        } catch (InvocationTargetException failure) {
            throw new IllegalStateException("Shared native region translation failed", failure.getCause());
        } catch (ReflectiveOperationException failure) {
            throw new IllegalStateException("Could not invoke shared native region translation", failure);
        } finally {
            translating = false;
        }
    }
}
