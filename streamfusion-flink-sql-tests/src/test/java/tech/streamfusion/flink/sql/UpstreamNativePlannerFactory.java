/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.sql;

import java.util.List;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.table.delegation.Planner;
import tech.streamfusion.flink.StreamFusionPlannerFactory;

/**
 * Flink's testing table environment casts its planner to PlannerBase. Install the production
 * graph processor through the usual hook, retaining that concrete type for the upstream harness.
 * Production SQL parity tests separately exercise StreamFusion's decorating Planner facade.
 */
public final class UpstreamNativePlannerFactory {
    private UpstreamNativePlannerFactory() {}

    public static Planner decorate(Planner planner) {
        StreamFusionPlannerFactory.decorate(planner);
        return planner;
    }

    public static void finalizePipeline(StreamGraph graph, List<Transformation<?>> roots) {
        StreamFusionPlannerFactory.finalizePipeline(graph, roots);
    }
}
