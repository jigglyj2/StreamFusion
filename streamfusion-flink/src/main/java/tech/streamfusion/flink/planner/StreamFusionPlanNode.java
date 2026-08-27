/*
 * Copyright 2026 StreamFusion Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package tech.streamfusion.flink.planner;

import java.util.List;
import java.util.Objects;
import tech.streamfusion.flink.operator.StreamFusionPlanImplementation;

/** Planner-neutral representation used while deciding whether a whole plan can be replaced. */
public final class StreamFusionPlanNode {
    public enum Role {
        SOURCE,
        INTERNAL,
        SINK
    }

    private final String flinkOperator;
    private final Role role;
    private final StreamFusionPlanImplementation streamFusionImplementation;
    private final String rejectionReason;
    private final List<StreamFusionPlanNode> inputs;

    private StreamFusionPlanNode(
            String flinkOperator,
            Role role,
            StreamFusionPlanImplementation streamFusionImplementation,
            String rejectionReason,
            List<StreamFusionPlanNode> inputs) {
        this.flinkOperator = Objects.requireNonNull(flinkOperator, "flinkOperator");
        this.role = Objects.requireNonNull(role, "role");
        this.streamFusionImplementation = streamFusionImplementation;
        this.rejectionReason = rejectionReason;
        this.inputs = List.copyOf(inputs);
        if ((streamFusionImplementation == null) == (rejectionReason == null)) {
            throw new IllegalArgumentException("A node must have exactly one operator or rejection reason");
        }
    }

    public static StreamFusionPlanNode supported(
            String flinkOperator,
            Role role,
            StreamFusionPlanImplementation streamFusionImplementation,
            StreamFusionPlanNode... inputs) {
        return new StreamFusionPlanNode(flinkOperator, role, streamFusionImplementation, null, List.of(inputs));
    }

    public static StreamFusionPlanNode unsupported(
            String flinkOperator, Role role, String rejectionReason, StreamFusionPlanNode... inputs) {
        return new StreamFusionPlanNode(flinkOperator, role, null, rejectionReason, List.of(inputs));
    }

    String flinkOperator() {
        return flinkOperator;
    }

    Role role() {
        return role;
    }

    StreamFusionPlanImplementation streamFusionImplementation() {
        return streamFusionImplementation;
    }

    String rejectionReason() {
        return rejectionReason;
    }

    List<StreamFusionPlanNode> inputs() {
        return inputs;
    }
}
