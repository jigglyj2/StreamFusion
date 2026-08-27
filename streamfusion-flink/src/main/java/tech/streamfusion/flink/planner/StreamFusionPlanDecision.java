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

/** Immutable all-or-nothing acceleration decision and its EXPLAIN diagnostics. */
public final class StreamFusionPlanDecision {
    public static final class Rejection {
        private final String flinkOperator;
        private final StreamFusionPlanNode.Role role;
        private final String reason;

        Rejection(String flinkOperator, StreamFusionPlanNode.Role role, String reason) {
            this.flinkOperator = flinkOperator;
            this.role = role;
            this.reason = reason;
        }

        public String flinkOperator() {
            return flinkOperator;
        }

        public String reason() {
            return reason;
        }
    }

    private final List<Rejection> rejections;

    StreamFusionPlanDecision(List<Rejection> rejections) {
        this.rejections = List.copyOf(rejections);
    }

    public boolean acceleratesWholePlan() {
        return rejections.isEmpty();
    }

    public List<Rejection> rejections() {
        return rejections;
    }

    public String explain() {
        StringBuilder result = new StringBuilder("== StreamFusion Acceleration ==\n");
        if (acceleratesWholePlan()) {
            return result.append("Accelerated: yes\n")
                    .append(
                            "Plan reason: every internal node has a StreamFusion operator; source and sink boundaries are covered.")
                    .toString();
        }
        result.append("Accelerated: no\n")
                .append("Plan reason: all-or-nothing coverage failed; the entire plan will use Flink.\n")
                .append("Operator rejections:\n");
        for (Rejection rejection : rejections) {
            result.append("- ")
                    .append(rejection.flinkOperator)
                    .append(" [")
                    .append(rejection.role)
                    .append("]: ")
                    .append(rejection.reason)
                    .append('\n');
        }
        return result.toString().stripTrailing();
    }
}
