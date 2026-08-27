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

import java.util.ArrayList;
import java.util.List;

/** Enforces whole-plan StreamFusion operator coverage. */
public final class StreamFusionPlanAnalyzer {
    public StreamFusionPlanDecision analyze(StreamFusionPlanNode root) {
        List<StreamFusionPlanDecision.Rejection> rejections = new ArrayList<>();
        collectRejections(root, rejections);
        return new StreamFusionPlanDecision(rejections);
    }

    private static void collectRejections(
            StreamFusionPlanNode node, List<StreamFusionPlanDecision.Rejection> rejections) {
        if (node.streamFusionOperator() == null) {
            rejections.add(
                    new StreamFusionPlanDecision.Rejection(node.flinkOperator(), node.role(), node.rejectionReason()));
        }
        for (StreamFusionPlanNode input : node.inputs()) {
            collectRejections(input, rejections);
        }
    }
}
