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

/** Per-planning-thread acceleration outcome consumed by the EXPLAIN wrapper. */
public final class StreamFusionPlanningDiagnostics {
    private static final ThreadLocal<Report> CURRENT = new ThreadLocal<>();

    private StreamFusionPlanningDiagnostics() {}

    static void begin() {
        CURRENT.set(new Report());
    }

    static void reject(String path, String reason) {
        CURRENT.get().rejections.add(path + ": " + reason);
    }

    static void accelerate() {
        CURRENT.get().accelerated = true;
    }

    public static String explain() {
        Report report = CURRENT.get();
        if (report == null) {
            return "Accelerated: no\nPlan reason: StreamFusion physical planning diagnostics were not available.";
        }
        if (report.accelerated) {
            return "Accelerated: yes\nPlan reason: every internal node and expression has a StreamFusion implementation.";
        }
        StringBuilder explanation = new StringBuilder("Accelerated: no\nPlan reason: the entire plan will use Flink.");
        for (String rejection : report.rejections) {
            explanation.append("\nFallback: ").append(rejection);
        }
        return explanation.toString();
    }

    private static final class Report {
        private boolean accelerated;
        private final List<String> rejections = new ArrayList<>();
    }
}
