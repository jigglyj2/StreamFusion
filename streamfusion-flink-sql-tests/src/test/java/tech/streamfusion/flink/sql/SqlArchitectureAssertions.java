/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.sql;

import static org.assertj.core.api.Assertions.assertThat;

import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

/** Native parity requires actual acceleration. Fallback must use explicit fallback assertions. */
final class SqlArchitectureAssertions {
    private SqlArchitectureAssertions() {}

    static void admission() {
        requireAcceleration(StreamFusionPlanningDiagnostics.explain());
    }

    static void requireAcceleration(String explain) {
        assertThat(explain)
                .withFailMessage("Native execution required: %s", explain)
                .startsWith("Accelerated: yes");
    }

    static void nativeBatchesAtLeast(long actual, long minimum) {
        admission();
        assertThat(actual).isGreaterThanOrEqualTo(minimum);
    }

    static void nativeBatchesExactly(long actual, long expected) {
        admission();
        assertThat(actual).isEqualTo(expected);
    }
}
