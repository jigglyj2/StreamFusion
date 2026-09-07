/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.sql;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Method;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

/** Explicit fallback coverage; these checks do not establish native operator parity. */
final class SqlFallbackAssertions {
    private SqlFallbackAssertions() {}

    static void admission() {
        assertTemporaryFallback(StreamFusionPlanningDiagnostics.explain());
    }

    static void nativeBatchesAreZero(long actual) {
        admission();
        assertThat(actual).isZero();
    }

    private static void assertTemporaryFallback(String explain) {
        unaccelerated();
        assertThat(explain).contains("Accelerated: no", "the entire plan will use Flink", ": architecture:");
        assertThat(explain.lines()
                        .filter(line -> line.startsWith("Fallback:"))
                        .collect(java.util.stream.Collectors.toList()))
                .isNotEmpty()
                .allSatisfy(reason -> assertThat(reason)
                        .matches(
                                line -> line.contains(": architecture:")
                                        || line.contains(
                                                "frame: bounded native OVER does not yet reproduce Flink's batch-sort tie permutation for ROWS frames")
                                        || line.contains("floating-point ordering; Flink's NaN/signed-zero comparator"),
                                "a documented architecture restriction, with no unrelated fallback"));
    }

    static void unaccelerated() {
        assertThat(StreamFusionPlanningDiagnostics.explain())
                .contains("Accelerated: no", "the entire plan will use Flink");
        // Fallback is all-or-nothing, including VALUES and otherwise eligible Calc stages.
        for (Method method : StreamFusionPlannerFactory.class.getMethods()) {
            if (method.getName().startsWith("native")
                    && method.getName().endsWith("BatchCount")
                    && method.getParameterCount() == 0) {
                try {
                    assertThat(((Number) method.invoke(null)).longValue())
                            .as(method.getName())
                            .isZero();
                } catch (ReflectiveOperationException failure) {
                    throw new AssertionError("Could not inspect native execution counter " + method.getName(), failure);
                }
            }
        }
    }
}
