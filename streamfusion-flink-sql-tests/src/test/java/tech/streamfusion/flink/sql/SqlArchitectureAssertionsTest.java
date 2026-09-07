/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.sql;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class SqlArchitectureAssertionsTest {
    @Test
    void nativeParityRejectsArchitectureFallback() {
        assertThatThrownBy(
                        () -> SqlArchitectureAssertions.requireAcceleration(
                                "Accelerated: no\nFallback: root: architecture: native state disabled; the entire plan will use Flink"))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("Native execution required");
        SqlArchitectureAssertions.requireAcceleration("Accelerated: yes");
    }
}
