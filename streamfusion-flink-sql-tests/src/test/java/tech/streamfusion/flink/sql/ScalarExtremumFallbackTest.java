/*
 * Copyright 2026 StreamFusion Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package tech.streamfusion.flink.sql;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

class ScalarExtremumFallbackTest extends SqlParityTestSupport {
    @Test
    void floatingExtremumFallsBackWithSemanticReason() throws Exception {
        assertParity(
                "SELECT GREATEST(a, b), LEAST(a, b) FROM "
                        + "(VALUES (CAST(1.0 AS DOUBLE), CAST(2.0 AS DOUBLE)), "
                        + "(CAST(NULL AS DOUBLE), CAST(3.0 AS DOUBLE))) input(a, b)",
                true);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isZero();
        assertThat(StreamFusionPlanningDiagnostics.explain())
                .contains("currently accelerates signed integer, DATE, TIME, and timezone-free TIMESTAMP common types")
                .contains("floating NaN/signed-zero")
                .contains("Accelerated: no");
    }
}
