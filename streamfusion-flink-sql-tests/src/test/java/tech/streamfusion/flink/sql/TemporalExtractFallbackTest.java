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

class TemporalExtractFallbackTest extends SqlParityTestSupport {
    @Test
    void timestampExtractionFallsBackWithTimezoneAndPrecisionReason() throws Exception {
        assertParity(
                "SELECT EXTRACT(YEAR FROM timestamp_value) FROM "
                        + "(VALUES (TIMESTAMP '1969-12-31 23:59:59.123'), "
                        + "(TIMESTAMP '2024-02-29 12:34:56.987'), "
                        + "(CAST(NULL AS TIMESTAMP(3)))) input(timestamp_value)",
                true);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isZero();
        assertThat(StreamFusionPlanningDiagnostics.explain())
                .contains("timestamp EXTRACT stays on Flink")
                .contains("session-zone and subsecond precision semantics")
                .contains("Accelerated: no");
    }

    @Test
    void centuryExtractionFallsBackWithCalendarConventionReason() throws Exception {
        assertParity(
                "SELECT EXTRACT(CENTURY FROM date_value) FROM "
                        + "(VALUES (DATE '0001-01-01'), (DATE '2000-02-29'), "
                        + "(DATE '9999-12-31'), (CAST(NULL AS DATE))) input(date_value)",
                true);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isZero();
        assertThat(StreamFusionPlanningDiagnostics.explain())
                .contains("DATE EXTRACT field CENTURY stays on Flink")
                .contains("BCE and year-zero calendar conventions")
                .contains("Accelerated: no");
    }
}
