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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
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
                true,
                false);

        assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isZero();
        assertThat(StreamFusionPlanningDiagnostics.explain())
                .contains("timestamp EXTRACT field YEAR")
                .contains("only timezone-free TIMESTAMP(3)")
                .contains("Accelerated: no");
    }

    @ParameterizedTest
    @ValueSource(strings = {"TIMESTAMP(0)", "TIMESTAMP(6)", "TIMESTAMP(9)", "TIMESTAMP_LTZ(3)"})
    void timestampClockFieldsRejectUnprovenPrecisionAndZone(String type) throws Exception {
        assertParity(
                "SELECT EXTRACT(HOUR FROM ts) FROM (VALUES "
                        + "(CAST(TIMESTAMP '1969-12-31 23:59:59.123456789' AS " + type + ")), "
                        + "(CAST(NULL AS " + type + "))) input(ts)",
                true,
                false);
        assertThat(StreamFusionPlanningDiagnostics.explain())
                .contains("timestamp EXTRACT field HOUR")
                .contains("only timezone-free TIMESTAMP(3)")
                .contains("Accelerated: no");
    }

    @Test
    void centuryExtractionFallsBackWithCalendarConventionReason() throws Exception {
        assertParity(
                "SELECT EXTRACT(CENTURY FROM date_value) FROM "
                        + "(VALUES (DATE '0001-01-01'), (DATE '2000-02-29'), "
                        + "(DATE '9999-12-31'), (CAST(NULL AS DATE))) input(date_value)",
                true,
                false);

        assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isZero();
        assertThat(StreamFusionPlanningDiagnostics.explain())
                .contains("DATE EXTRACT field CENTURY stays on Flink")
                .contains("BCE and year-zero calendar conventions")
                .contains("Accelerated: no");
    }
}
