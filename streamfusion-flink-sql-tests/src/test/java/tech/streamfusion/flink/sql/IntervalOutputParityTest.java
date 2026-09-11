/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.sql;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

class IntervalOutputParityTest extends SqlParityTestSupport {
    @Test
    void literalOutputsUseFlinksPhysicalMonthsAndMilliseconds() throws Exception {
        for (boolean streaming : List.of(false, true)) {
            assertUnorderedInsertParity("SELECT INTERVAL '2' WEEK, INTERVAL '3' QUARTER", streaming);
            for (String months : List.of("-25", "0", "9", "120")) {
                assertUnorderedInsertParity(
                        "SELECT id + 1, INTERVAL '" + months + "' MONTH(3), "
                                + "INTERVAL '-2 01:02:03.004' DAY TO SECOND, CAST(NULL AS INTERVAL DAY TO SECOND), "
                                + "CAST(NULL AS INTERVAL YEAR TO MONTH) FROM (VALUES (1), (2), (3)) AS input(id)",
                        streaming);
            }
        }
    }

    @Test
    void unsupportedComputedIntervalOutputsFallBackBeforeTheArrowBoundary() throws Exception {
        for (String expression : List.of(
                "CASE WHEN metric > 0 THEN INTERVAL '1' DAY ELSE INTERVAL '-2' DAY END",
                "ARRAY[CASE WHEN metric > 0 THEN INTERVAL '1' DAY ELSE INTERVAL '-2' DAY END]")) {
            assertFallbackIntegerDataStreamParity("SELECT " + expression + " FROM integer_input");
            assertThat(StreamFusionPlanningDiagnostics.explain())
                    .contains("computed interval output", "months/milliseconds");
        }
    }
}
