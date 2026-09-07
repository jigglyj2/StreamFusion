/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.sql;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

class NullAwareSearchParityTest extends SqlParityTestSupport {
    @ParameterizedTest
    @ValueSource(
            strings = {
                "metric IS NULL OR metric <> 0",
                "metric IS NOT NULL AND metric <> 0",
                "(metric IN (1, 3)) IS NOT FALSE",
                "(metric IN (1, 3)) IS TRUE",
                "metric IS NULL OR metric BETWEEN -2 AND 2",
                "metric IS NOT NULL AND metric NOT BETWEEN -2 AND 2"
            })
    void nullAwareRangesMatchFlinkInFiltersAndProjections(String predicate) throws Exception {
        for (String sql : List.of(
                "SELECT metric, " + predicate + " FROM integer_input",
                "SELECT metric FROM integer_input WHERE " + predicate)) {
            assertIntegerDataStreamParity(sql);
            assertThat(StreamFusionPlannerFactory.nativePlanBatchCount())
                    .withFailMessage(StreamFusionPlanningDiagnostics.explain())
                    .isGreaterThan(0);
        }
    }
}
