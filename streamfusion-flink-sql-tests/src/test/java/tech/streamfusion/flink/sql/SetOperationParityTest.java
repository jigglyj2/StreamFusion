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

import java.sql.Timestamp;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

class SetOperationParityTest extends SqlParityTestSupport {
    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {true, false})
    void scalarAndNestedTypesUseTheSharedRegionInBothExecutionModes(boolean streaming) throws Exception {
        List<String> branches = List.of(
                SCALAR_TYPE_PROJECTION_SQL,
                "SELECT ARRAY[1, CAST(NULL AS INT)] AS a, MAP['k', CAST(1.25 AS DECIMAL(38, 5))] AS m, "
                        + "ROW(CAST(NULL AS STRING), TIMESTAMP '2026-08-27 12:34:56.123456') AS r");
        for (String branch : branches) {
            // Distinct sources keep type coverage separate from shared-internal-node ownership.
            String other = branch.replace("2026-08-27", "2026-08-28");
            assertParity("(" + branch + ") UNION ALL (" + other + ")", streaming);
            assertThat(StreamFusionPlanningDiagnostics.explain()).startsWith("Accelerated: yes");
            assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isPositive();
            assertThat(StreamFusionPlannerFactory.nativeUnionBatchCount()).isZero();
        }
    }

    private static final String LEFT =
            "SELECT id + 10 AS metric FROM (VALUES (1), (2)) AS left_input(id) WHERE id >= 1";
    private static final String RIGHT =
            "SELECT id + 20 AS metric FROM (VALUES (2), (3)) AS right_input(id) WHERE id >= 2";

    @Test
    void streamingUnionAllWithNativeBranchesMatchesFlinkByteForByte() throws Exception {
        assertParity(LEFT + " UNION ALL " + RIGHT, true);
        assertThat(StreamFusionPlanningDiagnostics.explain()).startsWith("Accelerated: yes");
        assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isGreaterThanOrEqualTo(2);
        assertThat(StreamFusionPlannerFactory.nativeUnionBatchCount()).isZero();
    }

    @Test
    void threeWayUnionAllPreservesDuplicatesAndNullsByteForByte() throws Exception {
        String branch = "SELECT metric FROM union_input";
        assertDataStreamParity(
                branch + " UNION ALL " + branch + " UNION ALL " + branch,
                Types.INT,
                List.of(Row.of(1), Row.of(1), Row.of(2), Row.of((Object) null)),
                "union_input");

        assertThat(StreamFusionPlanningDiagnostics.explain()).startsWith("Accelerated: yes");
        assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isPositive();
        assertThat(StreamFusionPlannerFactory.nativeUnionBatchCount()).isZero();
    }

    @Test
    void sourceOnlyNanosecondTimestampUnionFallsBackBeforeArrowConversion() throws Exception {
        String branch = "SELECT metric FROM timestamp_union_input";
        assertFallbackDataStreamParity(
                branch + " UNION ALL " + branch,
                Types.SQL_TIMESTAMP,
                List.of(Row.of(Timestamp.valueOf("9999-12-30 22:11:22.987654321"))),
                "timestamp_union_input");

        assertThat(StreamFusionPlannerFactory.nativeUnionBatchCount()).isZero();
        assertThat(StreamFusionPlanningDiagnostics.explain())
                .contains("StreamExecUnion")
                .contains("TIMESTAMP precision 9 stays on Flink");
    }

    @Test
    void unionDistinctFallsBackUntilStateAdmission() throws Exception {
        assertFallbackParity(LEFT + " UNION " + RIGHT, true);

        SqlFallbackAssertions.nativeBatchesAreZero(StreamFusionPlannerFactory.nativeUnionBatchCount());
        SqlFallbackAssertions.nativeBatchesAreZero(StreamFusionPlannerFactory.nativeGroupAggregateBatchCount());
    }

    @Test
    void explainReportsWholePlanFallbackForUnionDistinctState() {
        System.setProperty(
                StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, StreamFusionPlannerFactory.class.getName());
        StreamTableEnvironment tableEnvironment =
                StreamTableEnvironment.create(StreamExecutionEnvironment.getExecutionEnvironment());

        assertThat(tableEnvironment.explainSql(LEFT + " UNION " + RIGHT))
                .contains("== StreamFusion Acceleration ==")
                .contains("Accelerated: no")
                .contains("native persistent state is temporarily disabled")
                .doesNotContain("StreamFusionGroupAggregate", "StreamFusionUnionAll");
    }
}
