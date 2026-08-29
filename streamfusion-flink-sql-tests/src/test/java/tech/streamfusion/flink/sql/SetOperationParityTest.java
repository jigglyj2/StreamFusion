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

import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.StreamFusionPlannerFactory;

class SetOperationParityTest extends SqlParityTestSupport {
    private static final String LEFT =
            "SELECT id + 10 AS metric FROM (VALUES (1), (2)) AS left_input(id) WHERE id >= 1";
    private static final String RIGHT =
            "SELECT id + 20 AS metric FROM (VALUES (2), (3)) AS right_input(id) WHERE id >= 2";

    @Test
    void streamingUnionAllWithNativeBranchesMatchesFlinkByteForByte() throws Exception {
        assertParity(LEFT + " UNION ALL " + RIGHT, true);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThanOrEqualTo(2);
        assertThat(StreamFusionPlannerFactory.nativeUnionBatchCount()).isGreaterThan(0);
    }

    @Test
    void threeWayUnionAllPreservesDuplicatesAndNullsByteForByte() throws Exception {
        String branch = "SELECT metric FROM union_input";
        assertDataStreamParity(
                branch + " UNION ALL " + branch + " UNION ALL " + branch,
                Types.INT,
                List.of(Row.of(1), Row.of(1), Row.of(2), Row.of((Object) null)),
                "union_input");

        assertThat(StreamFusionPlannerFactory.nativeUnionBatchCount()).isGreaterThan(0);
    }

    @Test
    void unionDistinctFallsBackBecauseDeduplicationIsNotAccelerated() throws Exception {
        assertParity(LEFT + " UNION " + RIGHT, true);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isZero();
    }

    @Test
    void explainReportsUnionDistinctDeduplicationFallback() {
        System.setProperty(
                StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, StreamFusionPlannerFactory.class.getName());
        StreamTableEnvironment tableEnvironment =
                StreamTableEnvironment.create(StreamExecutionEnvironment.getExecutionEnvironment());

        assertThat(tableEnvironment.explainSql(LEFT + " UNION " + RIGHT))
                .contains("== StreamFusion Acceleration ==")
                .contains("Accelerated: no")
                .contains("operator has no StreamFusion physical implementation")
                .contains("the entire plan will use Flink");
    }
}
