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

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.StreamFusionPlannerFactory;

class GroupingSetsParityTest extends SqlParityTestSupport {
    private static final String SQL = "SELECT category, SUM(amount) FROM "
            + "(VALUES ('a', 1), ('b', 2), ('a', 3)) AS input(category, amount) "
            + "GROUP BY GROUPING SETS ((category), ())";

    @Test
    void groupingSetsFallBackAsAWholeAndMatchFlinkByteForByte() throws Exception {
        assertParity(SQL, true);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isZero();
    }

    @Test
    void explainAttributesFallbackToTheUnsupportedAggregate() {
        System.setProperty(
                StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, StreamFusionPlannerFactory.class.getName());
        StreamTableEnvironment tableEnvironment =
                StreamTableEnvironment.create(StreamExecutionEnvironment.getExecutionEnvironment());

        assertThat(tableEnvironment.explainSql(SQL))
                .contains("== StreamFusion Acceleration ==")
                .contains("Accelerated: no")
                .contains("StreamExecGroupAggregate")
                .contains("operator has no StreamFusion physical implementation")
                .doesNotContain("StreamExecExpand: operator has no StreamFusion physical implementation");
    }
}
