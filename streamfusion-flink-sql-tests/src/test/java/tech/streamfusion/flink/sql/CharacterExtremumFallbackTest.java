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

import java.util.Arrays;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

class CharacterExtremumFallbackTest extends SqlParityTestSupport {
    @Test
    void fixedWidthCharacterInputFallsBackWithPaddingReason() throws Exception {
        assertDataStreamParity(
                "SELECT GREATEST(metric, CAST('b' AS CHAR(4))), "
                        + "LEAST(metric, CAST('b' AS CHAR(4))) FROM character_input",
                Types.STRING,
                DataTypes.CHAR(4),
                Arrays.asList(Row.of("a"), Row.of("z"), Row.of((Object) null)),
                "character_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isZero();
        assertThat(StreamFusionPlanningDiagnostics.explain())
                .contains("fixed-width CHAR padding")
                .contains("Accelerated: no");
    }
}
