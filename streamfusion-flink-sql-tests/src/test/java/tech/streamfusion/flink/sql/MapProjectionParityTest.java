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

class MapProjectionParityTest extends SqlParityTestSupport {
    @Test
    void mapKeysAndValuesComposeWithNativeMapConstructor() throws Exception {
        assertDataStreamParity(
                "SELECT MAP_KEYS(MAP['a', metric, 'b', metric + 1]), MAP_VALUES(MAP['a', metric, 'b', metric + 1]) FROM int_input",
                Types.INT,
                DataTypes.INT(),
                Arrays.asList(Row.of(12), Row.of(-1), Row.of((Object) null)),
                "int_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount())
                .withFailMessage(StreamFusionPlanningDiagnostics.explain())
                .isGreaterThan(0);
    }

    @Test
    void mapEntriesPreservesKeyValueStructShape() throws Exception {
        assertDataStreamParity(
                "SELECT MAP_ENTRIES(MAP['a', metric, 'b', metric + 1]) FROM int_input",
                Types.INT,
                DataTypes.INT(),
                Arrays.asList(Row.of(12), Row.of(-1), Row.of((Object) null)),
                "int_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount())
                .withFailMessage(StreamFusionPlanningDiagnostics.explain())
                .isGreaterThan(0);
    }
}
