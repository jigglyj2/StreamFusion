/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.sql;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

class VarcharCastFallbackTest extends SqlParityTestSupport {
    @ParameterizedTest
    @ValueSource(strings = {"VARCHAR(2)", "CHAR(2)", "CHAR(20)"})
    void narrowingAndPaddingRemainOnFlink(String target) throws Exception {
        assertFallbackDataStreamParity(
                "SELECT CAST(metric AS " + target + ") FROM cast_input",
                Types.STRING,
                DataTypes.VARCHAR(9),
                List.of(Row.of("abc"), Row.of("界😀é"), Row.of(""), Row.of(" "), Row.of((Object) null)),
                "cast_input");
        assertThat(StreamFusionPlanningDiagnostics.explain())
                .contains("projection[0]/CAST")
                .contains(target);
    }
}
