/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.window;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.planner.plan.logical.SessionWindowSpec;
import org.apache.flink.table.planner.plan.logical.TimeAttributeWindowingStrategy;
import org.apache.flink.table.planner.plan.logical.TumblingWindowSpec;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;
import org.junit.jupiter.api.Test;

class StreamFusionWindowTableFunctionSupportTest {
    @Test
    void acceptsEventTimeTumbleAndExplainsSemanticFallbacks() {
        TimestampType rowtime = new TimestampType(false, TimestampKind.ROWTIME, 3);
        RowType input = RowType.of(new IntType(false), rowtime);
        RowType output = outputType(rowtime);

        assertThat(StreamFusionWindowTableFunctionTranslator.unsupportedReason(
                        input,
                        output,
                        new TimeAttributeWindowingStrategy(
                                new TumblingWindowSpec(Duration.ofSeconds(5), null), rowtime, 1),
                        new Configuration()))
                .isNull();
        LocalZonedTimestampType ltz = new LocalZonedTimestampType(false, TimestampKind.ROWTIME, 3);
        assertThat(StreamFusionWindowTableFunctionTranslator.unsupportedReason(
                        RowType.of(new IntType(false), ltz),
                        outputType(ltz),
                        new TimeAttributeWindowingStrategy(new TumblingWindowSpec(Duration.ofSeconds(5), null), ltz, 1),
                        new Configuration()))
                .contains("TIMESTAMP_LTZ");
        assertThat(StreamFusionWindowTableFunctionTranslator.unsupportedReason(
                        input,
                        output,
                        new TimeAttributeWindowingStrategy(
                                new SessionWindowSpec(Duration.ofSeconds(5), new int[] {0}), rowtime, 1),
                        new Configuration()))
                .isNull();
    }

    private static RowType outputType(org.apache.flink.table.types.logical.LogicalType timeType) {
        return RowType.of(
                new IntType(false),
                timeType,
                new TimestampType(false, 3),
                new TimestampType(false, 3),
                new TimestampType(false, 3));
    }
}
