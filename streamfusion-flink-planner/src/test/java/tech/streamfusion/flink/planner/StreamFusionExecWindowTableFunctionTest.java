/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.planner.plan.logical.TimeAttributeWindowingStrategy;
import org.apache.flink.table.planner.plan.logical.TumblingWindowSpec;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecWindowTableFunction;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;
import org.junit.jupiter.api.Test;

class StreamFusionExecWindowTableFunctionTest {
    @Test
    void representsAlignedWindowWithADistinctStreamFusionPhysicalNode() {
        TimestampType rowtime = new TimestampType(false, TimestampKind.ROWTIME, 3);
        TimeAttributeWindowingStrategy strategy =
                new TimeAttributeWindowingStrategy(new TumblingWindowSpec(Duration.ofSeconds(5), null), rowtime, 1);
        RowType outputType = RowType.of(
                new IntType(false),
                rowtime,
                new TimestampType(false, 3),
                new TimestampType(false, 3),
                new TimestampType(false, 3));

        StreamFusionExecWindowTableFunction node = new StreamFusionExecWindowTableFunction(
                new Configuration(), strategy, InputProperty.DEFAULT, outputType, "window");

        assertThat(node).isInstanceOf(CommonExecWindowTableFunction.class);
        assertThat(node.getDescription()).isEqualTo("window");
    }
}
