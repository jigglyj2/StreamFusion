/*
 * Copyright 2026 StreamFusion Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecValues;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

class StreamFusionExecValuesTest {
    @Test
    void representsValuesWithADistinctStreamFusionPhysicalNode() {
        StreamFusionExecValues values = new StreamFusionExecValues(
                new Configuration(), Collections.emptyList(), RowType.of(new IntType()), "values");
        values.setInputEdges(Collections.emptyList());

        assertThat(values).isInstanceOf(CommonExecValues.class);
        assertThat(values.getInputEdges()).isEmpty();
        assertThat(values.getDescription()).isEqualTo("values");
    }
}
