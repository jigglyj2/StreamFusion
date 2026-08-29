/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecExchange;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

class StreamFusionExecExchangeTest {
    @Test
    void representsHashExchangeWithADistinctPhysicalNode() {
        InputProperty input = InputProperty.builder()
                .requiredDistribution(InputProperty.hashDistribution(new int[] {0}))
                .build();
        RowType rowType = RowType.of(new IntType(false));

        StreamFusionExecExchange node = new StreamFusionExecExchange(new Configuration(), input, rowType, "exchange");

        assertThat(node).isInstanceOf(CommonExecExchange.class);
        assertThat(node.getDescription()).isEqualTo("exchange");
        assertThat(StreamFusionExchangeSupport.unsupportedReason(rowType, input.getRequiredDistribution()))
                .isNull();
    }

    @Test
    void explainsComplexHashKeyFallbackPrecisely() {
        InputProperty input = InputProperty.builder()
                .requiredDistribution(InputProperty.hashDistribution(new int[] {0}))
                .build();
        RowType rowType = RowType.of(new ArrayType(new IntType()));

        assertThat(StreamFusionExchangeSupport.unsupportedReason(rowType, input.getRequiredDistribution()))
                .contains("exchange key 0")
                .contains("no exact Flink BinaryRow encoding");
    }
}
