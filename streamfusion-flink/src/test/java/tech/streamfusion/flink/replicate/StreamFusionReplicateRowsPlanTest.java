/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.replicate;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;
import tech.streamfusion.proto.plan.v1.Expression;
import tech.streamfusion.proto.plan.v1.InputReference;
import tech.streamfusion.proto.plan.v1.NativePlan;

class StreamFusionReplicateRowsPlanTest {
    @Test
    void serializesAStandaloneReplicatorWithItsExpressions() throws Exception {
        NativePlan plan =
                NativePlan.parseFrom(StreamFusionReplicateRowsPlan.create(input(0), List.of(input(1), input(2))));

        assertThat(plan.getProtocolVersion()).isEqualTo(1);
        assertThat(plan.getRoot().getReplicateRows().getInput().hasInput()).isTrue();
        assertThat(plan.getRoot().getReplicateRows().getRepetition()).isEqualTo(input(0));
        assertThat(plan.getRoot().getReplicateRows().getValuesList()).containsExactly(input(1), input(2));
    }

    private static Expression input(int index) {
        return Expression.newBuilder()
                .setInputReference(InputReference.newBuilder().setIndex(index))
                .build();
    }
}
