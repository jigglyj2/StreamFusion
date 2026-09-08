/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.proto.plan.v1;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class DateFormatPlanTest {
    @Test
    void literalPatternAndTimestampRoundTrip() throws Exception {
        var plan = Expression.newBuilder()
                .setDateFormat(DateFormat.newBuilder()
                        .setOperand(Expression.newBuilder()
                                .setInputReference(InputReference.newBuilder().setIndex(2)))
                        .setPattern("'年'yyyy-MM-dd HH:mm:ss.SSS 'it''s %'"))
                .build();
        assertThat(Expression.parseFrom(plan.toByteArray())).isEqualTo(plan);
    }
}
