/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.proto.plan.v1;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class RegexExtractPlanTest {
    @Test
    void soleCaptureAndInputRoundTrip() throws Exception {
        var expression = Expression.newBuilder()
                .setRegexExtract(RegexExtract.newBuilder()
                        .setOperand(Expression.newBuilder()
                                .setInputReference(InputReference.newBuilder().setIndex(2)))
                        .setCapturePattern("(?:&|^)channel_id=([^&]*)"))
                .build();
        assertThat(Expression.parseFrom(expression.toByteArray())).isEqualTo(expression);
    }
}
