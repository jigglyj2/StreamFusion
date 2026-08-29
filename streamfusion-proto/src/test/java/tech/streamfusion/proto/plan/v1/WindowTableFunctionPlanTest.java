/*
 * Copyright 2026 StreamFusion Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package tech.streamfusion.proto.plan.v1;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class WindowTableFunctionPlanTest {
    @Test
    void roundTripsWindowKindAndFlinkMillisecondParameters() throws Exception {
        WindowTableFunction window = WindowTableFunction.newBuilder()
                .setInput(Operator.newBuilder().setInput(Input.newBuilder()))
                .setTimeAttributeIndex(2)
                .setKind(WindowKind.WINDOW_KIND_HOP)
                .setSizeMillis(10_000)
                .setSlideOrStepMillis(4_000)
                .setOffsetMillis(1_000)
                .build();
        NativePlan plan = NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setWindowTableFunction(window))
                .build();

        WindowTableFunction decoded =
                NativePlan.parseFrom(plan.toByteArray()).getRoot().getWindowTableFunction();

        assertThat(decoded).isEqualTo(window);
    }
}
