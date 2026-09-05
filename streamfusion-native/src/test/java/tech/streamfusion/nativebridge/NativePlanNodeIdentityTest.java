/*
 * Copyright 2026 StreamFusion Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package tech.streamfusion.nativebridge;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import tech.streamfusion.proto.plan.v1.Calc;
import tech.streamfusion.proto.plan.v1.Input;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;

class NativePlanNodeIdentityTest {
    @Test
    void assignsStablePreOrderIdsWithoutReplacingPlannerIds() throws Exception {
        Operator input =
                Operator.newBuilder().setInput(Input.getDefaultInstance()).build();
        NativePlan original = NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder()
                        .setPlanNodeId(41)
                        .setCalc(Calc.newBuilder().setInput(input)))
                .build();

        NativePlan identified = NativePlan.parseFrom(NativePlanNodeIdentity.assign(original.toByteArray()));

        assertThat(identified.getRoot().getPlanNodeId()).isEqualTo(41);
        assertThat(identified.getRoot().getCalc().getInput().getPlanNodeId()).isEqualTo(42);
        assertThat(NativePlanNodeIdentity.assign(original.toByteArray()))
                .containsExactly(NativePlanNodeIdentity.assign(original.toByteArray()));
    }
}
