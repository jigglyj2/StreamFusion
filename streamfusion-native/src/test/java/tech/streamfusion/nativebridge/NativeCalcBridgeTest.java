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
import tech.streamfusion.proto.plan.v1.EmptyType;
import tech.streamfusion.proto.plan.v1.Expression;
import tech.streamfusion.proto.plan.v1.Input;
import tech.streamfusion.proto.plan.v1.InputReference;
import tech.streamfusion.proto.plan.v1.LogicalType;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;

class NativeCalcBridgeTest {
    @Test
    void executesIdentityCalcInDataFusion() {
        LogicalType integer = LogicalType.newBuilder()
                .setNullable(false)
                .setInteger(EmptyType.getDefaultInstance())
                .build();
        Operator input = Operator.newBuilder().setInput(Input.newBuilder()).build();
        NativePlan plan = NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder()
                        .setCalc(Calc.newBuilder()
                                .setInput(input)
                                .addProjections(Expression.newBuilder()
                                        .setInputReference(InputReference.newBuilder()
                                                .setIndex(0)
                                                .setType(integer)))))
                .build();
        NativeCalcBridge.resetMetrics();

        int[] output = NativeCalcBridge.executeIdentity(plan.toByteArray(), new int[] {1, 2, 3});

        assertThat(output).containsExactly(1, 2, 3);
        assertThat(NativeCalcBridge.executedBatchCount()).isOne();
    }
}
