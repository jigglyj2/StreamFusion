/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.operator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.TestingNativeMemoryManager;
import tech.streamfusion.nativebridge.NativeExecutionContext;
import tech.streamfusion.proto.plan.v1.Input;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;

class NativeProcessingTimeDeadlinesTest {
    @Test
    void packagedJniReturnsNoDeadlinesForStatelessPlansAndRejectsClosedHandles() {
        var memory = TestingNativeMemoryManager.create();
        var plan = NativePlan.newBuilder()
                .setProtocolVersion(3)
                .setRoot(Operator.newBuilder().setPlanNodeId(1).setInput(Input.getDefaultInstance()))
                .build()
                .toByteArray();
        var context = new NativeExecutionContext(plan, memory);
        try (context) {
            long available = memory.available();
            assertThat(context.processingTimeDeadlines()).isEmpty();
            assertThat(context.processingTimeDeadlines()).isEmpty();
            assertThat(memory.available()).isEqualTo(available);
        }
        assertThat(memory.available()).isEqualTo(memory.limit());
        assertThatThrownBy(context::processingTimeDeadlines).hasMessageContaining("closed");
    }
}
