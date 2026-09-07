/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.arrow;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.protobuf.ByteString;
import com.google.protobuf.UnknownFieldSet;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import tech.streamfusion.nativebridge.NativeExecutionContext;
import tech.streamfusion.nativebridge.NativeMemoryManager;
import tech.streamfusion.proto.plan.v1.Input;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;

class NativePlanCopyAdmissionTest {
    @Test
    void rejectsCopiesBeforeEnteringEitherNativeConstructor() {
        for (int size : List.of(0, 128, 65536)) {
            byte[] plan = plan(size);
            for (byte[] bindings : new byte[][] {null, new byte[4096]}) {
                var memory = new RecordingMemory(1);
                long copies = plan.length + (bindings == null ? 0L : bindings.length);
                assertThatThrownBy(() -> new NativeExecutionContext(plan, memory, bindings))
                        .hasMessageContaining("native plan/state-binding JNI copies");
                assertThat(memory.requests).containsExactly(copies);
                assertThat(memory.releases).isEmpty();
                assertThat(memory.reserved).isZero();
            }
        }
    }

    @Test
    void nativeConstructionDenialAndMalformedBindingsReleaseTheCopyCreditLast() {
        byte[] plan = plan(4096);
        for (byte[] bindings : new byte[][] {null, new byte[2048]}) {
            var memory = new RecordingMemory(2);
            long copies = plan.length + (bindings == null ? 0L : bindings.length);
            assertThatThrownBy(() -> new NativeExecutionContext(plan, memory, bindings))
                    .isInstanceOf(IllegalStateException.class);
            assertThat(memory.requests).hasSize(2);
            assertThat(memory.requests.get(0)).isEqualTo(copies);
            assertThat(memory.releases).containsExactly(copies);
            assertThat(memory.reserved).isZero();
        }
        byte[] invalidBindings = new byte[2048];
        var memory = new RecordingMemory(0);
        assertThatThrownBy(() -> new NativeExecutionContext(plan, memory, invalidBindings))
                .hasMessageContaining("state-binding");
        assertThat(memory.releases.get(memory.releases.size() - 1))
                .isEqualTo((long) plan.length + invalidBindings.length);
        assertThat(memory.reserved).isZero();
    }

    @Test
    void successfulConstructionReturnsOnlyTemporaryCopyCreditUntilContextCloses() {
        for (int size : List.of(0, 128, 65536)) {
            byte[] plan = plan(size);
            var memory = new RecordingMemory(0);
            try (var context = new NativeExecutionContext(plan, memory)) {
                assertThat(context.identifiedPlan()).isEqualTo(plan);
                assertThat(memory.requests.get(0)).isEqualTo((long) plan.length);
                assertThat(memory.releases).containsExactly((long) plan.length);
                assertThat(memory.reserved).isPositive();
            }
            assertThat(memory.reserved).isZero();
        }
    }

    private static byte[] plan(int payloadBytes) {
        // A valid unknown field varies wire-copy size without introducing operator-specific work.
        return NativePlan.newBuilder()
                .setProtocolVersion(2)
                .setRoot(Operator.newBuilder().setPlanNodeId(1).setInput(Input.getDefaultInstance()))
                .setUnknownFields(UnknownFieldSet.newBuilder()
                        .addField(
                                999,
                                UnknownFieldSet.Field.newBuilder()
                                        .addLengthDelimited(ByteString.copyFrom(new byte[payloadBytes]))
                                        .build())
                        .build())
                .build()
                .toByteArray();
    }

    private static final class RecordingMemory implements NativeMemoryManager {
        private final int deniedRequest;
        private final List<Long> requests = new ArrayList<>();
        private final List<Long> releases = new ArrayList<>();
        private long reserved;

        private RecordingMemory(int deniedRequest) {
            this.deniedRequest = deniedRequest;
        }

        public boolean tryReserve(long bytes) {
            requests.add(bytes);
            if (requests.size() == deniedRequest || bytes > available()) return false;
            reserved += bytes;
            return true;
        }

        public void release(long bytes) {
            assertThat(bytes).isBetween(0L, reserved);
            releases.add(bytes);
            reserved -= bytes;
        }

        public long available() {
            return limit() - reserved;
        }

        public long limit() {
            return 64L << 20;
        }
    }
}
