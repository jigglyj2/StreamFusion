/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.exchange;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.junit.jupiter.api.Test;

class NativeExchangeFramesTest {
    @Test
    void decodesDestinationAndSchemaFreeArrowPayloads() {
        byte[] encoded = ByteBuffer.allocate(4 + 12 + 2 + 3 + 12 + 1 + 2)
                .order(ByteOrder.LITTLE_ENDIAN)
                .putInt(2)
                .putInt(1)
                .putInt(2)
                .putInt(3)
                .put(new byte[] {10, 11})
                .put(new byte[] {20, 21, 22})
                .putInt(3)
                .putInt(1)
                .putInt(2)
                .put((byte) 30)
                .put(new byte[] {40, 41})
                .array();

        java.util.List<NativeExchangeFrame> frames = NativeExchangeFrames.decode(encoded);
        assertThat(frames).hasSize(2);
        assertThat(frames.get(0).keyGroup()).isEqualTo(1);
        assertThat(frames.get(0).metadata()).containsExactly(10, 11);
        assertThat(frames.get(0).body()).containsExactly(20, 21, 22);
        assertThat(frames.get(1).keyGroup()).isEqualTo(3);
        assertThat(frames.get(1).metadata()).containsExactly(30);
        assertThat(frames.get(1).body()).containsExactly(40, 41);
    }

    @Test
    void rejectsTruncatedNativePayload() {
        byte[] encoded = ByteBuffer.allocate(16)
                .order(ByteOrder.LITTLE_ENDIAN)
                .putInt(1)
                .putInt(0)
                .putInt(1)
                .putInt(1)
                .array();

        assertThatThrownBy(() -> NativeExchangeFrames.decode(encoded))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exceeds its JNI envelope");
    }
}
