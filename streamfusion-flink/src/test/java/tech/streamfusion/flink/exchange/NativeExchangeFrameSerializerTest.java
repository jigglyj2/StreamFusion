/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.exchange;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.junit.jupiter.api.Test;

class NativeExchangeFrameSerializerTest {
    @Test
    void roundTripsRoutingMetadataAndArrowBuffers() throws Exception {
        NativeExchangeFrame expected = new NativeExchangeFrame(73, new byte[] {1, 2}, new byte[] {3, 4, 5});
        DataOutputSerializer output = new DataOutputSerializer(32);

        NativeExchangeFrameSerializer.INSTANCE.serialize(expected, output);
        NativeExchangeFrame actual =
                NativeExchangeFrameSerializer.INSTANCE.deserialize(new DataInputDeserializer(output.getCopyOfBuffer()));

        assertThat(actual.keyGroup()).isEqualTo(73);
        assertThat(actual.metadata()).containsExactly(1, 2);
        assertThat(actual.body()).containsExactly(3, 4, 5);
    }

    @Test
    void copiesSerializedFramesWithoutDecodingArrow() throws Exception {
        DataOutputSerializer serialized = new DataOutputSerializer(32);
        NativeExchangeFrameSerializer.INSTANCE.serialize(
                new NativeExchangeFrame(7, new byte[] {10}, new byte[] {20, 21}), serialized);
        DataOutputSerializer copied = new DataOutputSerializer(32);

        NativeExchangeFrameSerializer.INSTANCE.copy(new DataInputDeserializer(serialized.getCopyOfBuffer()), copied);

        assertThat(copied.getCopyOfBuffer()).containsExactly(serialized.getCopyOfBuffer());
    }
}
