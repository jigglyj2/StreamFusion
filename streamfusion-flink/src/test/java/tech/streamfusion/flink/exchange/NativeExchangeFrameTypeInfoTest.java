/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.exchange;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.junit.jupiter.api.Test;

class NativeExchangeFrameTypeInfoTest {
    @Test
    void installsTheArrowFrameSerializerWithoutKryo() {
        assertThat(NativeExchangeFrameTypeInfo.INSTANCE.createSerializer(new SerializerConfigImpl()))
                .isSameAs(NativeExchangeFrameSerializer.INSTANCE);
        assertThat(NativeExchangeFrameTypeInfo.INSTANCE.isKeyType()).isFalse();
    }
}
