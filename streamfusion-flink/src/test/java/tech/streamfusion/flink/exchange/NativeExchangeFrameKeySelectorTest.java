/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.exchange;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class NativeExchangeFrameKeySelectorTest {
    @ParameterizedTest
    @ValueSource(ints = {1, 2, 7, 128, 32768})
    void syntheticKeysPreserveEveryFrameKeyGroup(int maxParallelism) throws Exception {
        NativeExchangeFrameKeySelector selector = new NativeExchangeFrameKeySelector(maxParallelism);
        for (int keyGroup = 0; keyGroup < maxParallelism; keyGroup++) {
            NativeExchangeFrame frame = new NativeExchangeFrame(keyGroup, new byte[0], new byte[0]);
            assertThat(KeyGroupRangeAssignment.assignToKeyGroup(selector.getKey(frame), maxParallelism))
                    .isEqualTo(keyGroup);
        }
    }
}
