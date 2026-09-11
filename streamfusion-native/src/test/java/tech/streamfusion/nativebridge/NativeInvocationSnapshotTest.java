/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.nativebridge;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class NativeInvocationSnapshotTest {
    @Test
    void retainsCounterSectionsGaugeBitsAndSignedDeadlines() {
        long bits = Double.doubleToRawLongBits(-0.0);
        var snapshot = new NativeInvocationSnapshot(new long[] {1, 3, 1, 2, 9, 20, 30, bits, 9, Long.MIN_VALUE});
        assertThat(snapshot.metrics()).containsExactly(9, 20, 30);
        assertThat(snapshot.gauges()).containsExactly(bits);
        assertThat(snapshot.deadlines()).containsExactly(9, Long.MIN_VALUE);
        var empty = new NativeInvocationSnapshot(new long[] {1, 0, 0, 0});
        assertThat(empty.metrics()).isEmpty();
        assertThat(empty.gauges()).isEmpty();
        assertThat(empty.deadlines()).isEmpty();
    }

    @Test
    void rejectsVersionLengthOverflowAndSectionMisalignment() {
        for (long[] value : new long[][] {
            null,
            {},
            {2, 0, 0, 0},
            {1, 1, 0, 0, 1},
            {1, 0, 0, 1, 1},
            {1, 0, -1, 0},
            {1, 0, Long.MAX_VALUE, 0},
            {1, 0, 0, 0, 5}
        }) {
            assertThatThrownBy(() -> new NativeInvocationSnapshot(value)).isInstanceOf(IllegalArgumentException.class);
        }
    }
}
