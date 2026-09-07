/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.state;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.flink.runtime.memory.MemoryManager;
import org.junit.jupiter.api.Test;

class NativeRocksDbMemoryLeaseTest {
    @Test
    void sharesOnlyWithinTheFlinkResourceAndReleasesAfterItsLastOwner() throws Exception {
        var first = MemoryManager.create(16 << 20, 32 << 10);
        var second = MemoryManager.create(16 << 20, 32 << 10);
        var a = NativeRocksDbMemoryLease.reserve(first, 0.5);
        var b = NativeRocksDbMemoryLease.reserve(first, 0.5);
        var c = NativeRocksDbMemoryLease.reserve(second, 0.5);
        try {
            assertThat(a.size()).isEqualTo(c.size());
            assertThat(a.scopeId()).isEqualTo(b.scopeId()).isNotEqualTo(c.scopeId());
            a.close();
            assertThat(first.verifyEmpty()).isFalse();
            b.close();
            assertThat(first.verifyEmpty()).isTrue();
            try (var replacement = NativeRocksDbMemoryLease.reserve(first, 0.5)) {
                assertThat(replacement.scopeId()).isNotEqualTo(a.scopeId());
            }
        } finally {
            a.close();
            b.close();
            c.close();
        }
        assertThat(first.verifyEmpty()).isTrue();
        assertThat(second.verifyEmpty()).isTrue();
        first.shutdown();
        second.shutdown();
    }
}
