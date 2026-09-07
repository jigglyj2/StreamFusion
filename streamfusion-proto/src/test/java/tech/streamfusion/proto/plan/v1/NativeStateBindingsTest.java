/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.proto.plan.v1;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class NativeStateBindingsTest {
    @Test
    void roundTripsIndependentNodeIdentitiesAndFlinkResourceAssignments() throws Exception {
        var first = NativeStateBinding.newBuilder()
                .setPlanNodeId(1L << 32)
                .setMaxParallelism(128)
                .setFirstKeyGroup(32)
                .setLastKeyGroup(63)
                .setMemory(NativeMemoryState.getDefaultInstance())
                .build();
        var second = first.toBuilder()
                .setPlanNodeId((1L << 32) + 9)
                .setRocksdb(NativeRocksDbState.newBuilder()
                        .setPluginPath("/tmp/plugin.so")
                        .setDatabasePath("/tmp/node-9")
                        .setMemoryLimit(16L << 20))
                .build();
        var bindings = NativeStateBindings.newBuilder()
                .setProtocolVersion(1)
                .addBindings(first)
                .addBindings(second)
                .build();
        assertThat(NativeStateBindings.parseFrom(bindings.toByteArray())).isEqualTo(bindings);
        assertThat(second.hasMemory()).isFalse();
        assertThat(second.getFirstKeyGroup()).isEqualTo(32);
        assertThat(second.getLastKeyGroup()).isEqualTo(63);
    }
}
