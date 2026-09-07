/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.state;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.runtime.memory.MemoryManager;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.memory.FlinkManagedMemory;
import tech.streamfusion.nativebridge.NativeKeyedStateBridge;

class NativeKeyedStateLifecycleCloseTest {
    @Test
    void returnsExplicitRocksCacheAllowanceAfterDestroyEvenWhenTerminalCallbackFails() throws Exception {
        for (boolean failCallback : List.of(false, true)) {
            MemoryManager manager = MemoryManager.create(1024 * 1024, 32 * 1024);
            var constructor =
                    FlinkManagedMemory.class.getDeclaredConstructor(MemoryManager.class, long.class, String.class);
            constructor.setAccessible(true);
            FlinkManagedMemory memory = constructor.newInstance(manager, 4096L, "rocks-cache-close");
            assertThat(memory.tryReserve(1024)).isTrue();
            List<Long> destroyed = new ArrayList<>();
            var lifecycle = new NativeKeyedStateLifecycle(
                    new byte[0], "cache test", NativeKeyedStateBridge.of(null, null, null, null, null, null, handle -> {
                        assertThat(memory.reserved()).isEqualTo(1024);
                        destroyed.add(handle);
                    }));
            set(lifecycle, "nativeHandle", 42L);
            set(lifecycle, "managedMemory", memory);
            set(lifecycle, "rocksDbOperatorReservation", 1024L);
            Exception failure = new Exception("terminal callback failed");
            if (failCallback) {
                assertThatThrownBy(() -> lifecycle.close(() -> {
                            throw failure;
                        }))
                        .isSameAs(failure);
            } else {
                lifecycle.close(() -> {});
            }
            lifecycle.close(() -> {});
            assertThat(destroyed).containsExactly(42L);
            assertThat(memory.reserved()).isZero();
            assertThat(manager.verifyEmpty()).isTrue();
        }
    }

    private static void set(NativeKeyedStateLifecycle lifecycle, String name, Object value) throws Exception {
        var field = NativeKeyedStateLifecycle.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(lifecycle, value);
    }

    @Test
    void callbackSeesLiveHandleAndFailureStillDestroysItExactlyOnce() throws Exception {
        List<Long> destroyed = new ArrayList<>();
        NativeKeyedStateLifecycle lifecycle = new NativeKeyedStateLifecycle(
                new byte[0],
                "close test",
                NativeKeyedStateBridge.of(null, null, null, null, null, null, destroyed::add));
        var field = NativeKeyedStateLifecycle.class.getDeclaredField("nativeHandle");
        field.setAccessible(true);
        field.setLong(lifecycle, 42);
        Exception failure = new Exception("failed terminal metric callback");
        assertThatThrownBy(() -> lifecycle.close(() -> {
                    assertThat(lifecycle.nativeHandle()).isEqualTo(42);
                    throw failure;
                }))
                .isSameAs(failure);
        assertThat(lifecycle.nativeHandle()).isZero();
        assertThat(destroyed).containsExactly(42L);
        lifecycle.close(() -> {});
        assertThat(destroyed).containsExactly(42L);
    }
}
