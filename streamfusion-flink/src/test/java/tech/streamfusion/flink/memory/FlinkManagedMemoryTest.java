/*
 * Copyright 2026 StreamFusion Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package tech.streamfusion.flink.memory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.flink.runtime.memory.MemoryManager;
import org.junit.jupiter.api.Test;

class FlinkManagedMemoryTest {
    @Test
    void closedProducerKeepsOutstandingNativeBuffersChargedUntilTheirLastRelease() {
        MemoryManager manager = MemoryManager.create(1024 * 1024, 32 * 1024);
        FlinkManagedMemory memory = new FlinkManagedMemory(manager, 4096, "retained-native");
        long initial = manager.availableMemory();
        assertThat(memory.tryReserve(1024)).isTrue();
        memory.close();
        memory.close();
        assertThat(memory.available()).isZero();
        assertThat(memory.reserved()).isEqualTo(1024);
        assertThat(manager.availableMemory()).isEqualTo(initial - 1024);
        assertThat(manager.verifyEmpty()).isFalse();
        assertThatThrownBy(() -> memory.tryReserve(1)).isInstanceOf(IllegalStateException.class);
        memory.release(1024);
        assertThat(memory.reserved()).isZero();
        assertThat(manager.verifyEmpty()).isTrue();
    }

    @Test
    void sharesOneFlinkReservationBetweenArrowAndNativeConsumers() {
        MemoryManager memoryManager = MemoryManager.create(1024 * 1024, 32 * 1024);
        FlinkManagedMemory managedMemory = new FlinkManagedMemory(memoryManager, 4096, "test");
        long availableBefore = memoryManager.availableMemory();

        try (ArrowBuf buffer = managedMemory.allocator().buffer(1024)) {
            long arrowReservation = managedMemory.reserved();
            assertThat(arrowReservation).isGreaterThanOrEqualTo(1024);
            assertThat(memoryManager.availableMemory()).isEqualTo(availableBefore - arrowReservation);

            assertThat(managedMemory.tryReserve(4096 - arrowReservation)).isTrue();
            assertThat(managedMemory.tryReserve(1)).isFalse();
            managedMemory.release(4096 - arrowReservation);
        } finally {
            managedMemory.close();
        }

        assertThat(memoryManager.verifyEmpty()).isTrue();
    }

    @Test
    void transfersNativeReservationToArrowWithoutChargingItTwice() {
        MemoryManager memoryManager = MemoryManager.create(1024 * 1024, 32 * 1024);
        FlinkManagedMemory managedMemory = new FlinkManagedMemory(memoryManager, 4096, "transfer-test");

        assertThat(managedMemory.tryReserve(4096)).isTrue();
        managedMemory.transferToArrow(1024);
        try (ArrowBuf ignored = managedMemory.allocator().buffer(1024)) {
            managedMemory.finishArrowTransfer();
            assertThat(managedMemory.reserved()).isEqualTo(4096);
        }
        managedMemory.release(3072);
        managedMemory.close();

        assertThat(memoryManager.verifyEmpty()).isTrue();
    }
}
