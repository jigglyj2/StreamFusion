/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.state;

import org.apache.flink.runtime.state.StateBackend;

/** Rechecks public programmatic settings before a native owner acquires any state resources. */
final class NativeRocksDbBackendSupport {
    private NativeRocksDbBackendSupport() {}

    static void validate(StateBackend backend) throws ReflectiveOperationException {
        Object memory = backend.getClass().getMethod("getMemoryConfiguration").invoke(backend);
        // Flink chooses fixed-per-slot first, even when managed=true. Reserving a managed
        // fraction here would silently replace the user's fixed allocation and sharing rules.
        if ((Boolean) memory.getClass().getMethod("isUsingFixedMemoryPerSlot").invoke(memory)) {
            throw unsupported(
                    "state.backend.rocksdb.memory.fixed-per-slot",
                    "one fixed cache budget cannot yet be shared across Java and native RocksDB owners");
        }
        if (!(Boolean) memory.getClass().getMethod("isUsingManagedMemory").invoke(memory)) {
            throw unsupported(
                    "state.backend.rocksdb.memory.managed",
                    "native state currently requires Flink's managed STATE_BACKEND budget");
        }
        Object factory = backend.getClass().getMethod("getRocksDBOptions").invoke(backend);
        if (factory != null) {
            throw unsupported(
                    "state.backend.rocksdb.options-factory",
                    "custom factory " + factory.getClass().getName() + " cannot be translated to native options");
        }
        Object timer = backend.getClass().getMethod("getPriorityQueueStateType").invoke(backend);
        if (!((Enum<?>) timer).name().equals("ROCKSDB")) {
            throw unsupported(
                    "state.backend.rocksdb.timer-service.factory",
                    "native timer-service parity has not been verified for " + timer);
        }
    }

    private static IllegalArgumentException unsupported(String key, String reason) {
        return new IllegalArgumentException("Cannot preserve native RocksDB configuration: " + key + "; " + reason);
    }
}
