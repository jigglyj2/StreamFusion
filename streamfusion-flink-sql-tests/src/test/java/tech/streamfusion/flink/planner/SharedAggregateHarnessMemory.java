/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

/** Assigns the same explicit test memory manager before either engine initializes state. */
final class SharedAggregateHarnessMemory {
    private SharedAggregateHarnessMemory() {}

    static void configureLocalRecovery(
            org.apache.flink.runtime.state.TaskStateManager manager,
            org.apache.flink.runtime.state.LocalRecoveryConfig local)
            throws Exception {
        if (local == null) return;
        // The upstream test manager has no configuration setter; install before initialization.
        var field = org.apache.flink.runtime.state.TestTaskStateManager.class.getDeclaredField(
                "localRecoveryDirectoryProvider");
        field.setAccessible(true);
        field.set(manager, local);
    }

    static void configure(org.apache.flink.runtime.execution.Environment environment, long bytes) throws Exception {
        if (bytes <= 0) return;
        // Flink's harness defaults to 3 MiB; replace its empty manager before opening operators.
        var manager = environment.getMemoryManager();
        var field = environment.getClass().getDeclaredField("memManager");
        field.setAccessible(true);
        field.set(
                environment,
                org.apache.flink.runtime.memory.MemoryManagerBuilder.newBuilder()
                        .setMemorySize(bytes)
                        .build());
        manager.shutdown();
    }
}
