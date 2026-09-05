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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.streaming.api.graph.StreamConfig;
import tech.streamfusion.nativebridge.NativeExecutionContext;

/** Owns task-scoped Arrow and native execution resources in close-safe order. */
public final class StreamFusionTaskMemory implements AutoCloseable {
    /** Minimum Flink operator-memory weight for a retained native DataFusion execution context. */
    public static final int MANAGED_MEMORY_WEIGHT = 2;

    private final FlinkManagedMemory managedMemory;
    private final NativeExecutionContext executionContext;

    public static StreamFusionTaskMemory create(
            Environment environment,
            StreamConfig operatorConfig,
            OperatorMetricGroup metricGroup,
            String name,
            byte[] serializedPlan) {
        FlinkManagedMemory managedMemory = FlinkManagedMemory.create(environment, operatorConfig, metricGroup, name);
        try {
            return new StreamFusionTaskMemory(managedMemory, new NativeExecutionContext(serializedPlan, managedMemory));
        } catch (RuntimeException failure) {
            managedMemory.close();
            throw failure;
        }
    }

    private StreamFusionTaskMemory(FlinkManagedMemory managedMemory, NativeExecutionContext executionContext) {
        this.managedMemory = managedMemory;
        this.executionContext = executionContext;
    }

    public BufferAllocator allocator() {
        return managedMemory.allocator();
    }

    public NativeExecutionContext executionContext() {
        return executionContext;
    }

    @Override
    public void close() {
        try {
            executionContext.close();
        } finally {
            managedMemory.close();
        }
    }
}
