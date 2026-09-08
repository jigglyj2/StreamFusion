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

    /** Relative Flink OPERATOR weight for each independently retained native keyed-state owner. */
    public static final int STATEFUL_MANAGED_MEMORY_WEIGHT = 8;

    private final FlinkManagedMemory managedMemory;
    private final NativeExecutionContext executionContext;

    public static StreamFusionTaskMemory create(
            Environment environment,
            StreamConfig operatorConfig,
            OperatorMetricGroup metricGroup,
            String name,
            byte[] serializedPlan) {
        return createWithState(environment, operatorConfig, metricGroup, name, serializedPlan, ignored -> null);
    }

    /** Supplies task-local state bindings after Flink has assigned the shared operator allowance. */
    public static StreamFusionTaskMemory createWithState(
            Environment environment,
            StreamConfig operatorConfig,
            OperatorMetricGroup metricGroup,
            String name,
            byte[] serializedPlan,
            java.util.function.Function<tech.streamfusion.nativebridge.NativeMemoryManager, byte[]> bindings) {
        return createWithState(environment, operatorConfig, metricGroup, name, serializedPlan, bindings, null);
    }

    /** Original Flink task geometry and native state are bound before the shared tree is lowered. */
    public static StreamFusionTaskMemory createWithState(
            Environment environment,
            StreamConfig operatorConfig,
            OperatorMetricGroup metricGroup,
            String name,
            byte[] serializedPlan,
            java.util.function.Function<tech.streamfusion.nativebridge.NativeMemoryManager, byte[]> bindings,
            byte[] taskBindings) {
        FlinkManagedMemory managedMemory = FlinkManagedMemory.create(environment, operatorConfig, metricGroup, name);
        try {
            return new StreamFusionTaskMemory(
                    managedMemory,
                    new NativeExecutionContext(
                            serializedPlan, managedMemory, bindings.apply(managedMemory), taskBindings));
        } catch (RuntimeException | Error failure) {
            try {
                managedMemory.close();
            } catch (RuntimeException | Error cleanup) {
                failure.addSuppressed(cleanup);
            }
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

    /** Exchange decoding at a region edge shares the execution context's Flink allowance. */
    public tech.streamfusion.nativebridge.NativeMemoryManager nativeMemoryManager() {
        return managedMemory;
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
