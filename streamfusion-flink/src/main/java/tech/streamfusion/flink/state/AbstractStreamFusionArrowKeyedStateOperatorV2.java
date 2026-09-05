/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.state;

import java.nio.file.Path;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorV2;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.nativebridge.NativeKeyedStateBridge;
import tech.streamfusion.nativebridge.NativeMemoryManager;

/** Flink V2 adapter for the shared persistent native keyed-state lifecycle. */
public abstract class AbstractStreamFusionArrowKeyedStateOperatorV2 extends AbstractStreamOperatorV2<ArrowRowDataBatch>
        implements NativeIncrementalStateParticipant {
    private final NativeKeyedStateLifecycle lifecycle;
    private final transient Environment taskEnvironment;

    protected AbstractStreamFusionArrowKeyedStateOperatorV2(
            StreamOperatorParameters<ArrowRowDataBatch> parameters,
            int inputCount,
            byte[] serializedPlan,
            String stateName,
            NativeKeyedStateBridge bridge) {
        super(parameters, inputCount);
        lifecycle = new NativeKeyedStateLifecycle(serializedPlan, stateName, bridge);
        taskEnvironment = parameters.getContainingTask().getEnvironment();
    }

    @Override
    public final void initializeState(StateInitializationContext context) throws Exception {
        lifecycle.initialize(
                context,
                taskEnvironment,
                getOperatorConfig(),
                getMetricGroup(),
                getKeyedStateBackend(),
                getRuntimeContext().getTaskInfo().getMaxNumberOfParallelSubtasks(),
                this);
        afterNativeStateInitialized(context);
    }

    @Override
    protected final boolean isUsingCustomRawKeyedState() {
        return true;
    }

    protected final long nativeHandle() {
        return lifecycle.nativeHandle();
    }

    protected final BufferAllocator allocator() {
        return lifecycle.allocator();
    }

    protected final NativeMemoryManager memoryManager() {
        return lifecycle.memoryManager();
    }

    protected final void recordProcessedWithoutStateCalls(long inputRows, ArrowRowDataBatch output) {
        lifecycle.metrics().processedWithoutStateCalls(inputRows, output);
    }

    protected final void recordNativeStateStatistics(long stateReads, long stateWrites) {
        lifecycle.metrics().nativeWindowStatistics(stateReads, stateWrites, 0, 0, 0);
    }

    protected final void recordProcessingFailure() {
        lifecycle.metrics().processingFailed();
    }

    protected final void recordWatermark() {
        lifecycle.metrics().watermarkAdvanced();
    }

    protected final List<byte[]> preencodeKeys(
            ArrowRowDataBatch input, RowDataKeySelector keySelector, String operatorName) throws Exception {
        return FlinkBinaryRowKeyEncoder.encode(input, keySelector, operatorName);
    }

    protected static boolean requiresPreencodedKeys(RowType rowType, int[] keyFields) {
        return FlinkBinaryRowKeyEncoder.requiresPreencoding(rowType, keyFields);
    }

    @Override
    public final OperatorSnapshotFutures snapshotState(
            long checkpointId,
            long timestamp,
            org.apache.flink.runtime.checkpoint.CheckpointOptions checkpointOptions,
            CheckpointStreamFactory factory)
            throws Exception {
        long startedNanos = lifecycle.beginSnapshot(checkpointId, checkpointOptions, getKeyedStateBackend());
        try {
            OperatorSnapshotFutures futures = super.snapshotState(checkpointId, timestamp, checkpointOptions, factory);
            lifecycle.snapshotSucceeded(checkpointId, checkpointOptions, startedNanos);
            return futures;
        } catch (Throwable failure) {
            lifecycle.snapshotFailed(checkpointId);
            throw failure;
        } finally {
            lifecycle.finishSnapshotAttempt();
        }
    }

    @Override
    public final void snapshotState(StateSnapshotContext context) throws Exception {
        beforeNativeStateSnapshot(context);
        lifecycle.writeRawSnapshot(context);
    }

    @Override
    public final Path prepareIncrementalCheckpoint(long checkpointId) {
        return lifecycle.prepareIncrementalCheckpoint(checkpointId);
    }

    @Override
    public final void completeIncrementalCheckpoint(long checkpointId, long uploadedBytes, long reusedBytes) {
        lifecycle.completeIncrementalCheckpoint(checkpointId, uploadedBytes, reusedBytes);
    }

    @Override
    public final void failIncrementalCheckpoint(long checkpointId) {
        lifecycle.failIncrementalCheckpoint(checkpointId);
    }

    @Override
    public final void restoreIncrementalCheckpoint(Path checkpointDirectory, KeyGroupRange restoredRange) {
        lifecycle.restoreIncrementalCheckpoint(checkpointDirectory, restoredRange);
    }

    @Override
    public void close() throws Exception {
        try {
            lifecycle.close(this::beforeNativeClose);
        } finally {
            super.close();
        }
    }

    protected void afterNativeStateInitialized(StateInitializationContext context) throws Exception {}

    protected void beforeNativeStateSnapshot(StateSnapshotContext context) throws Exception {}

    protected void beforeNativeClose() throws Exception {}
}
