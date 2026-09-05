/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.state;

import java.nio.file.Path;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.nativebridge.NativeKeyedStateBridge;
import tech.streamfusion.nativebridge.NativeMemoryManager;

/** Flink V1 adapter for the shared persistent native keyed-state lifecycle. */
public abstract class AbstractStreamFusionArrowKeyedStateOperator extends AbstractStreamOperator<ArrowRowDataBatch>
        implements NativeIncrementalStateParticipant {
    private final NativeKeyedStateLifecycle lifecycle;

    protected AbstractStreamFusionArrowKeyedStateOperator(
            byte[] serializedPlan, String stateName, NativeKeyedStateBridge bridge) {
        lifecycle = new NativeKeyedStateLifecycle(serializedPlan, stateName, bridge);
    }

    @Override
    public final void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        lifecycle.initialize(
                context,
                getContainingTask().getEnvironment(),
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

    protected final long managedMemoryUsed() {
        return lifecycle.managedMemoryUsed();
    }

    protected final void recordProcessed(ArrowRowDataBatch input, ArrowRowDataBatch output) {
        lifecycle.metrics().processed(input, output);
    }

    protected final void recordProcessedWithoutStateCalls(ArrowRowDataBatch input, ArrowRowDataBatch output) {
        lifecycle.metrics().processedWithoutStateCalls(input, output);
    }

    protected final void recordProcessedWithoutStateCalls(long inputRows, ArrowRowDataBatch output) {
        lifecycle.metrics().processedWithoutStateCalls(inputRows, output);
    }

    protected final void recordProcessedWithoutStateCalls(ArrowRowDataBatch input) {
        lifecycle.metrics().processedWithoutStateCalls(input);
    }

    protected final void recordNativeWindowStatistics(
            long stateReads, long stateWrites, long registered, long deleted, long fired) {
        lifecycle.metrics().nativeWindowStatistics(stateReads, stateWrites, registered, deleted, fired);
    }

    protected final void recordProcessingFailure() {
        lifecycle.metrics().processingFailed();
    }

    protected final void recordTimerOutput(ArrowRowDataBatch output, boolean processingTime) {
        lifecycle.metrics().timerOutput(output, processingTime);
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

    public void endInput() throws Exception {}

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
        super.snapshotState(context);
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
    public final void close() throws Exception {
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
