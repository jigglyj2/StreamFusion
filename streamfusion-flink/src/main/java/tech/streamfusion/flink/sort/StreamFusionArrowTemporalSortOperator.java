/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.sort;

import java.nio.file.Path;
import java.util.Collections;
import org.apache.flink.api.common.operators.ProcessingTimeService.ProcessingTimeCallback;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowTemporalSortCDataBridge;
import tech.streamfusion.flink.metrics.FlinkMetricParity;
import tech.streamfusion.flink.state.AbstractStreamFusionArrowKeyedStateOperator;
import tech.streamfusion.nativebridge.NativeMemoryManager;
import tech.streamfusion.nativebridge.NativeTemporalSortBridge;

/** Time-ascending native sort over Arrow rows with native timers and keyed state. */
final class StreamFusionArrowTemporalSortOperator extends AbstractStreamFusionArrowKeyedStateOperator
        implements OneInputStreamOperator<ArrowRowDataBatch, ArrowRowDataBatch>,
                BoundedOneInput,
                ProcessingTimeCallback {
    private final RowType outputType;
    private final boolean processingTime;

    private transient ListState<Long> watermarkState;
    private transient long restoredWatermark = Long.MIN_VALUE;
    private transient long currentWatermark = Long.MIN_VALUE;
    private transient long registeredProcessingTimer = Long.MAX_VALUE;
    private transient long[] observedStatistics;
    private transient Counter lateRecordsDropped;

    StreamFusionArrowTemporalSortOperator(RowType outputType, boolean processingTime, byte[] plan) {
        super(plan, "temporal sort");
        this.outputType = outputType;
        this.processingTime = processingTime;
    }

    @Override
    public void open() throws Exception {
        super.open();
        observedStatistics = NativeTemporalSortBridge.statistics(nativeHandle());
        MetricGroup diagnostics = getMetricGroup().addGroup("StreamFusion");
        diagnostics.gauge("pendingEventTimeTimers", () -> NativeTemporalSortBridge.statistics(nativeHandle())[5]);
        diagnostics.gauge("pendingProcessingTimeTimers", () -> NativeTemporalSortBridge.statistics(nativeHandle())[6]);
        diagnostics.gauge(
                "watermarkLatency",
                () -> currentWatermark < 0
                        ? 0L
                        : getProcessingTimeService().getCurrentProcessingTime() - currentWatermark);
        lateRecordsDropped = diagnostics.counter("lateRecordsDropped");
        if (!processingTime && restoredWatermark != Long.MIN_VALUE) {
            currentWatermark = restoredWatermark;
            drainTimers(false, restoredWatermark);
        }
        scheduleNextProcessingTimer();
    }

    @Override
    public void processElement(StreamRecord<ArrowRowDataBatch> element) throws Exception {
        ArrowRowDataBatch input = element.getValue();
        try {
            long[] processingTimes = null;
            if (processingTime) {
                processingTimes = new long[input.size()];
                for (int index = 0; index < processingTimes.length; index++) {
                    processingTimes[index] = getProcessingTimeService().getCurrentProcessingTime();
                }
            }
            ArrowTemporalSortCDataBridge.process(nativeHandle(), input, processingTimes, memoryManager());
            FlinkMetricParity.replacePhysicalRecords(
                    getMetricGroup().getIOMetricGroup().getNumRecordsInCounter(), 1, input.size());
            recordProcessedWithoutStateCalls(input);
            updateStatistics();
            if (processingTime) {
                scheduleNextProcessingTimer();
            } else if (NativeTemporalSortBridge.nextEventTimer(nativeHandle()) <= currentWatermark) {
                drainTimers(false, currentWatermark);
            }
        } catch (Throwable failure) {
            recordProcessingFailure();
            throw failure;
        }
    }

    @Override
    public void processWatermark(Watermark watermark) throws Exception {
        if (!processingTime && watermark.getTimestamp() > currentWatermark) {
            currentWatermark = watermark.getTimestamp();
            drainTimers(false, currentWatermark);
            recordWatermark();
        }
        super.processWatermark(watermark);
    }

    @Override
    public void onProcessingTime(long timestamp) throws Exception {
        registeredProcessingTimer = Long.MAX_VALUE;
        drainTimers(true, timestamp);
        scheduleNextProcessingTimer();
    }

    @Override
    public void endInput() throws Exception {
        drainTimers(processingTime, Long.MAX_VALUE);
    }

    private void drainTimers(boolean processing, long progress) throws Exception {
        long next = processing
                ? NativeTemporalSortBridge.nextProcessingTimer(nativeHandle())
                : NativeTemporalSortBridge.nextEventTimer(nativeHandle());
        while (next != Long.MAX_VALUE && next <= progress) {
            try (ArrowRowDataBatch result = processing
                    ? ArrowTemporalSortCDataBridge.advanceProcessingTime(
                            nativeHandle(), progress, outputType, allocator(), memoryManager())
                    : ArrowTemporalSortCDataBridge.advanceEventTime(
                            nativeHandle(), progress, outputType, allocator(), memoryManager())) {
                int physicalOutput = 0;
                if (result.size() > 0) {
                    output.collect(new StreamRecord<>(result));
                    physicalOutput = 1;
                }
                FlinkMetricParity.replacePhysicalRecords(
                        getMetricGroup().getIOMetricGroup().getNumRecordsOutCounter(), physicalOutput, result.size());
                recordTimerOutput(result, processing);
            }
            next = processing
                    ? NativeTemporalSortBridge.nextProcessingTimer(nativeHandle())
                    : NativeTemporalSortBridge.nextEventTimer(nativeHandle());
        }
        updateStatistics();
    }

    private void scheduleNextProcessingTimer() {
        if (!processingTime) {
            return;
        }
        long next = NativeTemporalSortBridge.nextProcessingTimer(nativeHandle());
        if (next != Long.MAX_VALUE && next != registeredProcessingTimer) {
            registeredProcessingTimer = next;
            getProcessingTimeService().registerTimer(next, this);
        }
    }

    private void updateStatistics() {
        long[] current = NativeTemporalSortBridge.statistics(nativeHandle());
        if (current.length != 8 || observedStatistics.length != 8) {
            throw new IllegalStateException("Native temporal sort statistics have an incompatible shape");
        }
        recordNativeWindowStatistics(
                current[0] - observedStatistics[0],
                current[1] - observedStatistics[1],
                current[2] - observedStatistics[2],
                current[3] - observedStatistics[3],
                current[4] - observedStatistics[4]);
        lateRecordsDropped.inc(current[7] - observedStatistics[7]);
        observedStatistics = current;
    }

    @Override
    protected void afterNativeStateInitialized(StateInitializationContext context) throws Exception {
        watermarkState = context.getOperatorStateStore()
                .getUnionListState(new ListStateDescriptor<>("watermark", LongSerializer.INSTANCE));
        if (context.isRestored()) {
            for (Long watermark : watermarkState.get()) {
                restoredWatermark =
                        restoredWatermark == Long.MIN_VALUE ? watermark : Math.min(restoredWatermark, watermark);
            }
        }
    }

    @Override
    protected void beforeNativeStateSnapshot(StateSnapshotContext context) throws Exception {
        watermarkState.update(Collections.singletonList(currentWatermark));
    }

    @Override
    protected long createMemoryHandle(
            byte[] plan, int maxParallelism, int firstKeyGroup, int lastKeyGroup, NativeMemoryManager memoryManager) {
        return NativeTemporalSortBridge.create(plan, maxParallelism, firstKeyGroup, lastKeyGroup, memoryManager);
    }

    @Override
    protected long createRocksDbHandle(
            byte[] plan,
            int maxParallelism,
            int firstKeyGroup,
            int lastKeyGroup,
            Path databasePath,
            long memoryLimit,
            NativeMemoryManager memoryManager) {
        return NativeTemporalSortBridge.createRocksDb(
                plan, maxParallelism, firstKeyGroup, lastKeyGroup, databasePath, memoryLimit, memoryManager);
    }

    @Override
    protected byte[] snapshotKeyGroup(long handle, int keyGroup) {
        return NativeTemporalSortBridge.snapshot(handle, keyGroup);
    }

    @Override
    protected void restoreKeyGroup(long handle, int keyGroup, byte[] state) {
        NativeTemporalSortBridge.restore(handle, keyGroup, state);
    }

    @Override
    protected void checkpointRocks(long handle, Path checkpointDirectory) {
        NativeTemporalSortBridge.checkpointRocks(handle, checkpointDirectory);
    }

    @Override
    protected void importRocksCheckpoint(
            long handle, Path checkpointDirectory, int firstKeyGroup, int lastKeyGroup, long memoryLimit) {
        NativeTemporalSortBridge.importRocksCheckpoint(
                handle, checkpointDirectory, firstKeyGroup, lastKeyGroup, memoryLimit);
    }

    @Override
    protected void destroyHandle(long handle) {
        NativeTemporalSortBridge.destroy(handle);
    }
}
