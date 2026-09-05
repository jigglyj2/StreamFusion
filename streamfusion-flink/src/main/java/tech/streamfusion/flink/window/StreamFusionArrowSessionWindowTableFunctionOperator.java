/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.window;

import java.util.Collections;
import java.util.List;
import org.apache.flink.api.common.operators.ProcessingTimeService.ProcessingTimeCallback;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowSessionWindowTableFunctionCDataBridge;
import tech.streamfusion.flink.metrics.FlinkMetricParity;
import tech.streamfusion.flink.state.AbstractStreamFusionArrowKeyedStateOperator;
import tech.streamfusion.nativebridge.NativeSessionWindowTableFunctionBridge;

/** Native keyed merging SESSION Window TVF with event-time and processing-time timers. */
final class StreamFusionArrowSessionWindowTableFunctionOperator extends AbstractStreamFusionArrowKeyedStateOperator
        implements OneInputStreamOperator<ArrowRowDataBatch, ArrowRowDataBatch>,
                BoundedOneInput,
                ProcessingTimeCallback {
    private final RowType outputType;
    private final boolean processingTime;
    private final RowDataKeySelector keySelector;
    private final boolean preencodeKeys;

    private transient ListState<Long> watermarkState;
    private transient long restoredWatermark = Long.MIN_VALUE;
    private transient long currentWatermark = Long.MIN_VALUE;
    private transient long registeredProcessingTimer = Long.MAX_VALUE;
    private transient Counter lateRecordsDropped;
    private transient Meter lateRecordsDroppedRate;
    private transient Counter nullRowtimeDropped;
    private transient long observedNativeLateRecords;
    private transient long observedNativeNullRowtimes;
    private transient long[] observedNativeStatistics;

    StreamFusionArrowSessionWindowTableFunctionOperator(
            RowType inputType,
            RowType outputType,
            int[] partitionKeys,
            byte[] plan,
            boolean processingTime,
            RowDataKeySelector keySelector) {
        super(plan, "session window table function", NativeSessionWindowTableFunctionBridge.keyedStateBridge());
        this.outputType = outputType;
        this.processingTime = processingTime;
        this.keySelector = keySelector;
        this.preencodeKeys = requiresPreencodedKeys(inputType, partitionKeys);
    }

    @Override
    public void open() throws Exception {
        super.open();
        lateRecordsDropped = getMetricGroup().counter("numLateRecordsDropped");
        lateRecordsDroppedRate = getMetricGroup().meter("lateRecordsDroppedRate", new MeterView(lateRecordsDropped));
        nullRowtimeDropped = getMetricGroup().counter("numNullRowTimeRecordsDropped");
        observedNativeStatistics = NativeSessionWindowTableFunctionBridge.statistics(nativeHandle());
        getMetricGroup()
                .gauge(
                        "pendingEventTimeTimers",
                        () -> NativeSessionWindowTableFunctionBridge.statistics(nativeHandle())[5]);
        getMetricGroup()
                .gauge(
                        "pendingProcessingTimeTimers",
                        () -> NativeSessionWindowTableFunctionBridge.statistics(nativeHandle())[6]);
        getMetricGroup().gauge("watermarkLatency", () -> {
            if (currentWatermark < 0) {
                return 0L;
            }
            return getProcessingTimeService().getCurrentProcessingTime() - currentWatermark;
        });
        if (!processingTime && restoredWatermark != Long.MIN_VALUE) {
            currentWatermark = restoredWatermark;
            emitTimerOutput(false, restoredWatermark);
        }
        scheduleNextProcessingTimer();
    }

    @Override
    public void processElement(StreamRecord<ArrowRowDataBatch> element) throws Exception {
        ArrowRowDataBatch input = element.getValue();
        try {
            List<byte[]> keys =
                    preencodeKeys ? preencodeKeys(input, keySelector, "session window table function") : null;
            try (ArrowRowDataBatch result = ArrowSessionWindowTableFunctionCDataBridge.process(
                    nativeHandle(),
                    input,
                    keys,
                    getProcessingTimeService().getCurrentProcessingTime(),
                    outputType,
                    allocator(),
                    memoryManager())) {
                emitBatch(result, true, input.size());
                recordProcessedWithoutStateCalls(input, result);
            }
            updateNativeMetrics();
            scheduleNextProcessingTimer();
        } catch (Throwable failure) {
            recordProcessingFailure();
            throw failure;
        }
    }

    @Override
    public void processWatermark(Watermark watermark) throws Exception {
        if (!processingTime && watermark.getTimestamp() > currentWatermark) {
            emitTimerOutput(false, watermark.getTimestamp());
            currentWatermark = watermark.getTimestamp();
            recordWatermark();
        }
        super.processWatermark(watermark);
    }

    @Override
    public void onProcessingTime(long timestamp) throws Exception {
        registeredProcessingTimer = Long.MAX_VALUE;
        emitTimerOutput(true, timestamp);
        scheduleNextProcessingTimer();
    }

    private void emitTimerOutput(boolean processing, long timestamp) throws Exception {
        try (ArrowRowDataBatch result = ArrowSessionWindowTableFunctionCDataBridge.advance(
                nativeHandle(), processing, timestamp, outputType, allocator(), memoryManager())) {
            emitBatch(result, false, 0);
            recordTimerOutput(result, processing);
        }
        updateNativeMetrics();
    }

    private void emitBatch(ArrowRowDataBatch result, boolean hasInput, int inputRows) {
        int physicalOutput = 0;
        if (result.size() > 0) {
            output.collect(new StreamRecord<>(result));
            physicalOutput = 1;
        }
        if (hasInput) {
            FlinkMetricParity.replacePhysicalRecords(
                    getMetricGroup().getIOMetricGroup().getNumRecordsInCounter(), 1, inputRows);
        }
        FlinkMetricParity.replacePhysicalRecords(
                getMetricGroup().getIOMetricGroup().getNumRecordsOutCounter(), physicalOutput, result.size());
    }

    private void scheduleNextProcessingTimer() {
        if (!processingTime) {
            return;
        }
        long next = NativeSessionWindowTableFunctionBridge.nextProcessingTimeTimer(nativeHandle());
        if (next != Long.MAX_VALUE && next != registeredProcessingTimer) {
            registeredProcessingTimer = next;
            getProcessingTimeService().registerTimer(next, this);
        }
    }

    private void updateNativeMetrics() {
        long currentLate = NativeSessionWindowTableFunctionBridge.lateRecordCount(nativeHandle());
        long lateDelta = currentLate - observedNativeLateRecords;
        if (lateDelta > 0) {
            lateRecordsDroppedRate.markEvent(lateDelta);
            observedNativeLateRecords = currentLate;
        }
        long currentNull = NativeSessionWindowTableFunctionBridge.nullRowtimeCount(nativeHandle());
        long nullDelta = currentNull - observedNativeNullRowtimes;
        if (nullDelta > 0) {
            nullRowtimeDropped.inc(nullDelta);
            observedNativeNullRowtimes = currentNull;
        }
        long[] current = NativeSessionWindowTableFunctionBridge.statistics(nativeHandle());
        if (current.length != 7 || observedNativeStatistics.length != 7) {
            throw new IllegalStateException("Native session Window TVF statistics have an incompatible shape");
        }
        recordNativeWindowStatistics(
                current[0] - observedNativeStatistics[0],
                current[1] - observedNativeStatistics[1],
                current[2] - observedNativeStatistics[2],
                current[3] - observedNativeStatistics[3],
                current[4] - observedNativeStatistics[4]);
        observedNativeStatistics = current;
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
}
