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
import tech.streamfusion.flink.arrow.ArrowWindowAggregateCDataBridge;
import tech.streamfusion.flink.metrics.FlinkMetricParity;
import tech.streamfusion.flink.state.AbstractStreamFusionArrowKeyedStateOperator;
import tech.streamfusion.nativebridge.NativeWindowAggregateBridge;

/** Key-grouped native window aggregation with batch-native state access and timer firing. */
final class StreamFusionArrowWindowAggregateOperator extends AbstractStreamFusionArrowKeyedStateOperator
        implements OneInputStreamOperator<ArrowRowDataBatch, ArrowRowDataBatch>,
                BoundedOneInput,
                ProcessingTimeCallback {
    private final RowType inputType;
    private final RowType outputType;
    private final int[] grouping;
    private final boolean inputChangelog;
    private final boolean processingTime;
    private final RowDataKeySelector keySelector;
    private final boolean preencodeKeys;

    private transient ListState<Long> watermarkState;
    private transient long restoredWatermark = Long.MIN_VALUE;
    private transient long currentWatermark = Long.MIN_VALUE;
    private transient long registeredProcessingTimer = Long.MAX_VALUE;
    private transient Counter lateRecordsDropped;
    private transient Meter lateRecordsDroppedRate;
    private transient long observedNativeLateRecords;
    private transient long[] observedNativeStatistics;

    StreamFusionArrowWindowAggregateOperator(
            RowType inputType,
            RowType outputType,
            int[] grouping,
            byte[] plan,
            boolean inputChangelog,
            boolean processingTime,
            RowDataKeySelector keySelector) {
        super(plan, "window aggregate", NativeWindowAggregateBridge.keyedStateBridge());
        this.inputType = inputType;
        this.outputType = outputType;
        this.grouping = grouping.clone();
        this.inputChangelog = inputChangelog;
        this.processingTime = processingTime;
        this.keySelector = keySelector;
        this.preencodeKeys = requiresPreencodedKeys(inputType, grouping);
    }

    @Override
    public void open() throws Exception {
        super.open();
        lateRecordsDropped = getMetricGroup().counter("numLateRecordsDropped");
        lateRecordsDroppedRate = getMetricGroup().meter("lateRecordsDroppedRate", new MeterView(lateRecordsDropped));
        observedNativeStatistics = NativeWindowAggregateBridge.statistics(nativeHandle());
        getMetricGroup()
                .gauge("pendingEventTimeTimers", () -> NativeWindowAggregateBridge.statistics(nativeHandle())[5]);
        getMetricGroup()
                .gauge("pendingProcessingTimeTimers", () -> NativeWindowAggregateBridge.statistics(nativeHandle())[6]);
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
            if (!inputChangelog) {
                for (int row = 0; row < input.size(); row++) {
                    if (input.rowKind(row) != org.apache.flink.types.RowKind.INSERT) {
                        throw new IllegalStateException(
                                "Native append-only window aggregate got " + input.rowKind(row));
                    }
                }
            }
            List<byte[]> keys = preencodeKeys ? preencodeKeys(input, keySelector, "window aggregate") : null;
            try (ArrowRowDataBatch result = ArrowWindowAggregateCDataBridge.process(
                    nativeHandle(),
                    input,
                    keys,
                    inputChangelog,
                    getProcessingTimeService().getCurrentProcessingTime(),
                    outputType,
                    allocator(),
                    memoryManager())) {
                emitBatch(result, true, input.size());
                recordProcessedWithoutStateCalls(input, result);
            }
            updateNativeStatistics();
            updateLateMetric();
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
        try (ArrowRowDataBatch result = ArrowWindowAggregateCDataBridge.advance(
                nativeHandle(), processing, timestamp, outputType, allocator(), memoryManager())) {
            emitBatch(result, false, 0);
            recordTimerOutput(result, processing);
        }
        updateLateMetric();
        updateNativeStatistics();
    }

    private void emitBatch(ArrowRowDataBatch result, boolean hasInputBatch, int inputRows) {
        int physicalOutput = 0;
        if (result.size() > 0) {
            output.collect(new StreamRecord<>(result));
            physicalOutput = 1;
        }
        if (hasInputBatch) {
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
        long next = NativeWindowAggregateBridge.nextProcessingTimeTimer(nativeHandle());
        if (next != Long.MAX_VALUE && next != registeredProcessingTimer) {
            registeredProcessingTimer = next;
            getProcessingTimeService().registerTimer(next, this);
        }
    }

    private void updateLateMetric() {
        long nativeCount = NativeWindowAggregateBridge.lateRecordCount(nativeHandle());
        long delta = nativeCount - observedNativeLateRecords;
        if (delta > 0) {
            lateRecordsDroppedRate.markEvent(delta);
            observedNativeLateRecords = nativeCount;
        }
    }

    private void updateNativeStatistics() {
        long[] current = NativeWindowAggregateBridge.statistics(nativeHandle());
        if (current.length != 7 || observedNativeStatistics.length != 7) {
            throw new IllegalStateException("Native window statistics have an incompatible shape");
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
