/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.over;

import java.util.Collections;
import java.util.List;
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
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.arrow.ArrowOverAggregateCDataBridge;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.metrics.FlinkMetricParity;
import tech.streamfusion.flink.state.AbstractStreamFusionArrowKeyedStateOperator;
import tech.streamfusion.nativebridge.NativeOverAggregateBridge;

/** Ordered native OVER aggregation with canonical raw keyed state. */
final class StreamFusionArrowOverAggregateOperator extends AbstractStreamFusionArrowKeyedStateOperator
        implements OneInputStreamOperator<ArrowRowDataBatch, ArrowRowDataBatch>,
                BoundedOneInput,
                ProcessingTimeCallback {
    private final RowType outputType;
    private final boolean inputChangelog;
    private final boolean missingRowMetrics;
    private final boolean eventTime;
    private final boolean preencodeKeys;
    private final RowDataKeySelector keySelector;

    private transient Counter idsNotFound;
    private transient Counter sortKeysNotFound;
    private transient long[] observedStatistics;
    private transient Counter lateRecordsDropped;
    private transient ListState<Long> watermarkState;
    private transient long restoredWatermark = Long.MIN_VALUE;
    private transient long currentWatermark = Long.MIN_VALUE;
    private transient long registeredProcessingTimer = Long.MAX_VALUE;

    StreamFusionArrowOverAggregateOperator(
            RowType inputType,
            RowType outputType,
            int[] partitionKeys,
            byte[] plan,
            boolean inputChangelog,
            boolean missingRowMetrics,
            boolean eventTime,
            RowDataKeySelector keySelector) {
        super(plan, "over aggregate", NativeOverAggregateBridge.keyedStateBridge());
        this.outputType = outputType;
        this.inputChangelog = inputChangelog;
        this.missingRowMetrics = missingRowMetrics;
        this.eventTime = eventTime;
        this.preencodeKeys = requiresPreencodedKeys(inputType, partitionKeys);
        this.keySelector = keySelector;
    }

    @Override
    public void open() throws Exception {
        super.open();
        if (missingRowMetrics) {
            idsNotFound = getMetricGroup().counter("numOfIdsNotFound");
            sortKeysNotFound = getMetricGroup().counter("numOfSortKeysNotFound");
        }
        observedStatistics = NativeOverAggregateBridge.statistics(nativeHandle());
        MetricGroup diagnostics = getMetricGroup().addGroup("StreamFusion");
        diagnostics.gauge("pendingEventTimeTimers", () -> NativeOverAggregateBridge.statistics(nativeHandle())[7]);
        diagnostics.gauge("pendingProcessingTimeTimers", () -> NativeOverAggregateBridge.statistics(nativeHandle())[8]);
        if (eventTime) {
            lateRecordsDropped = getMetricGroup().counter("numLateRecordsDropped");
            if (restoredWatermark != Long.MIN_VALUE) {
                currentWatermark = restoredWatermark;
                emitTimerOutput(false, restoredWatermark);
            }
        }
        scheduleNextProcessingTimer();
    }

    @Override
    public void processWatermark(Watermark watermark) throws Exception {
        if (eventTime && watermark.getTimestamp() > currentWatermark) {
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

    private void emitTimerOutput(boolean processingTime, long timestamp) throws Exception {
        boolean more;
        do {
            try (ArrowRowDataBatch result = processingTime
                    ? ArrowOverAggregateCDataBridge.advanceProcessingTime(
                            nativeHandle(), timestamp, outputType, allocator(), memoryManager())
                    : ArrowOverAggregateCDataBridge.advanceEventTime(
                            nativeHandle(), timestamp, outputType, allocator(), memoryManager())) {
                int physicalOutputs = 0;
                if (result.size() > 0) {
                    output.collect(new StreamRecord<>(result));
                    physicalOutputs = 1;
                }
                FlinkMetricParity.replacePhysicalRecords(
                        getMetricGroup().getIOMetricGroup().getNumRecordsOutCounter(), physicalOutputs, result.size());
                recordTimerOutput(result, processingTime);
            }
            long next = processingTime
                    ? NativeOverAggregateBridge.nextProcessingTimeTimer(nativeHandle())
                    : NativeOverAggregateBridge.nextEventTimeTimer(nativeHandle());
            more = next != Long.MAX_VALUE && next <= timestamp;
        } while (more);
        updateStatistics();
    }

    private void scheduleNextProcessingTimer() {
        long next = NativeOverAggregateBridge.nextProcessingTimeTimer(nativeHandle());
        if (next != Long.MAX_VALUE && next != registeredProcessingTimer) {
            registeredProcessingTimer = next;
            getProcessingTimeService().registerTimer(next, this);
        }
    }

    @Override
    public void processElement(StreamRecord<ArrowRowDataBatch> element) throws Exception {
        ArrowRowDataBatch input = element.getValue();
        try {
            List<byte[]> keys = preencodeKeys ? preencodeKeys(input, keySelector, "over aggregate") : null;
            try (ArrowRowDataBatch result = ArrowOverAggregateCDataBridge.process(
                    nativeHandle(),
                    input,
                    keys,
                    inputChangelog,
                    getProcessingTimeService().getCurrentProcessingTime(),
                    outputType,
                    allocator(),
                    memoryManager())) {
                int physicalOutputs = 0;
                if (result.size() > 0) {
                    output.collect(new StreamRecord<>(result));
                    physicalOutputs = 1;
                }
                FlinkMetricParity.replacePhysicalRecords(
                        getMetricGroup().getIOMetricGroup().getNumRecordsInCounter(), 1, input.size());
                FlinkMetricParity.replacePhysicalRecords(
                        getMetricGroup().getIOMetricGroup().getNumRecordsOutCounter(), physicalOutputs, result.size());
                recordProcessedWithoutStateCalls(input, result);
            }
            updateStatistics();
            scheduleNextProcessingTimer();
        } catch (Throwable failure) {
            recordProcessingFailure();
            throw failure;
        }
    }

    private void updateStatistics() {
        long[] current = NativeOverAggregateBridge.statistics(nativeHandle());
        if (current.length != 10 || observedStatistics.length != 10) {
            throw new IllegalStateException("Native OVER statistics have an incompatible shape");
        }
        recordNativeWindowStatistics(
                current[0] - observedStatistics[0],
                current[1] - observedStatistics[1],
                current[4] - observedStatistics[4],
                current[5] - observedStatistics[5],
                current[6] - observedStatistics[6]);
        if (missingRowMetrics) {
            idsNotFound.inc(current[2] - observedStatistics[2]);
            sortKeysNotFound.inc(current[3] - observedStatistics[3]);
        }
        if (eventTime) {
            lateRecordsDropped.inc(current[9] - observedStatistics[9]);
        }
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
}
