/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.join;

import java.util.Collections;
import java.util.List;
import org.apache.flink.api.common.operators.ProcessingTimeService.ProcessingTimeCallback;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.arrow.ArrowIntervalJoinCDataBridge;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.exchange.ArrowExchangeInputBatch;
import tech.streamfusion.flink.exchange.NativeExchangeFrame;
import tech.streamfusion.flink.metrics.FlinkMetricParity;
import tech.streamfusion.flink.state.AbstractStreamFusionArrowKeyedStateOperator;
import tech.streamfusion.nativebridge.NativeIntervalJoinBridge;

/** Arrow-native Flink interval join with native timestamp-ordered keyed state and timers. */
final class StreamFusionArrowIntervalJoinOperator extends AbstractStreamFusionArrowKeyedStateOperator
        implements TwoInputStreamOperator<NativeExchangeFrame, NativeExchangeFrame, ArrowRowDataBatch>,
                BoundedMultiInput,
                ProcessingTimeCallback {
    private final RowType[] inputTypes;
    private final RowType outputType;
    private final RowDataKeySelector[] keySelectors;
    private final boolean[] preencodeKeys;
    private final byte[][] exchangePlans;
    private final boolean eventTime;
    private final long maxOutputDelay;

    private transient long[] inputWatermarks = {Long.MIN_VALUE, Long.MIN_VALUE};
    private transient long emittedInputWatermark = Long.MIN_VALUE;
    private transient long registeredProcessingTimer = Long.MAX_VALUE;
    private transient long[] observedStatistics;
    private transient ListState<Long> leftWatermarkState;
    private transient ListState<Long> rightWatermarkState;

    StreamFusionArrowIntervalJoinOperator(
            RowType leftType,
            RowType rightType,
            RowType outputType,
            int[] leftKeys,
            int[] rightKeys,
            byte[] plan,
            RowDataKeySelector leftSelector,
            RowDataKeySelector rightSelector,
            byte[] leftExchangePlan,
            byte[] rightExchangePlan,
            boolean eventTime,
            long maxOutputDelay) {
        super(plan, "interval join", NativeIntervalJoinBridge.keyedStateBridge());
        this.inputTypes = new RowType[] {leftType, rightType};
        this.outputType = outputType;
        this.keySelectors = new RowDataKeySelector[] {leftSelector, rightSelector};
        this.preencodeKeys =
                new boolean[] {requiresPreencodedKeys(leftType, leftKeys), requiresPreencodedKeys(rightType, rightKeys)
                };
        this.exchangePlans = new byte[][] {leftExchangePlan.clone(), rightExchangePlan.clone()};
        this.eventTime = eventTime;
        this.maxOutputDelay = maxOutputDelay;
    }

    @Override
    public void open() throws Exception {
        super.open();
        observedStatistics = NativeIntervalJoinBridge.statistics(nativeHandle());
        org.apache.flink.metrics.MetricGroup diagnostics = getMetricGroup().addGroup("StreamFusion");
        diagnostics.gauge("pendingEventTimeTimers", () -> NativeIntervalJoinBridge.statistics(nativeHandle())[5]);
        diagnostics.gauge("pendingProcessingTimeTimers", () -> NativeIntervalJoinBridge.statistics(nativeHandle())[6]);
        if (eventTime) {
            maybeAdvanceWatermark();
        }
        scheduleNextProcessingTimer();
    }

    @Override
    public void processElement1(StreamRecord<NativeExchangeFrame> element) throws Exception {
        processFrame(0, element.getValue());
    }

    @Override
    public void processElement2(StreamRecord<NativeExchangeFrame> element) throws Exception {
        processFrame(1, element.getValue());
    }

    private void processFrame(int side, NativeExchangeFrame frame) throws Exception {
        try (ArrowExchangeInputBatch input = tech.streamfusion.flink.arrow.ArrowExchangeInputCDataBridge.decode(
                exchangePlans[side], frame, inputTypes[side], allocator(), memoryManager())) {
            try {
                List<byte[]> keys = preencodeKeys[side]
                        ? preencodeKeys(input.arrowBatch(), keySelectors[side], "interval join")
                        : null;
                try (ArrowRowDataBatch result = ArrowIntervalJoinCDataBridge.process(
                        nativeHandle(),
                        side,
                        getProcessingTimeService().getCurrentProcessingTime(),
                        input,
                        keys,
                        outputType,
                        allocator(),
                        memoryManager())) {
                    emitBatch(result, true, input.size());
                    recordProcessedWithoutStateCalls(input.size(), result);
                }
                updateNativeStatistics();
                scheduleNextProcessingTimer();
            } catch (Throwable failure) {
                recordProcessingFailure();
                throw failure;
            }
        }
    }

    @Override
    public void processWatermark1(Watermark watermark) throws Exception {
        inputWatermarks[0] = Math.max(inputWatermarks[0], watermark.getTimestamp());
        maybeAdvanceWatermark();
    }

    @Override
    public void processWatermark2(Watermark watermark) throws Exception {
        inputWatermarks[1] = Math.max(inputWatermarks[1], watermark.getTimestamp());
        maybeAdvanceWatermark();
    }

    private void maybeAdvanceWatermark() throws Exception {
        long watermark = Math.min(inputWatermarks[0], inputWatermarks[1]);
        if (watermark <= emittedInputWatermark) {
            return;
        }
        if (eventTime) {
            emitTimerOutput(false, watermark);
        }
        emittedInputWatermark = watermark;
        recordWatermark();
        output.emitWatermark(new Watermark(eventTime ? delayedWatermark(watermark) : watermark));
    }

    private long delayedWatermark(long watermark) {
        // Match AbstractStreamOperator.DelayedOutputAdjustment, including Java long wrapping.
        return watermark - maxOutputDelay;
    }

    @Override
    public void onProcessingTime(long timestamp) throws Exception {
        registeredProcessingTimer = Long.MAX_VALUE;
        emitTimerOutput(true, timestamp);
        scheduleNextProcessingTimer();
    }

    private void emitTimerOutput(boolean processingTime, long timestamp) throws Exception {
        try (ArrowRowDataBatch result = ArrowIntervalJoinCDataBridge.advance(
                nativeHandle(), processingTime, timestamp, outputType, allocator(), memoryManager())) {
            emitBatch(result, false, 0);
            recordTimerOutput(result, processingTime);
        }
        updateNativeStatistics();
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
        if (eventTime) {
            return;
        }
        long next = NativeIntervalJoinBridge.nextProcessingTimeTimer(nativeHandle());
        if (next != Long.MAX_VALUE && next != registeredProcessingTimer) {
            registeredProcessingTimer = next;
            getProcessingTimeService().registerTimer(next, this);
        }
    }

    private void updateNativeStatistics() {
        long[] current = NativeIntervalJoinBridge.statistics(nativeHandle());
        if (current.length != 7 || observedStatistics.length != 7) {
            throw new IllegalStateException("Native interval join statistics have an incompatible shape");
        }
        recordNativeWindowStatistics(
                current[0] - observedStatistics[0],
                current[1] - observedStatistics[1],
                current[2] - observedStatistics[2],
                current[3] - observedStatistics[3],
                current[4] - observedStatistics[4]);
        observedStatistics = current;
    }

    @Override
    public void endInput(int inputId) {}

    @Override
    protected void afterNativeStateInitialized(StateInitializationContext context) throws Exception {
        inputWatermarks = new long[] {Long.MIN_VALUE, Long.MIN_VALUE};
        emittedInputWatermark = Long.MIN_VALUE;
        leftWatermarkState = context.getOperatorStateStore()
                .getUnionListState(new ListStateDescriptor<>("left-watermark", LongSerializer.INSTANCE));
        rightWatermarkState = context.getOperatorStateStore()
                .getUnionListState(new ListStateDescriptor<>("right-watermark", LongSerializer.INSTANCE));
        if (context.isRestored()) {
            inputWatermarks[0] = restoredWatermark(leftWatermarkState);
            inputWatermarks[1] = restoredWatermark(rightWatermarkState);
        }
    }

    @Override
    protected void beforeNativeStateSnapshot(StateSnapshotContext context) throws Exception {
        leftWatermarkState.update(Collections.singletonList(inputWatermarks[0]));
        rightWatermarkState.update(Collections.singletonList(inputWatermarks[1]));
    }

    private static long restoredWatermark(ListState<Long> state) throws Exception {
        long restored = Long.MIN_VALUE;
        for (Long candidate : state.get()) {
            restored = Math.max(restored, candidate);
        }
        return restored;
    }
}
