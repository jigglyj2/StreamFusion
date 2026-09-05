/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.join;

import java.util.Collections;
import java.util.List;
import org.apache.flink.api.common.functions.DefaultOpenContext;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.operators.ProcessingTimeService.ProcessingTimeCallback;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.generated.JoinCondition;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowTemporalJoinCDataBridge;
import tech.streamfusion.flink.exchange.ArrowExchangeInputBatch;
import tech.streamfusion.flink.exchange.NativeExchangeFrame;
import tech.streamfusion.flink.metrics.FlinkMetricParity;
import tech.streamfusion.flink.state.AbstractStreamFusionArrowKeyedStateOperator;
import tech.streamfusion.nativebridge.NativeTemporalJoinBridge;

/** Arrow-native Flink temporal join over versioned keyed state. */
final class StreamFusionArrowTemporalJoinOperator extends AbstractStreamFusionArrowKeyedStateOperator
        implements TwoInputStreamOperator<NativeExchangeFrame, NativeExchangeFrame, ArrowRowDataBatch>,
                BoundedMultiInput,
                ProcessingTimeCallback {
    private final RowType[] inputTypes;
    private final RowType outputType;
    private final RowDataKeySelector[] keySelectors;
    private final boolean[] preencodeKeys;
    private final byte[][] exchangePlans;
    private final boolean processingTime;
    private final FlinkJoinType joinType;
    private final GeneratedJoinCondition generatedCondition;

    private transient long[] inputWatermarks = {Long.MIN_VALUE, Long.MIN_VALUE};
    private transient long emittedWatermark = Long.MIN_VALUE;
    private transient long registeredProcessingTimer = Long.MAX_VALUE;
    private transient long[] observedStatistics;
    private transient ListState<Long> leftWatermarkState;
    private transient ListState<Long> rightWatermarkState;
    private transient JoinCondition condition;
    private transient Counter conditionEvaluations;

    StreamFusionArrowTemporalJoinOperator(
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
            boolean processingTime,
            FlinkJoinType joinType,
            GeneratedJoinCondition generatedCondition) {
        super(plan, "temporal join", NativeTemporalJoinBridge.keyedStateBridge());
        this.inputTypes = new RowType[] {leftType, rightType};
        this.outputType = outputType;
        this.keySelectors = new RowDataKeySelector[] {leftSelector, rightSelector};
        this.preencodeKeys =
                new boolean[] {requiresPreencodedKeys(leftType, leftKeys), requiresPreencodedKeys(rightType, rightKeys)
                };
        this.exchangePlans = new byte[][] {leftExchangePlan.clone(), rightExchangePlan.clone()};
        this.processingTime = processingTime;
        this.joinType = joinType;
        this.generatedCondition = generatedCondition;
    }

    @Override
    public void open() throws Exception {
        super.open();
        if (generatedCondition != null) {
            condition = generatedCondition.newInstance(getRuntimeContext().getUserCodeClassLoader());
            FunctionUtils.setFunctionRuntimeContext(condition, getRuntimeContext());
            FunctionUtils.openFunction(condition, DefaultOpenContext.INSTANCE);
        }
        observedStatistics = NativeTemporalJoinBridge.statistics(nativeHandle());
        org.apache.flink.metrics.MetricGroup diagnostics = getMetricGroup().addGroup("StreamFusion");
        conditionEvaluations = diagnostics.counter("joinConditionEvaluations");
        diagnostics.gauge("pendingEventTimeTimers", () -> NativeTemporalJoinBridge.statistics(nativeHandle())[5]);
        diagnostics.gauge("pendingProcessingTimeTimers", () -> NativeTemporalJoinBridge.statistics(nativeHandle())[6]);
        if (!processingTime) {
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
                        ? preencodeKeys(input.arrowBatch(), keySelectors[side], "temporal join")
                        : null;
                try (ArrowTemporalJoinCDataBridge.Result result = ArrowTemporalJoinCDataBridge.process(
                        nativeHandle(),
                        side,
                        getProcessingTimeService().getCurrentProcessingTime(),
                        input,
                        keys,
                        inputTypes[0],
                        inputTypes[1],
                        outputType,
                        joinType,
                        condition,
                        allocator(),
                        memoryManager())) {
                    emitBatch(result.output(), true, input.size());
                    recordProcessedWithoutStateCalls(input.size(), result.output());
                    conditionEvaluations.inc(result.conditionEvaluations());
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
        long watermark = processingTime ? inputWatermarks[0] : Math.min(inputWatermarks[0], inputWatermarks[1]);
        if (watermark <= emittedWatermark) {
            return;
        }
        if (!processingTime) {
            emitTimerOutput(false, watermark);
        }
        emittedWatermark = watermark;
        recordWatermark();
        output.emitWatermark(new Watermark(watermark));
    }

    @Override
    public void onProcessingTime(long timestamp) throws Exception {
        registeredProcessingTimer = Long.MAX_VALUE;
        emitTimerOutput(true, timestamp);
        scheduleNextProcessingTimer();
    }

    private void emitTimerOutput(boolean processingTimer, long timestamp) throws Exception {
        try (ArrowTemporalJoinCDataBridge.Result result = ArrowTemporalJoinCDataBridge.advance(
                nativeHandle(),
                processingTimer,
                timestamp,
                inputTypes[0],
                inputTypes[1],
                outputType,
                joinType,
                condition,
                allocator(),
                memoryManager())) {
            emitBatch(result.output(), false, 0);
            recordTimerOutput(result.output(), processingTimer);
            conditionEvaluations.inc(result.conditionEvaluations());
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
        long next = NativeTemporalJoinBridge.nextProcessingTimeTimer(nativeHandle());
        if (next != Long.MAX_VALUE && next != registeredProcessingTimer) {
            registeredProcessingTimer = next;
            getProcessingTimeService().registerTimer(next, this);
        }
    }

    private void updateNativeStatistics() {
        long[] current = NativeTemporalJoinBridge.statistics(nativeHandle());
        if (current.length != 7 || observedStatistics.length != 7) {
            throw new IllegalStateException("Native temporal join statistics have an incompatible shape");
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
        emittedWatermark = Long.MIN_VALUE;
        leftWatermarkState = context.getOperatorStateStore()
                .getUnionListState(new ListStateDescriptor<>("left-watermark", LongSerializer.INSTANCE));
        rightWatermarkState = context.getOperatorStateStore()
                .getUnionListState(new ListStateDescriptor<>("right-watermark", LongSerializer.INSTANCE));
        if (context.isRestored()) {
            inputWatermarks[0] = restoredWatermark(leftWatermarkState);
            inputWatermarks[1] = restoredWatermark(rightWatermarkState);
            emittedWatermark = processingTime ? inputWatermarks[0] : Math.min(inputWatermarks[0], inputWatermarks[1]);
        }
    }

    @Override
    protected void beforeNativeStateSnapshot(StateSnapshotContext context) throws Exception {
        leftWatermarkState.update(Collections.singletonList(inputWatermarks[0]));
        rightWatermarkState.update(Collections.singletonList(inputWatermarks[1]));
    }

    @Override
    protected void beforeNativeClose() throws Exception {
        if (condition != null) {
            FunctionUtils.closeFunction(condition);
            condition = null;
        }
    }

    private static long restoredWatermark(ListState<Long> state) throws Exception {
        long restored = Long.MIN_VALUE;
        for (Long candidate : state.get()) {
            restored = Math.max(restored, candidate);
        }
        return restored;
    }
}
