/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.join;

import java.util.Collections;
import java.util.List;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.api.operators.OperatorAttributes;
import org.apache.flink.streaming.api.operators.OperatorAttributesBuilder;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.arrow.ArrowRegularJoinCDataBridge;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.exchange.ArrowExchangeInputBatch;
import tech.streamfusion.flink.exchange.NativeExchangeFrame;
import tech.streamfusion.flink.metrics.FlinkMetricParity;
import tech.streamfusion.flink.state.AbstractStreamFusionArrowKeyedStateOperator;
import tech.streamfusion.nativebridge.NativeCalcBridge;
import tech.streamfusion.nativebridge.NativeRegularJoinBridge;

/** Native two-input regular streaming join over ordered Arrow-row multiset state. */
final class StreamFusionArrowRegularJoinOperator extends AbstractStreamFusionArrowKeyedStateOperator
        implements TwoInputStreamOperator<NativeExchangeFrame, NativeExchangeFrame, ArrowRowDataBatch>,
                BoundedMultiInput {
    private final RowType[] inputTypes;
    private final RowType outputType;
    private final RowDataKeySelector[] keySelectors;
    private final boolean[] preencodeKeys;
    private final byte[][] exchangePlans;
    private final boolean boundedFinalOutput;

    private transient long[] inputWatermarks = {Long.MIN_VALUE, Long.MIN_VALUE};
    private transient long emittedWatermark = Long.MIN_VALUE;
    private transient long[] observedStatistics;
    private transient ListState<Long> leftWatermarkState;
    private transient ListState<Long> rightWatermarkState;
    private transient boolean[] inputEnded;
    private transient boolean finished;

    StreamFusionArrowRegularJoinOperator(
            RowType leftType,
            RowType rightType,
            RowType outputType,
            int[] leftKeys,
            int[] rightKeys,
            byte[] plan,
            RowDataKeySelector leftSelector,
            RowDataKeySelector rightSelector,
            byte[] leftExchangePlan,
            byte[] rightExchangePlan) {
        this(
                leftType,
                rightType,
                outputType,
                leftKeys,
                rightKeys,
                plan,
                leftSelector,
                rightSelector,
                leftExchangePlan,
                rightExchangePlan,
                false);
    }

    StreamFusionArrowRegularJoinOperator(
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
            boolean boundedFinalOutput) {
        super(plan, "regular join", NativeRegularJoinBridge.keyedStateBridge());
        this.inputTypes = new RowType[] {leftType, rightType};
        this.outputType = outputType;
        this.keySelectors = new RowDataKeySelector[] {leftSelector, rightSelector};
        this.preencodeKeys =
                new boolean[] {requiresPreencodedKeys(leftType, leftKeys), requiresPreencodedKeys(rightType, rightKeys)
                };
        this.exchangePlans = new byte[][] {leftExchangePlan.clone(), rightExchangePlan.clone()};
        this.boundedFinalOutput = boundedFinalOutput;
    }

    @Override
    public void open() throws Exception {
        super.open();
        observedStatistics = NativeRegularJoinBridge.statistics(nativeHandle());
        getMetricGroup().addGroup("StreamFusion").gauge("pendingEventTimeTimers", () -> 0L);
        getMetricGroup().addGroup("StreamFusion").gauge("pendingProcessingTimeTimers", () -> 0L);
        if (boundedFinalOutput) {
            getMetricGroup().gauge("memoryUsedSizeInBytes", this::managedMemoryUsed);
            getMetricGroup().gauge("numSpillFiles", () -> 0L);
            getMetricGroup().gauge("spillInBytes", () -> 0L);
        }
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
        if (boundedFinalOutput) {
            try {
                long inputRows = frame.processBoundedRegularJoinNative(nativeHandle(), side, exchangePlans[side]);
                FlinkMetricParity.replacePhysicalRecords(
                        getMetricGroup().getIOMetricGroup().getNumRecordsInCounter(), 1, inputRows);
                recordProcessedWithoutStateCalls(inputRows);
                updateNativeStatistics();
                return;
            } catch (Throwable failure) {
                recordProcessingFailure();
                throw failure;
            }
        }
        try (ArrowExchangeInputBatch input = tech.streamfusion.flink.arrow.ArrowExchangeInputCDataBridge.decode(
                exchangePlans[side], frame, inputTypes[side], allocator(), memoryManager())) {
            try {
                List<byte[]> keys = preencodeKeys[side]
                        ? preencodeKeys(input.arrowBatch(), keySelectors[side], "regular join")
                        : null;
                try (ArrowRowDataBatch result = ArrowRegularJoinCDataBridge.execute(
                        nativeHandle(), side, input, keys, outputType, allocator(), memoryManager())) {
                    int physicalOutput = 0;
                    if (result.size() > 0) {
                        output.collect(new StreamRecord<>(result));
                        physicalOutput = 1;
                    }
                    FlinkMetricParity.replacePhysicalRecords(
                            getMetricGroup().getIOMetricGroup().getNumRecordsInCounter(), 1, input.size());
                    FlinkMetricParity.replacePhysicalRecords(
                            getMetricGroup().getIOMetricGroup().getNumRecordsOutCounter(),
                            physicalOutput,
                            result.size());
                    recordProcessedWithoutStateCalls(input.size(), result);
                }
                updateNativeStatistics();
            } catch (Throwable failure) {
                recordProcessingFailure();
                throw failure;
            }
        }
    }

    private void updateNativeStatistics() {
        long[] current = NativeRegularJoinBridge.statistics(nativeHandle());
        if (current.length != 3 || observedStatistics.length != 3) {
            throw new IllegalStateException("Native regular join statistics have an incompatible shape");
        }
        recordNativeWindowStatistics(current[0] - observedStatistics[0], current[1] - observedStatistics[1], 0, 0, 0);
        NativeCalcBridge.recordFusedBatches(current[2] - observedStatistics[2]);
        observedStatistics = current;
    }

    @Override
    public void processWatermark1(Watermark watermark) {
        inputWatermarks[0] = Math.max(inputWatermarks[0], watermark.getTimestamp());
        emitCombinedWatermark();
    }

    @Override
    public void processWatermark2(Watermark watermark) {
        inputWatermarks[1] = Math.max(inputWatermarks[1], watermark.getTimestamp());
        emitCombinedWatermark();
    }

    private void emitCombinedWatermark() {
        long watermark = Math.min(inputWatermarks[0], inputWatermarks[1]);
        if (watermark > emittedWatermark) {
            emittedWatermark = watermark;
            recordWatermark();
            output.emitWatermark(new Watermark(watermark));
        }
    }

    @Override
    public void endInput(int inputId) throws Exception {
        if (!boundedFinalOutput || finished) {
            return;
        }
        if (inputId < 1 || inputId > 2) {
            throw new IllegalArgumentException("Regular join input id must be 1 or 2");
        }
        inputEnded[inputId - 1] = true;
        if (!inputEnded[0] || !inputEnded[1]) {
            return;
        }
        finished = true;
        try {
            while (true) {
                try (ArrowRowDataBatch result =
                        ArrowRegularJoinCDataBridge.finish(nativeHandle(), outputType, allocator(), memoryManager())) {
                    if (result.size() == 0) {
                        break;
                    }
                    output.collect(new StreamRecord<>(result));
                    FlinkMetricParity.replacePhysicalRecords(
                            getMetricGroup().getIOMetricGroup().getNumRecordsOutCounter(), 1, result.size());
                    recordProcessedWithoutStateCalls(0, result);
                }
            }
            updateNativeStatistics();
        } catch (Throwable failure) {
            recordProcessingFailure();
            throw failure;
        }
    }

    @Override
    protected void afterNativeStateInitialized(StateInitializationContext context) throws Exception {
        // Transient initializers do not run on Flink's task-side deserialized operator.
        inputWatermarks = new long[] {Long.MIN_VALUE, Long.MIN_VALUE};
        inputEnded = new boolean[2];
        finished = false;
        emittedWatermark = Long.MIN_VALUE;
        leftWatermarkState = context.getOperatorStateStore()
                .getUnionListState(new ListStateDescriptor<>("left-watermark", LongSerializer.INSTANCE));
        rightWatermarkState = context.getOperatorStateStore()
                .getUnionListState(new ListStateDescriptor<>("right-watermark", LongSerializer.INSTANCE));
        if (context.isRestored()) {
            inputWatermarks[0] = restoredWatermark(leftWatermarkState);
            inputWatermarks[1] = restoredWatermark(rightWatermarkState);
            emittedWatermark = Math.min(inputWatermarks[0], inputWatermarks[1]);
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

    @Override
    public OperatorAttributes getOperatorAttributes() {
        if (!boundedFinalOutput) {
            return super.getOperatorAttributes();
        }
        return new OperatorAttributesBuilder()
                .setOutputOnlyAfterEndOfStream(true)
                .setInternalSorterSupported(true)
                .build();
    }
}
