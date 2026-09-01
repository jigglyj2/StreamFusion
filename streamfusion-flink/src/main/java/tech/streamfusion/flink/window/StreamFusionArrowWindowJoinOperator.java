/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.window;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.flink.api.common.functions.DefaultOpenContext;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.binary.BinarySegmentUtils;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.generated.JoinCondition;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowWindowJoinCDataBridge;
import tech.streamfusion.flink.exchange.ArrowExchangeInputBatch;
import tech.streamfusion.flink.exchange.NativeExchangeFrame;
import tech.streamfusion.flink.metrics.FlinkMetricParity;
import tech.streamfusion.flink.state.AbstractStreamFusionArrowKeyedStateOperator;
import tech.streamfusion.nativebridge.NativeMemoryManager;
import tech.streamfusion.nativebridge.NativeWindowJoinBridge;

/** Native two-input Window Join state with batched Flink-condition result materialization. */
final class StreamFusionArrowWindowJoinOperator extends AbstractStreamFusionArrowKeyedStateOperator
        implements TwoInputStreamOperator<NativeExchangeFrame, NativeExchangeFrame, ArrowRowDataBatch>,
                BoundedMultiInput {
    private final RowType leftType;
    private final RowType rightType;
    private final RowType outputType;
    private final int[][] keyIndices;
    private final RowDataKeySelector[] keySelectors;
    private final boolean[] preencodeKeys;
    private final FlinkJoinType joinType;
    private final GeneratedJoinCondition generatedCondition;
    private final int[] nullFilterKeys;
    private final byte[][] exchangePlans;

    private transient RowDataSerializer[] serializers;
    private transient JoinCondition condition;
    private transient ListState<Long> leftWatermarkState;
    private transient ListState<Long> rightWatermarkState;
    private transient long[] inputWatermarks = {Long.MIN_VALUE, Long.MIN_VALUE};
    private transient long emittedWatermark = Long.MIN_VALUE;
    private transient Counter[] lateRecordsDropped;
    private transient Meter[] lateRecordsDroppedRates;
    private transient long[] observedNativeLateRecords = {0, 0};
    private transient long[] observedNativeStatistics;
    private transient Counter conditionEvaluations;

    StreamFusionArrowWindowJoinOperator(
            RowType leftType,
            RowType rightType,
            RowType outputType,
            int[] leftKeys,
            int[] rightKeys,
            byte[] plan,
            RowDataKeySelector leftSelector,
            RowDataKeySelector rightSelector,
            FlinkJoinType joinType,
            GeneratedJoinCondition generatedCondition,
            boolean[] filterNulls,
            byte[] leftExchangePlan,
            byte[] rightExchangePlan) {
        super(plan, "window join");
        this.leftType = leftType;
        this.rightType = rightType;
        this.outputType = outputType;
        this.keyIndices = new int[][] {leftKeys.clone(), rightKeys.clone()};
        this.keySelectors = new RowDataKeySelector[] {leftSelector, rightSelector};
        this.preencodeKeys =
                new boolean[] {requiresPreencodedKeys(leftType, leftKeys), requiresPreencodedKeys(rightType, rightKeys)
                };
        this.joinType = joinType;
        this.generatedCondition = generatedCondition;
        this.nullFilterKeys = filteredKeyIndices(leftKeys, filterNulls);
        this.exchangePlans = new byte[][] {leftExchangePlan.clone(), rightExchangePlan.clone()};
    }

    @Override
    public void open() throws Exception {
        super.open();
        serializers = new RowDataSerializer[] {new RowDataSerializer(leftType), new RowDataSerializer(rightType)};
        condition = generatedCondition.newInstance(getRuntimeContext().getUserCodeClassLoader());
        FunctionUtils.setFunctionRuntimeContext(condition, getRuntimeContext());
        FunctionUtils.openFunction(condition, DefaultOpenContext.INSTANCE);
        lateRecordsDropped = new Counter[] {
            getMetricGroup().counter("leftNumLateRecordsDropped"),
            getMetricGroup().counter("rightNumLateRecordsDropped")
        };
        lateRecordsDroppedRates = new Meter[] {
            getMetricGroup().meter("leftLateRecordsDroppedRate", new MeterView(lateRecordsDropped[0])),
            getMetricGroup().meter("rightLateRecordsDroppedRate", new MeterView(lateRecordsDropped[1]))
        };
        conditionEvaluations = getMetricGroup().counter("joinConditionEvaluations");
        observedNativeStatistics = NativeWindowJoinBridge.statistics(nativeHandle());
        getMetricGroup().gauge("pendingEventTimeTimers", () -> NativeWindowJoinBridge.statistics(nativeHandle())[5]);
        getMetricGroup().gauge("pendingProcessingTimeTimers", () -> 0L);
        getMetricGroup().gauge("watermarkLatency", () -> {
            if (emittedWatermark < 0) {
                return 0L;
            }
            return getProcessingTimeService().getCurrentProcessingTime() - emittedWatermark;
        });
        maybeAdvanceWatermark();
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
                exchangePlans[side], frame, side == 0 ? leftType : rightType, allocator(), memoryManager())) {
            processSide(side, input.arrowBatch());
        }
    }

    private void processSide(int side, ArrowRowDataBatch input) throws Exception {
        try {
            List<byte[]> keys = preencodeKeys[side] ? preencodeKeys(input, keySelectors[side], "window join") : null;
            List<byte[]> rows = new ArrayList<>(input.size());
            for (int index = 0; index < input.size(); index++) {
                BinaryRowData binary = serializers[side].toBinaryRow(input.rowView(index));
                rows.add(BinarySegmentUtils.copyToBytes(
                        binary.getSegments(), binary.getOffset(), binary.getSizeInBytes()));
            }
            try (ArrowWindowJoinCDataBridge.Result result = ArrowWindowJoinCDataBridge.process(
                    nativeHandle(),
                    side,
                    input,
                    keys,
                    rows,
                    leftType,
                    rightType,
                    outputType,
                    joinType,
                    condition,
                    nullFilterKeys,
                    allocator(),
                    memoryManager())) {
                emitBatch(result.output(), true, input.size());
                recordProcessedWithoutStateCalls(input, result.output());
                conditionEvaluations.inc(result.conditionEvaluations());
            }
            updateNativeMetrics();
        } catch (Throwable failure) {
            recordProcessingFailure();
            throw failure;
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
        if (watermark <= emittedWatermark) {
            return;
        }
        try (ArrowWindowJoinCDataBridge.Result result = ArrowWindowJoinCDataBridge.advance(
                nativeHandle(),
                watermark,
                leftType,
                rightType,
                outputType,
                joinType,
                condition,
                nullFilterKeys,
                allocator(),
                memoryManager())) {
            emitBatch(result.output(), false, 0);
            recordTimerOutput(result.output(), false);
            conditionEvaluations.inc(result.conditionEvaluations());
        }
        emittedWatermark = watermark;
        recordWatermark();
        output.emitWatermark(new Watermark(watermark));
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

    private void updateNativeMetrics() {
        long[] late = NativeWindowJoinBridge.lateRecordCounts(nativeHandle());
        for (int side = 0; side < 2; side++) {
            long delta = late[side] - observedNativeLateRecords[side];
            if (delta > 0) {
                lateRecordsDroppedRates[side].markEvent(delta);
                observedNativeLateRecords[side] = late[side];
            }
        }
        long[] current = NativeWindowJoinBridge.statistics(nativeHandle());
        if (current.length != 7 || observedNativeStatistics.length != 7) {
            throw new IllegalStateException("Native window join statistics have an incompatible shape");
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
    public void endInput(int inputId) {}

    @Override
    protected void afterNativeStateInitialized(StateInitializationContext context) throws Exception {
        // Flink serializes operator factories before constructing the task. Transient field
        // initializers therefore do not run on the task-side deserialized operator instance.
        inputWatermarks = new long[] {Long.MIN_VALUE, Long.MIN_VALUE};
        observedNativeLateRecords = new long[] {0, 0};
        emittedWatermark = Long.MIN_VALUE;
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

    @Override
    protected void beforeNativeClose() throws Exception {
        if (condition != null) {
            FunctionUtils.closeFunction(condition);
            condition = null;
        }
    }

    @Override
    protected long createMemoryHandle(
            byte[] plan, int maxParallelism, int first, int last, NativeMemoryManager manager) {
        return NativeWindowJoinBridge.create(plan, maxParallelism, first, last, manager);
    }

    @Override
    protected long createRocksDbHandle(
            byte[] plan,
            int maxParallelism,
            int first,
            int last,
            Path database,
            long limit,
            NativeMemoryManager manager) {
        return NativeWindowJoinBridge.createRocksDb(plan, maxParallelism, first, last, database, limit, manager);
    }

    @Override
    protected byte[] snapshotKeyGroup(long handle, int group) {
        return NativeWindowJoinBridge.snapshot(handle, group);
    }

    @Override
    protected void restoreKeyGroup(long handle, int group, byte[] state) {
        NativeWindowJoinBridge.restore(handle, group, state);
    }

    @Override
    protected void checkpointRocks(long handle, Path checkpoint) {
        NativeWindowJoinBridge.checkpointRocks(handle, checkpoint);
    }

    @Override
    protected void importRocksCheckpoint(long handle, Path checkpoint, int first, int last, long limit) {
        NativeWindowJoinBridge.importRocksCheckpoint(handle, checkpoint, first, last, limit);
    }

    @Override
    protected void destroyHandle(long handle) {
        NativeWindowJoinBridge.destroy(handle);
    }

    private static long restoredWatermark(ListState<Long> state) throws Exception {
        long restored = Long.MIN_VALUE;
        for (Long watermark : state.get()) {
            restored = restored == Long.MIN_VALUE ? watermark : Math.min(restored, watermark);
        }
        return restored;
    }

    private static int[] filteredKeyIndices(int[] leftKeys, boolean[] filterNulls) {
        int count = 0;
        for (boolean filter : filterNulls) {
            if (filter) {
                count++;
            }
        }
        int[] result = new int[count];
        int output = 0;
        for (int index = 0; index < filterNulls.length; index++) {
            if (filterNulls[index]) {
                result[output++] = leftKeys[index];
            }
        }
        return result;
    }
}
