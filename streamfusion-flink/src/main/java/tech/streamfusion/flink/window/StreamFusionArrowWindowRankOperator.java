/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.window;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.binary.BinarySegmentUtils;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowWindowRankCDataBridge;
import tech.streamfusion.flink.metrics.FlinkMetricParity;
import tech.streamfusion.flink.state.AbstractStreamFusionArrowKeyedStateOperator;
import tech.streamfusion.nativebridge.NativeMemoryManager;
import tech.streamfusion.nativebridge.NativeWindowRankBridge;

/** Key-grouped native Window Top-N with exact generated Flink sort comparison at timer output. */
final class StreamFusionArrowWindowRankOperator extends AbstractStreamFusionArrowKeyedStateOperator
        implements OneInputStreamOperator<ArrowRowDataBatch, ArrowRowDataBatch>, BoundedOneInput {
    private final RowType inputType;
    private final RowType outputType;
    private final int[] partitionKeys;
    private final int sortKeyArity;
    private final long rankStart;
    private final long rankEnd;
    private final boolean outputRankNumber;
    private final RowDataKeySelector partitionSelector;
    private final RowDataKeySelector sortSelector;
    private final GeneratedRecordComparator generatedComparator;
    private final boolean preencodeKeys;

    private transient RowDataSerializer rowSerializer;
    private transient RecordComparator comparator;
    private transient ListState<Long> watermarkState;
    private transient long restoredWatermark = Long.MIN_VALUE;
    private transient long currentWatermark = Long.MIN_VALUE;
    private transient Counter lateRecordsDropped;
    private transient Meter lateRecordsDroppedRate;
    private transient long observedNativeLateRecords;
    private transient long[] observedNativeStatistics;

    StreamFusionArrowWindowRankOperator(
            RowType inputType,
            RowType outputType,
            int[] partitionKeys,
            int sortKeyArity,
            long rankStart,
            long rankEnd,
            boolean outputRankNumber,
            byte[] plan,
            RowDataKeySelector partitionSelector,
            RowDataKeySelector sortSelector,
            GeneratedRecordComparator generatedComparator) {
        super(plan, "window rank");
        this.inputType = inputType;
        this.outputType = outputType;
        this.partitionKeys = partitionKeys.clone();
        this.sortKeyArity = sortKeyArity;
        this.rankStart = rankStart;
        this.rankEnd = rankEnd;
        this.outputRankNumber = outputRankNumber;
        this.partitionSelector = partitionSelector;
        this.sortSelector = sortSelector;
        this.generatedComparator = generatedComparator;
        this.preencodeKeys = requiresPreencodedKeys(inputType, partitionKeys);
    }

    @Override
    public void open() throws Exception {
        super.open();
        rowSerializer = new RowDataSerializer(inputType);
        comparator = generatedComparator.newInstance(getRuntimeContext().getUserCodeClassLoader());
        lateRecordsDropped = getMetricGroup().counter("numLateRecordsDropped");
        lateRecordsDroppedRate = getMetricGroup().meter("lateRecordsDroppedRate", new MeterView(lateRecordsDropped));
        observedNativeStatistics = NativeWindowRankBridge.statistics(nativeHandle());
        getMetricGroup().gauge("pendingEventTimeTimers", () -> NativeWindowRankBridge.statistics(nativeHandle())[5]);
        getMetricGroup().gauge("pendingProcessingTimeTimers", () -> 0L);
        getMetricGroup().gauge("watermarkLatency", () -> {
            if (currentWatermark < 0) {
                return 0L;
            }
            return getProcessingTimeService().getCurrentProcessingTime() - currentWatermark;
        });
        if (restoredWatermark != Long.MIN_VALUE) {
            currentWatermark = restoredWatermark;
            emitTimerOutput(restoredWatermark);
        }
    }

    @Override
    public void processElement(StreamRecord<ArrowRowDataBatch> element) throws Exception {
        ArrowRowDataBatch input = element.getValue();
        try {
            List<byte[]> keys = preencodeKeys ? preencodeKeys(input, partitionSelector, "window rank") : null;
            List<byte[]> rows = new ArrayList<>(input.size());
            List<byte[]> sortKeys = new ArrayList<>(input.size());
            for (int index = 0; index < input.size(); index++) {
                RowData row = input.rowView(index);
                rows.add(bytes(rowSerializer.toBinaryRow(row)));
                RowData sort = sortSelector.getKey(row);
                if (!(sort instanceof BinaryRowData)) {
                    throw new IllegalStateException("Native window rank requires a BinaryRowData sort selector");
                }
                sortKeys.add(bytes((BinaryRowData) sort));
            }
            try (ArrowRowDataBatch result = ArrowWindowRankCDataBridge.process(
                    nativeHandle(), input, keys, rows, sortKeys, outputType, allocator(), memoryManager())) {
                emitBatch(result, true, input.size());
                recordProcessedWithoutStateCalls(input, result);
            }
            updateNativeStatistics();
            updateLateMetric();
        } catch (Throwable failure) {
            recordProcessingFailure();
            throw failure;
        }
    }

    @Override
    public void processWatermark(Watermark watermark) throws Exception {
        if (watermark.getTimestamp() > currentWatermark) {
            emitTimerOutput(watermark.getTimestamp());
            currentWatermark = watermark.getTimestamp();
            recordWatermark();
        }
        super.processWatermark(watermark);
    }

    private void emitTimerOutput(long watermark) throws Exception {
        try (ArrowRowDataBatch result = ArrowWindowRankCDataBridge.advance(
                nativeHandle(),
                watermark,
                inputType,
                outputType,
                sortKeyArity,
                rankStart,
                rankEnd,
                outputRankNumber,
                comparator,
                allocator(),
                memoryManager())) {
            emitBatch(result, false, 0);
            recordTimerOutput(result, false);
        }
        updateNativeStatistics();
        updateLateMetric();
    }

    private static byte[] bytes(BinaryRowData row) {
        return BinarySegmentUtils.copyToBytes(row.getSegments(), row.getOffset(), row.getSizeInBytes());
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

    private void updateLateMetric() {
        long current = NativeWindowRankBridge.lateRecordCount(nativeHandle());
        long delta = current - observedNativeLateRecords;
        if (delta > 0) {
            lateRecordsDroppedRate.markEvent(delta);
            observedNativeLateRecords = current;
        }
    }

    private void updateNativeStatistics() {
        long[] current = NativeWindowRankBridge.statistics(nativeHandle());
        if (current.length != 7 || observedNativeStatistics.length != 7) {
            throw new IllegalStateException("Native window rank statistics have an incompatible shape");
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

    @Override
    protected long createMemoryHandle(
            byte[] plan, int maxParallelism, int firstKeyGroup, int lastKeyGroup, NativeMemoryManager memoryManager) {
        return NativeWindowRankBridge.create(plan, maxParallelism, firstKeyGroup, lastKeyGroup, memoryManager);
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
        return NativeWindowRankBridge.createRocksDb(
                plan, maxParallelism, firstKeyGroup, lastKeyGroup, databasePath, memoryLimit, memoryManager);
    }

    @Override
    protected byte[] snapshotKeyGroup(long handle, int keyGroup) {
        return NativeWindowRankBridge.snapshot(handle, keyGroup);
    }

    @Override
    protected void restoreKeyGroup(long handle, int keyGroup, byte[] state) {
        NativeWindowRankBridge.restore(handle, keyGroup, state);
    }

    @Override
    protected void checkpointRocks(long handle, Path checkpointDirectory) {
        NativeWindowRankBridge.checkpointRocks(handle, checkpointDirectory);
    }

    @Override
    protected void importRocksCheckpoint(
            long handle, Path checkpointDirectory, int firstKeyGroup, int lastKeyGroup, long memoryLimit) {
        NativeWindowRankBridge.importRocksCheckpoint(
                handle, checkpointDirectory, firstKeyGroup, lastKeyGroup, memoryLimit);
    }

    @Override
    protected void destroyHandle(long handle) {
        NativeWindowRankBridge.destroy(handle);
    }
}
