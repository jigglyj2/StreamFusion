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
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.binary.BinarySegmentUtils;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowWindowDeduplicateCDataBridge;
import tech.streamfusion.flink.metrics.FlinkMetricParity;
import tech.streamfusion.flink.state.AbstractStreamFusionArrowKeyedStateOperator;
import tech.streamfusion.nativebridge.NativeMemoryManager;
import tech.streamfusion.nativebridge.NativeWindowDeduplicateBridge;

/** Key-grouped native window first/last-row deduplication. */
final class StreamFusionArrowWindowDeduplicateOperator extends AbstractStreamFusionArrowKeyedStateOperator
        implements OneInputStreamOperator<ArrowRowDataBatch, ArrowRowDataBatch> {
    private final RowType rowType;
    private final int[] partitionKeys;
    private final RowDataKeySelector keySelector;
    private final boolean preencodeKeys;

    private transient RowDataSerializer serializer;
    private transient ListState<Long> watermarkState;
    private transient long restoredWatermark = Long.MIN_VALUE;
    private transient long currentWatermark = Long.MIN_VALUE;
    private transient Counter lateRecordsDropped;
    private transient Meter lateRecordsDroppedRate;
    private transient long observedNativeLateRecords;
    private transient long[] observedNativeStatistics;

    StreamFusionArrowWindowDeduplicateOperator(
            RowType rowType, int[] partitionKeys, byte[] plan, RowDataKeySelector keySelector) {
        super(plan, "window deduplicate");
        this.rowType = rowType;
        this.partitionKeys = partitionKeys.clone();
        this.keySelector = keySelector;
        this.preencodeKeys = requiresPreencodedKeys(rowType, partitionKeys);
    }

    @Override
    public void open() throws Exception {
        super.open();
        serializer = new RowDataSerializer(rowType);
        lateRecordsDropped = getMetricGroup().counter("numLateRecordsDropped");
        lateRecordsDroppedRate = getMetricGroup().meter("lateRecordsDroppedRate", new MeterView(lateRecordsDropped));
        observedNativeStatistics = NativeWindowDeduplicateBridge.statistics(nativeHandle());
        getMetricGroup()
                .gauge("pendingEventTimeTimers", () -> NativeWindowDeduplicateBridge.statistics(nativeHandle())[5]);
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
            List<byte[]> keys = preencodeKeys ? preencodeKeys(input, keySelector, "window deduplicate") : null;
            List<byte[]> rows = new ArrayList<>(input.size());
            for (int index = 0; index < input.size(); index++) {
                BinaryRowData binary = serializer.toBinaryRow(input.rowView(index));
                rows.add(BinarySegmentUtils.copyToBytes(
                        binary.getSegments(), binary.getOffset(), binary.getSizeInBytes()));
            }
            try (ArrowRowDataBatch result = ArrowWindowDeduplicateCDataBridge.process(
                    nativeHandle(), input, keys, rows, rowType, allocator(), memoryManager())) {
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
        try (ArrowRowDataBatch result = ArrowWindowDeduplicateCDataBridge.advance(
                nativeHandle(), watermark, rowType, allocator(), memoryManager())) {
            emitBatch(result, false, 0);
            recordTimerOutput(result, false);
        }
        updateNativeStatistics();
        updateLateMetric();
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
        long current = NativeWindowDeduplicateBridge.lateRecordCount(nativeHandle());
        long delta = current - observedNativeLateRecords;
        if (delta > 0) {
            lateRecordsDroppedRate.markEvent(delta);
            observedNativeLateRecords = current;
        }
    }

    private void updateNativeStatistics() {
        long[] current = NativeWindowDeduplicateBridge.statistics(nativeHandle());
        if (current.length != 7 || observedNativeStatistics.length != 7) {
            throw new IllegalStateException("Native window deduplicate statistics have an incompatible shape");
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
        return NativeWindowDeduplicateBridge.create(plan, maxParallelism, firstKeyGroup, lastKeyGroup, memoryManager);
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
        return NativeWindowDeduplicateBridge.createRocksDb(
                plan, maxParallelism, firstKeyGroup, lastKeyGroup, databasePath, memoryLimit, memoryManager);
    }

    @Override
    protected byte[] snapshotKeyGroup(long handle, int keyGroup) {
        return NativeWindowDeduplicateBridge.snapshot(handle, keyGroup);
    }

    @Override
    protected void restoreKeyGroup(long handle, int keyGroup, byte[] state) {
        NativeWindowDeduplicateBridge.restore(handle, keyGroup, state);
    }

    @Override
    protected void checkpointRocks(long handle, Path checkpointDirectory) {
        NativeWindowDeduplicateBridge.checkpointRocks(handle, checkpointDirectory);
    }

    @Override
    protected void importRocksCheckpoint(
            long handle, Path checkpointDirectory, int firstKeyGroup, int lastKeyGroup, long memoryLimit) {
        NativeWindowDeduplicateBridge.importRocksCheckpoint(
                handle, checkpointDirectory, firstKeyGroup, lastKeyGroup, memoryLimit);
    }

    @Override
    protected void destroyHandle(long handle) {
        NativeWindowDeduplicateBridge.destroy(handle);
    }
}
