/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.aggregate;

import java.nio.file.Path;
import java.util.List;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.arrow.ArrowGroupAggregateCDataBridge;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.metrics.FlinkMetricParity;
import tech.streamfusion.flink.state.AbstractStreamFusionArrowKeyedStateOperator;
import tech.streamfusion.nativebridge.NativeGroupAggregateBridge;
import tech.streamfusion.nativebridge.NativeMemoryManager;

/** Timer-free keyed group aggregate whose input and output remain Arrow-backed. */
final class StreamFusionArrowGroupAggregateOperator extends AbstractStreamFusionArrowKeyedStateOperator
        implements OneInputStreamOperator<ArrowRowDataBatch, ArrowRowDataBatch>, BoundedOneInput {
    private final RowType outputType;
    private final boolean inputChangelog;
    private final boolean preencodeKeys;
    private final RowDataKeySelector keySelector;
    private final long miniBatchSize;
    private final String operatorName;
    private transient long[] observedNativeStatistics;

    StreamFusionArrowGroupAggregateOperator(
            RowType inputType,
            RowType outputType,
            int[] grouping,
            byte[] serializedPlan,
            boolean inputChangelog,
            RowDataKeySelector keySelector) {
        this(inputType, outputType, grouping, serializedPlan, inputChangelog, keySelector, 0L);
    }

    StreamFusionArrowGroupAggregateOperator(
            RowType inputType,
            RowType outputType,
            int[] grouping,
            byte[] serializedPlan,
            boolean inputChangelog,
            RowDataKeySelector keySelector,
            long miniBatchSize) {
        this(
                inputType,
                outputType,
                grouping,
                serializedPlan,
                inputChangelog,
                keySelector,
                miniBatchSize,
                grouping.length == outputType.getFieldCount() ? "select distinct" : "group aggregate");
    }

    StreamFusionArrowGroupAggregateOperator(
            RowType inputType,
            RowType outputType,
            int[] grouping,
            byte[] serializedPlan,
            boolean inputChangelog,
            RowDataKeySelector keySelector,
            long miniBatchSize,
            String operatorName) {
        super(serializedPlan, operatorName);
        this.outputType = outputType;
        this.inputChangelog = inputChangelog;
        this.preencodeKeys = requiresPreencodedKeys(inputType, grouping);
        this.keySelector = keySelector;
        this.miniBatchSize = miniBatchSize;
        this.operatorName = operatorName;
    }

    @Override
    public void open() throws Exception {
        super.open();
        observedNativeStatistics = NativeGroupAggregateBridge.statistics(nativeHandle());
        if (miniBatchSize > 0) {
            getRuntimeContext()
                    .getMetricGroup()
                    .gauge(
                            "bundleSize",
                            () -> Math.toIntExact(NativeGroupAggregateBridge.pendingElementCount(nativeHandle())));
            getRuntimeContext().getMetricGroup().gauge("bundleRatio", () -> {
                long keys = NativeGroupAggregateBridge.pendingKeyCount(nativeHandle());
                return keys == 0 ? 0.0 : 1.0 * NativeGroupAggregateBridge.pendingElementCount(nativeHandle()) / keys;
            });
        }
    }

    @Override
    public void processElement(StreamRecord<ArrowRowDataBatch> element) throws Exception {
        ArrowRowDataBatch input = element.getValue();
        try {
            if (!inputChangelog) {
                for (int row = 0; row < input.size(); row++) {
                    if (input.rowKind(row) != org.apache.flink.types.RowKind.INSERT) {
                        throw new IllegalStateException("Native append-only group aggregate got " + input.rowKind(row));
                    }
                }
            }
            List<byte[]> keys = preencodeKeys ? preencodeKeys(input, keySelector, operatorName) : null;
            try (ArrowRowDataBatch outputBatch = ArrowGroupAggregateCDataBridge.execute(
                    nativeHandle(), input, keys, inputChangelog, outputType, allocator(), memoryManager())) {
                int physicalOutputRecords = 0;
                if (outputBatch.size() > 0) {
                    output.collect(new StreamRecord<>(outputBatch));
                    physicalOutputRecords = 1;
                }
                FlinkMetricParity.replacePhysicalRecords(
                        getMetricGroup().getIOMetricGroup().getNumRecordsInCounter(), 1, input.size());
                FlinkMetricParity.replacePhysicalRecords(
                        getMetricGroup().getIOMetricGroup().getNumRecordsOutCounter(),
                        physicalOutputRecords,
                        outputBatch.size());
                recordProcessedWithoutStateCalls(input, outputBatch);
            }
            updateNativeStatistics();
        } catch (Throwable failure) {
            recordProcessingFailure();
            throw failure;
        }
    }

    @Override
    public void processWatermark(Watermark watermark) throws Exception {
        flushBundle();
        recordWatermark();
        super.processWatermark(watermark);
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        flushBundle();
    }

    @Override
    public void endInput() throws Exception {
        flushBundle();
    }

    private void flushBundle() throws Exception {
        if (miniBatchSize == 0 || NativeGroupAggregateBridge.pendingElementCount(nativeHandle()) == 0) {
            return;
        }
        try (ArrowRowDataBatch outputBatch =
                ArrowGroupAggregateCDataBridge.finishBundle(nativeHandle(), outputType, allocator(), memoryManager())) {
            int physicalOutputRecords = 0;
            if (outputBatch.size() > 0) {
                output.collect(new StreamRecord<>(outputBatch));
                physicalOutputRecords = 1;
            }
            FlinkMetricParity.replacePhysicalRecords(
                    getMetricGroup().getIOMetricGroup().getNumRecordsOutCounter(),
                    physicalOutputRecords,
                    outputBatch.size());
            recordProcessedWithoutStateCalls(0, outputBatch);
            updateNativeStatistics();
        } catch (Throwable failure) {
            recordProcessingFailure();
            throw failure;
        }
    }

    private void updateNativeStatistics() {
        long[] current = NativeGroupAggregateBridge.statistics(nativeHandle());
        if (current.length != 2 || observedNativeStatistics.length != 2) {
            throw new IllegalStateException("Native group aggregate statistics have an incompatible shape");
        }
        recordNativeWindowStatistics(
                current[0] - observedNativeStatistics[0], current[1] - observedNativeStatistics[1], 0, 0, 0);
        observedNativeStatistics = current;
    }

    @Override
    protected long createMemoryHandle(
            byte[] plan, int maxParallelism, int firstKeyGroup, int lastKeyGroup, NativeMemoryManager memoryManager) {
        return NativeGroupAggregateBridge.create(plan, maxParallelism, firstKeyGroup, lastKeyGroup, memoryManager);
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
        return NativeGroupAggregateBridge.createRocksDb(
                plan, maxParallelism, firstKeyGroup, lastKeyGroup, databasePath, memoryLimit, memoryManager);
    }

    @Override
    protected byte[] snapshotKeyGroup(long handle, int keyGroup) {
        return NativeGroupAggregateBridge.snapshot(handle, keyGroup);
    }

    @Override
    protected void restoreKeyGroup(long handle, int keyGroup, byte[] state) {
        NativeGroupAggregateBridge.restore(handle, keyGroup, state);
    }

    @Override
    protected void checkpointRocks(long handle, Path checkpointDirectory) {
        NativeGroupAggregateBridge.checkpointRocks(handle, checkpointDirectory);
    }

    @Override
    protected void importRocksCheckpoint(
            long handle, Path checkpointDirectory, int firstKeyGroup, int lastKeyGroup, long memoryLimit) {
        NativeGroupAggregateBridge.importRocksCheckpoint(
                handle, checkpointDirectory, firstKeyGroup, lastKeyGroup, memoryLimit);
    }

    @Override
    protected void destroyHandle(long handle) {
        NativeGroupAggregateBridge.destroy(handle);
    }
}
