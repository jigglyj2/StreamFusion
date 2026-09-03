/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.over;

import java.nio.file.Path;
import java.util.List;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.arrow.ArrowOverAggregateCDataBridge;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.metrics.FlinkMetricParity;
import tech.streamfusion.flink.state.AbstractStreamFusionArrowKeyedStateOperator;
import tech.streamfusion.nativebridge.NativeMemoryManager;
import tech.streamfusion.nativebridge.NativeOverAggregateBridge;

/** Ordered native OVER aggregation with canonical raw keyed state. */
final class StreamFusionArrowOverAggregateOperator extends AbstractStreamFusionArrowKeyedStateOperator
        implements OneInputStreamOperator<ArrowRowDataBatch, ArrowRowDataBatch>, BoundedOneInput {
    private final RowType outputType;
    private final boolean inputChangelog;
    private final boolean preencodeKeys;
    private final RowDataKeySelector keySelector;

    private transient Counter idsNotFound;
    private transient Counter sortKeysNotFound;
    private transient long[] observedStatistics;

    StreamFusionArrowOverAggregateOperator(
            RowType inputType,
            RowType outputType,
            int[] partitionKeys,
            byte[] plan,
            boolean inputChangelog,
            RowDataKeySelector keySelector) {
        super(plan, "over aggregate");
        this.outputType = outputType;
        this.inputChangelog = inputChangelog;
        this.preencodeKeys = requiresPreencodedKeys(inputType, partitionKeys);
        this.keySelector = keySelector;
    }

    @Override
    public void open() throws Exception {
        super.open();
        idsNotFound = getMetricGroup().counter("numOfIdsNotFound");
        sortKeysNotFound = getMetricGroup().counter("numOfSortKeysNotFound");
        observedStatistics = NativeOverAggregateBridge.statistics(nativeHandle());
    }

    @Override
    public void processElement(StreamRecord<ArrowRowDataBatch> element) throws Exception {
        ArrowRowDataBatch input = element.getValue();
        try {
            List<byte[]> keys = preencodeKeys ? preencodeKeys(input, keySelector, "over aggregate") : null;
            try (ArrowRowDataBatch result = ArrowOverAggregateCDataBridge.process(
                    nativeHandle(), input, keys, inputChangelog, outputType, allocator(), memoryManager())) {
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
        } catch (Throwable failure) {
            recordProcessingFailure();
            throw failure;
        }
    }

    private void updateStatistics() {
        long[] current = NativeOverAggregateBridge.statistics(nativeHandle());
        if (current.length != 4 || observedStatistics.length != 4) {
            throw new IllegalStateException("Native OVER statistics have an incompatible shape");
        }
        recordNativeWindowStatistics(current[0] - observedStatistics[0], current[1] - observedStatistics[1], 0, 0, 0);
        idsNotFound.inc(current[2] - observedStatistics[2]);
        sortKeysNotFound.inc(current[3] - observedStatistics[3]);
        observedStatistics = current;
    }

    @Override
    protected long createMemoryHandle(
            byte[] plan, int maxParallelism, int firstKeyGroup, int lastKeyGroup, NativeMemoryManager memoryManager) {
        return NativeOverAggregateBridge.create(plan, maxParallelism, firstKeyGroup, lastKeyGroup, memoryManager);
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
        return NativeOverAggregateBridge.createRocksDb(
                plan, maxParallelism, firstKeyGroup, lastKeyGroup, databasePath, memoryLimit, memoryManager);
    }

    @Override
    protected byte[] snapshotKeyGroup(long handle, int keyGroup) {
        return NativeOverAggregateBridge.snapshot(handle, keyGroup);
    }

    @Override
    protected void restoreKeyGroup(long handle, int keyGroup, byte[] state) {
        NativeOverAggregateBridge.restore(handle, keyGroup, state);
    }

    @Override
    protected void checkpointRocks(long handle, Path checkpointDirectory) {
        NativeOverAggregateBridge.checkpointRocks(handle, checkpointDirectory);
    }

    @Override
    protected void importRocksCheckpoint(
            long handle, Path checkpointDirectory, int firstKeyGroup, int lastKeyGroup, long memoryLimit) {
        NativeOverAggregateBridge.importRocksCheckpoint(
                handle, checkpointDirectory, firstKeyGroup, lastKeyGroup, memoryLimit);
    }

    @Override
    protected void destroyHandle(long handle) {
        NativeOverAggregateBridge.destroy(handle);
    }
}
