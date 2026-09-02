/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.topn;

import java.nio.file.Path;
import java.util.List;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowTopNCDataBridge;
import tech.streamfusion.flink.metrics.FlinkMetricParity;
import tech.streamfusion.flink.state.AbstractStreamFusionArrowKeyedStateOperator;
import tech.streamfusion.nativebridge.NativeMemoryManager;
import tech.streamfusion.nativebridge.NativeTopNBridge;

/** Native Arrow Top-N: Java owns only Flink lifecycle, metrics, and checkpoint coordination. */
final class StreamFusionArrowTopNOperator extends AbstractStreamFusionArrowKeyedStateOperator
        implements OneInputStreamOperator<ArrowRowDataBatch, ArrowRowDataBatch>, BoundedOneInput {
    private final RowType outputType;
    private final boolean preencodeKeys;
    private final RowDataKeySelector partitionSelector;
    private final StreamFusionTopNStrategy strategy;
    private final long flinkCacheSize;

    private transient Counter invalidTopSize;
    private transient Counter comparatorCalls;
    private transient Counter stateGroupsLoaded;
    private transient Counter stateGroupsCommitted;
    private transient Counter invalidRetractions;
    private transient Counter expiredStateGroups;
    private transient long[] observedStatistics;

    StreamFusionArrowTopNOperator(
            RowType inputType,
            RowType outputType,
            int[] partitionKeys,
            byte[] plan,
            RowDataKeySelector partitionSelector,
            StreamFusionTopNStrategy strategy,
            long flinkCacheSize) {
        super(plan, "top-n");
        this.outputType = outputType;
        this.preencodeKeys = requiresPreencodedKeys(inputType, partitionKeys);
        this.partitionSelector = partitionSelector;
        this.strategy = strategy;
        this.flinkCacheSize = flinkCacheSize;
    }

    @Override
    public void open() throws Exception {
        super.open();
        invalidTopSize = getMetricGroup().counter("topn.invalidTopSize");
        comparatorCalls = getMetricGroup().addGroup("StreamFusion").counter("topNComparatorCalls");
        stateGroupsLoaded = getMetricGroup().addGroup("StreamFusion").counter("topNStateGroupsLoaded");
        stateGroupsCommitted = getMetricGroup().addGroup("StreamFusion").counter("topNStateGroupsCommitted");
        invalidRetractions = getMetricGroup().addGroup("StreamFusion").counter("topNInvalidRetractions");
        expiredStateGroups = getMetricGroup().addGroup("StreamFusion").counter("topNExpiredStateGroups");
        if (strategy != StreamFusionTopNStrategy.RETRACT) {
            // Preserve the exact Flink Rank metric contract. Flink captures its zero request/hit
            // values when registering this gauge, so its observable value is always 1.0.
            getMetricGroup().gauge("topn.cache.hitRate", () -> 1.0D);
            long cacheSize = strategy == StreamFusionTopNStrategy.UPDATE_FAST ? flinkCacheSize : 0L;
            getMetricGroup().gauge("topn.cache.size", () -> cacheSize);
        }
        observedStatistics = NativeTopNBridge.statistics(nativeHandle());
    }

    @Override
    public void processElement(StreamRecord<ArrowRowDataBatch> element) throws Exception {
        ArrowRowDataBatch input = element.getValue();
        long now = getProcessingTimeService().getCurrentProcessingTime();
        try {
            List<byte[]> keys = preencodeKeys ? preencodeKeys(input, partitionSelector, "Top-N") : null;
            try (ArrowRowDataBatch result = ArrowTopNCDataBridge.execute(
                    nativeHandle(), now, input, keys, outputType, allocator(), memoryManager())) {
                int physicalOutput = 0;
                if (result.size() > 0) {
                    output.collect(new StreamRecord<>(result));
                    physicalOutput = 1;
                }
                FlinkMetricParity.replacePhysicalRecords(
                        getMetricGroup().getIOMetricGroup().getNumRecordsInCounter(), 1, input.size());
                FlinkMetricParity.replacePhysicalRecords(
                        getMetricGroup().getIOMetricGroup().getNumRecordsOutCounter(), physicalOutput, result.size());
                recordProcessedWithoutStateCalls(input, result);
            }
            updateNativeStatistics();
        } catch (Throwable failure) {
            recordProcessingFailure();
            throw failure;
        }
    }

    private void updateNativeStatistics() {
        long[] current = NativeTopNBridge.statistics(nativeHandle());
        if (current.length != 8 || observedStatistics.length != 8) {
            throw new IllegalStateException("Native Top-N statistics have an incompatible shape");
        }
        recordNativeWindowStatistics(current[0] - observedStatistics[0], current[1] - observedStatistics[1], 0, 0, 0);
        stateGroupsLoaded.inc(current[2] - observedStatistics[2]);
        stateGroupsCommitted.inc(current[3] - observedStatistics[3]);
        expiredStateGroups.inc(current[4] - observedStatistics[4]);
        comparatorCalls.inc(current[5] - observedStatistics[5]);
        invalidRetractions.inc(current[6] - observedStatistics[6]);
        invalidTopSize.inc(current[7] - observedStatistics[7]);
        observedStatistics = current;
    }

    @Override
    protected long createMemoryHandle(
            byte[] plan, int maxParallelism, int firstKeyGroup, int lastKeyGroup, NativeMemoryManager memoryManager) {
        return NativeTopNBridge.create(plan, maxParallelism, firstKeyGroup, lastKeyGroup, memoryManager);
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
        return NativeTopNBridge.createRocksDb(
                plan, maxParallelism, firstKeyGroup, lastKeyGroup, databasePath, memoryLimit, memoryManager);
    }

    @Override
    protected byte[] snapshotKeyGroup(long handle, int keyGroup) {
        return NativeTopNBridge.snapshot(handle, keyGroup);
    }

    @Override
    protected void restoreKeyGroup(long handle, int keyGroup, byte[] state) {
        NativeTopNBridge.restore(handle, keyGroup, state);
    }

    @Override
    protected void checkpointRocks(long handle, Path checkpointDirectory) {
        NativeTopNBridge.checkpointRocks(handle, checkpointDirectory);
    }

    @Override
    protected void importRocksCheckpoint(
            long handle, Path checkpointDirectory, int firstKeyGroup, int lastKeyGroup, long memoryLimit) {
        NativeTopNBridge.importRocksCheckpoint(handle, checkpointDirectory, firstKeyGroup, lastKeyGroup, memoryLimit);
    }

    @Override
    protected void destroyHandle(long handle) {
        NativeTopNBridge.destroy(handle);
    }
}
