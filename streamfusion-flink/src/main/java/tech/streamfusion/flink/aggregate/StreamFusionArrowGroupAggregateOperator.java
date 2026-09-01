/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.aggregate;

import java.nio.file.Path;
import java.util.List;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
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
    private final boolean selectDistinct;
    private final RowDataKeySelector keySelector;

    StreamFusionArrowGroupAggregateOperator(
            RowType inputType,
            RowType outputType,
            int[] grouping,
            byte[] serializedPlan,
            boolean inputChangelog,
            RowDataKeySelector keySelector) {
        super(serializedPlan, grouping.length == outputType.getFieldCount() ? "select distinct" : "group aggregate");
        this.outputType = outputType;
        this.inputChangelog = inputChangelog;
        this.preencodeKeys = requiresPreencodedKeys(inputType, grouping);
        this.selectDistinct = grouping.length == outputType.getFieldCount();
        this.keySelector = keySelector;
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
            List<byte[]> keys = preencodeKeys
                    ? preencodeKeys(input, keySelector, selectDistinct ? "select distinct" : "group aggregate")
                    : null;
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
                recordProcessed(input, outputBatch);
            }
        } catch (Throwable failure) {
            recordProcessingFailure();
            throw failure;
        }
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
