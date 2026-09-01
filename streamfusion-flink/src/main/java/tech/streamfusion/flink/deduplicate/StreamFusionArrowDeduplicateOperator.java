/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.deduplicate;

import java.nio.file.Path;
import java.util.List;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.arrow.ArrowDeduplicateCDataBridge;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.NativeArrowDeduplicateResult;
import tech.streamfusion.flink.metrics.FlinkMetricParity;
import tech.streamfusion.flink.state.AbstractStreamFusionArrowKeyedStateOperator;
import tech.streamfusion.nativebridge.NativeDeduplicateBridge;
import tech.streamfusion.nativebridge.NativeMemoryManager;
import tech.streamfusion.proto.plan.v1.Deduplicate;
import tech.streamfusion.proto.plan.v1.Input;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;

/** Stateful keep-last deduplicate whose input and output remain Arrow-backed. */
final class StreamFusionArrowDeduplicateOperator extends AbstractStreamFusionArrowKeyedStateOperator
        implements OneInputStreamOperator<ArrowRowDataBatch, ArrowRowDataBatch> {
    private final RowType rowType;
    private final boolean preencodeKeys;
    private final RowDataKeySelector keySelector;

    StreamFusionArrowDeduplicateOperator(
            RowType rowType, int[] uniqueKeys, int orderIndex, boolean generateInsert, RowDataKeySelector keySelector) {
        super(createPlan(uniqueKeys, orderIndex, generateInsert), "deduplicate");
        this.rowType = rowType;
        this.preencodeKeys = requiresPreencodedKeys(rowType, uniqueKeys);
        this.keySelector = keySelector;
    }

    @Override
    public void processElement(StreamRecord<ArrowRowDataBatch> element) throws Exception {
        ArrowRowDataBatch input = element.getValue();
        try {
            for (int row = 0; row < input.size(); row++) {
                if (input.rowKind(row) != org.apache.flink.types.RowKind.INSERT) {
                    throw new IllegalStateException(
                            "Native rowtime keep-last deduplicate requires insert-only input, got "
                                    + input.rowKind(row));
                }
            }
            List<byte[]> keys = preencodeKeys ? preencodeKeys(input, keySelector, "deduplicate") : null;
            try (NativeArrowDeduplicateResult result =
                    ArrowDeduplicateCDataBridge.executeArrow(nativeHandle(), input, keys, rowType, allocator())) {
                ArrowRowDataBatch outputBatch = result.selectEnvelopeFrom(input);
                output.collect(new StreamRecord<>(outputBatch));
                FlinkMetricParity.replacePhysicalRecords(
                        getMetricGroup().getIOMetricGroup().getNumRecordsInCounter(), 1, input.size());
                FlinkMetricParity.replacePhysicalRecords(
                        getMetricGroup().getIOMetricGroup().getNumRecordsOutCounter(), 1, result.size());
                recordProcessed(input, outputBatch);
            }
        } catch (Throwable failure) {
            recordProcessingFailure();
            throw failure;
        }
    }

    private static byte[] createPlan(int[] uniqueKeys, int orderIndex, boolean generateInsert) {
        Deduplicate.Builder deduplicate = Deduplicate.newBuilder()
                .setInput(Operator.newBuilder().setInput(Input.newBuilder()))
                .setOrderIndex(orderIndex)
                .setKeepLast(true)
                .setGenerateInsert(generateInsert);
        for (int key : uniqueKeys) {
            deduplicate.addKeyIndices(key);
        }
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setDeduplicate(deduplicate))
                .build()
                .toByteArray();
    }

    @Override
    protected long createMemoryHandle(
            byte[] plan, int maxParallelism, int firstKeyGroup, int lastKeyGroup, NativeMemoryManager memoryManager) {
        return NativeDeduplicateBridge.create(plan, maxParallelism, firstKeyGroup, lastKeyGroup, memoryManager);
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
        return NativeDeduplicateBridge.createRocksDb(
                plan, maxParallelism, firstKeyGroup, lastKeyGroup, databasePath, memoryLimit);
    }

    @Override
    protected byte[] snapshotKeyGroup(long handle, int keyGroup) {
        return NativeDeduplicateBridge.snapshot(handle, keyGroup);
    }

    @Override
    protected void restoreKeyGroup(long handle, int keyGroup, byte[] state) {
        NativeDeduplicateBridge.restore(handle, keyGroup, state);
    }

    @Override
    protected void checkpointRocks(long handle, Path checkpointDirectory) {
        NativeDeduplicateBridge.checkpointRocks(handle, checkpointDirectory);
    }

    @Override
    protected void importRocksCheckpoint(
            long handle, Path checkpointDirectory, int firstKeyGroup, int lastKeyGroup, long memoryLimit) {
        NativeDeduplicateBridge.importRocksCheckpoint(
                handle, checkpointDirectory, firstKeyGroup, lastKeyGroup, memoryLimit);
    }

    @Override
    protected void destroyHandle(long handle) {
        NativeDeduplicateBridge.destroy(handle);
    }
}
