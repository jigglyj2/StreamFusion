/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.aggregate;

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

/** Timer-free keyed group aggregate whose input and output remain Arrow-backed. */
final class StreamFusionArrowGroupAggregateOperator extends AbstractStreamFusionArrowKeyedStateOperator
        implements OneInputStreamOperator<ArrowRowDataBatch, ArrowRowDataBatch>, BoundedOneInput {
    private final RowType outputType;
    private final boolean inputChangelog;
    private final boolean preencodeKeys;
    private final RowDataKeySelector keySelector;
    private final long miniBatchSize;
    private final String operatorName;
    private final boolean terminalOutputOnly;
    private transient long[] observedNativeStatistics;
    private transient boolean terminalOutputEmitted;

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
                grouping.length == outputType.getFieldCount() ? "select distinct" : "group aggregate",
                false);
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
        this(
                inputType,
                outputType,
                grouping,
                serializedPlan,
                inputChangelog,
                keySelector,
                miniBatchSize,
                operatorName,
                false);
    }

    StreamFusionArrowGroupAggregateOperator(
            RowType inputType,
            RowType outputType,
            int[] grouping,
            byte[] serializedPlan,
            boolean inputChangelog,
            RowDataKeySelector keySelector,
            long miniBatchSize,
            String operatorName,
            boolean terminalOutputOnly) {
        super(serializedPlan, operatorName, NativeGroupAggregateBridge.keyedStateBridge());
        this.outputType = outputType;
        this.inputChangelog = inputChangelog;
        this.preencodeKeys = requiresPreencodedKeys(inputType, grouping);
        this.keySelector = keySelector;
        this.miniBatchSize = miniBatchSize;
        this.operatorName = operatorName;
        this.terminalOutputOnly = terminalOutputOnly;
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
        if (!terminalOutputOnly) {
            flushBundle();
        }
        recordWatermark();
        super.processWatermark(watermark);
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        if (!terminalOutputOnly) {
            flushBundle();
        }
    }

    @Override
    public void endInput() throws Exception {
        flushBundle();
    }

    private void flushBundle() throws Exception {
        if (terminalOutputOnly && terminalOutputEmitted) {
            return;
        }
        if (!terminalOutputOnly
                && (miniBatchSize == 0 || NativeGroupAggregateBridge.pendingElementCount(nativeHandle()) == 0)) {
            return;
        }
        try {
            do {
                try (ArrowRowDataBatch outputBatch = ArrowGroupAggregateCDataBridge.finishBundle(
                        nativeHandle(), outputType, allocator(), memoryManager())) {
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
                    if (!terminalOutputOnly || outputBatch.size() == 0) {
                        break;
                    }
                }
            } while (true);
            updateNativeStatistics();
            if (terminalOutputOnly) {
                terminalOutputEmitted = true;
            }
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
}
