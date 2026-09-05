/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.sort;

import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.OperatorAttributes;
import org.apache.flink.streaming.api.operators.OperatorAttributesBuilder;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.arrow.ArrowBoundedSortCDataBridge;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.metrics.FlinkMetricParity;
import tech.streamfusion.flink.state.AbstractStreamFusionArrowKeyedStateOperator;
import tech.streamfusion.nativebridge.NativeBoundedSortBridge;

/** Local or global bounded SortLimit over an Arrow stream. */
final class StreamFusionArrowBoundedSortLimitOperator extends AbstractStreamFusionArrowKeyedStateOperator
        implements OneInputStreamOperator<ArrowRowDataBatch, ArrowRowDataBatch>, BoundedOneInput {
    private final RowType outputType;

    private transient long[] observedStatistics;
    private transient Counter rowsLoaded;
    private transient Counter rowsCommitted;
    private transient Counter invalidRetractions;
    private transient Counter comparatorCalls;
    private transient Counter emittedRows;
    private transient boolean finished;

    StreamFusionArrowBoundedSortLimitOperator(RowType outputType, byte[] plan) {
        super(plan, "bounded sort limit", NativeBoundedSortBridge.keyedStateBridge());
        this.outputType = outputType;
    }

    @Override
    public void open() throws Exception {
        super.open();
        observedStatistics = NativeBoundedSortBridge.statistics(nativeHandle());
        rowsLoaded = getMetricGroup().addGroup("StreamFusion").counter("boundedSortLimitRowsLoaded");
        rowsCommitted = getMetricGroup().addGroup("StreamFusion").counter("boundedSortLimitRowsCommitted");
        invalidRetractions = getMetricGroup().addGroup("StreamFusion").counter("boundedSortLimitInvalidRetractions");
        comparatorCalls = getMetricGroup().addGroup("StreamFusion").counter("boundedSortLimitComparatorCalls");
        emittedRows = getMetricGroup().addGroup("StreamFusion").counter("boundedSortLimitEmittedRows");
    }

    @Override
    public void processElement(StreamRecord<ArrowRowDataBatch> element) throws Exception {
        ArrowRowDataBatch input = element.getValue();
        try {
            ArrowBoundedSortCDataBridge.process(nativeHandle(), input, memoryManager());
            FlinkMetricParity.replacePhysicalRecords(
                    getMetricGroup().getIOMetricGroup().getNumRecordsInCounter(), 1, input.size());
            recordProcessedWithoutStateCalls(input);
            updateStatistics();
        } catch (Throwable failure) {
            recordProcessingFailure();
            throw failure;
        }
    }

    @Override
    public void endInput() throws Exception {
        if (finished) {
            return;
        }
        finished = true;
        try {
            while (true) {
                try (ArrowRowDataBatch result =
                        ArrowBoundedSortCDataBridge.finish(nativeHandle(), outputType, allocator(), memoryManager())) {
                    if (result.size() == 0) {
                        break;
                    }
                    output.collect(new StreamRecord<>(result));
                    FlinkMetricParity.replacePhysicalRecords(
                            getMetricGroup().getIOMetricGroup().getNumRecordsOutCounter(), 1, result.size());
                    recordProcessedWithoutStateCalls(0, result);
                }
            }
        } catch (Throwable failure) {
            recordProcessingFailure();
            throw failure;
        }
        updateStatistics();
    }

    @Override
    @SuppressWarnings("rawtypes")
    public void setKeyContextElement1(StreamRecord record) {
        // Native bounded state is explicitly key-group scoped. Flink still initializes its keyed
        // backend from the transformation selector, but no per-batch current key is needed.
    }

    private void updateStatistics() {
        long[] current = NativeBoundedSortBridge.statistics(nativeHandle());
        if (current.length != 7 || observedStatistics.length != 7) {
            throw new IllegalStateException("Native bounded sort-limit statistics have an incompatible shape");
        }
        recordNativeWindowStatistics(current[0] - observedStatistics[0], current[1] - observedStatistics[1], 0, 0, 0);
        rowsLoaded.inc(current[2] - observedStatistics[2]);
        rowsCommitted.inc(current[3] - observedStatistics[3]);
        invalidRetractions.inc(current[4] - observedStatistics[4]);
        comparatorCalls.inc(current[5] - observedStatistics[5]);
        emittedRows.inc(current[6] - observedStatistics[6]);
        observedStatistics = current;
    }

    @Override
    public OperatorAttributes getOperatorAttributes() {
        return new OperatorAttributesBuilder()
                .setOutputOnlyAfterEndOfStream(true)
                .setInternalSorterSupported(true)
                .build();
    }
}
