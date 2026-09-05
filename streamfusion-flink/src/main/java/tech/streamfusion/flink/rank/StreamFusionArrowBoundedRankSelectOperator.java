/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.rank;

import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.OperatorAttributes;
import org.apache.flink.streaming.api.operators.OperatorAttributesBuilder;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.arrow.ArrowExchangeInputCDataBridge;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowTopNCDataBridge;
import tech.streamfusion.flink.exchange.ArrowExchangeInputBatch;
import tech.streamfusion.flink.exchange.NativeExchangeFrame;
import tech.streamfusion.flink.metrics.FlinkMetricParity;
import tech.streamfusion.flink.state.AbstractStreamFusionArrowKeyedStateOperator;
import tech.streamfusion.nativebridge.NativeBoundedRankBridge;
import tech.streamfusion.nativebridge.NativeTopNBridge;

/** Tie-aware bounded RANK selection directly over hash-exchange frames. */
final class StreamFusionArrowBoundedRankSelectOperator extends AbstractStreamFusionArrowKeyedStateOperator
        implements OneInputStreamOperator<NativeExchangeFrame, ArrowRowDataBatch>, BoundedOneInput {
    private final RowType inputType;
    private final RowType outputType;
    private final byte[] exchangePlan;

    private transient long[] observedStatistics;
    private transient Counter groupsLoaded;
    private transient Counter groupsCommitted;
    private transient Counter comparatorCalls;
    private transient Counter invalidRetractions;
    private transient Counter emittedRows;
    private transient Counter nativeInvocations;
    private transient boolean finished;

    StreamFusionArrowBoundedRankSelectOperator(
            RowType inputType, RowType outputType, byte[] plan, byte[] exchangePlan) {
        super(plan, "bounded rank selection", NativeTopNBridge.keyedStateBridge());
        this.inputType = inputType;
        this.outputType = outputType;
        this.exchangePlan = exchangePlan.clone();
    }

    @Override
    public void open() throws Exception {
        super.open();
        observedStatistics = NativeTopNBridge.statistics(nativeHandle());
        groupsLoaded = getMetricGroup().addGroup("StreamFusion").counter("boundedRankStateGroupsLoaded");
        groupsCommitted = getMetricGroup().addGroup("StreamFusion").counter("boundedRankStateGroupsCommitted");
        comparatorCalls = getMetricGroup().addGroup("StreamFusion").counter("boundedRankComparatorCalls");
        invalidRetractions = getMetricGroup().addGroup("StreamFusion").counter("boundedRankInvalidRetractions");
        emittedRows = getMetricGroup().addGroup("StreamFusion").counter("boundedRankEmittedRows");
        nativeInvocations = getMetricGroup().addGroup("StreamFusion").counter("nativeInvocations");
        // This operator owns the two SortOperator stages removed with Flink's local/global rank
        // pipeline, so retain their complete public metric surface with actual native memory and
        // zero spill values for the bounded in-memory/direct-Rocks selection algorithm.
        getMetricGroup().gauge("memoryUsedSizeInBytes", this::managedMemoryUsed);
        getMetricGroup().gauge("numSpillFiles", () -> 0L);
        getMetricGroup().gauge("spillInBytes", () -> 0L);
    }

    @Override
    public void processElement(StreamRecord<NativeExchangeFrame> element) throws Exception {
        try (ArrowExchangeInputBatch decoded = ArrowExchangeInputCDataBridge.decode(
                        exchangePlan, element.getValue(), inputType, allocator(), memoryManager());
                ArrowRowDataBatch result = ArrowTopNCDataBridge.execute(
                        nativeHandle(),
                        0,
                        decoded.arrowBatch(),
                        decoded.routingKeys(),
                        outputType,
                        allocator(),
                        memoryManager())) {
            if (result.size() != 0) {
                throw new IllegalStateException("Bounded RANK emitted before end-of-input");
            }
            FlinkMetricParity.replacePhysicalRecords(
                    getMetricGroup().getIOMetricGroup().getNumRecordsInCounter(), 1, decoded.size());
            recordProcessedWithoutStateCalls(decoded.arrowBatch());
            nativeInvocations.inc();
            NativeBoundedRankBridge.recordExecutedBatch();
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
                        ArrowTopNCDataBridge.finish(nativeHandle(), outputType, allocator(), memoryManager())) {
                    nativeInvocations.inc();
                    NativeBoundedRankBridge.recordExecutedBatch();
                    if (result.size() == 0) {
                        break;
                    }
                    output.collect(new StreamRecord<>(result));
                    FlinkMetricParity.replacePhysicalRecords(
                            getMetricGroup().getIOMetricGroup().getNumRecordsOutCounter(), 1, result.size());
                    recordProcessedWithoutStateCalls(0, result);
                    emittedRows.inc(result.size());
                }
            }
            updateStatistics();
        } catch (Throwable failure) {
            recordProcessingFailure();
            throw failure;
        }
    }

    private void updateStatistics() {
        long[] current = NativeTopNBridge.statistics(nativeHandle());
        if (current.length != 8 || observedStatistics.length != 8) {
            throw new IllegalStateException("Native bounded RANK statistics have an incompatible shape");
        }
        recordNativeWindowStatistics(current[0] - observedStatistics[0], current[1] - observedStatistics[1], 0, 0, 0);
        groupsLoaded.inc(current[2] - observedStatistics[2]);
        groupsCommitted.inc(current[3] - observedStatistics[3]);
        comparatorCalls.inc(current[5] - observedStatistics[5]);
        invalidRetractions.inc(current[6] - observedStatistics[6]);
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
