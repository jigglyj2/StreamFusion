/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.rank;

import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.arrow.ArrowBoundedRankCDataBridge;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.memory.FlinkManagedMemory;
import tech.streamfusion.flink.metrics.FlinkMetricParity;
import tech.streamfusion.nativebridge.NativeBoundedRankBridge;

/** Native BatchExecRank state machine over its Flink-sorted Arrow input. */
final class StreamFusionArrowBoundedRankOperator extends AbstractStreamOperator<ArrowRowDataBatch>
        implements OneInputStreamOperator<ArrowRowDataBatch, ArrowRowDataBatch> {
    private final byte[] plan;
    private final RowType outputType;

    private transient FlinkManagedMemory managedMemory;
    private transient long nativeHandle;
    private transient long[] observedStatistics;
    private transient Counter comparatorCalls;
    private transient Counter emittedRows;
    private transient Counter nativeInvocations;
    private transient Counter processingFailures;

    StreamFusionArrowBoundedRankOperator(byte[] plan, RowType outputType) {
        this.plan = plan.clone();
        this.outputType = outputType;
    }

    @Override
    public void open() throws Exception {
        super.open();
        managedMemory = FlinkManagedMemory.create(
                getContainingTask().getEnvironment(), getOperatorConfig(), getMetricGroup(), "bounded-rank");
        nativeHandle = NativeBoundedRankBridge.create(plan, managedMemory);
        observedStatistics = NativeBoundedRankBridge.statistics(nativeHandle);
        comparatorCalls = getMetricGroup().addGroup("StreamFusion").counter("boundedRankComparatorCalls");
        emittedRows = getMetricGroup().addGroup("StreamFusion").counter("boundedRankEmittedRows");
        nativeInvocations = getMetricGroup().addGroup("StreamFusion").counter("nativeInvocations");
        processingFailures = getMetricGroup().addGroup("StreamFusion").counter("processingFailures");
    }

    @Override
    public void processElement(StreamRecord<ArrowRowDataBatch> element) throws Exception {
        ArrowRowDataBatch input = element.getValue();
        try (ArrowRowDataBatch result = ArrowBoundedRankCDataBridge.process(
                nativeHandle, input, outputType, managedMemory.allocator(), managedMemory)) {
            int physicalOutput = 0;
            if (result.size() > 0) {
                output.collect(new StreamRecord<>(result));
                physicalOutput = 1;
            }
            FlinkMetricParity.replacePhysicalRecords(
                    getMetricGroup().getIOMetricGroup().getNumRecordsInCounter(), 1, input.size());
            FlinkMetricParity.replacePhysicalRecords(
                    getMetricGroup().getIOMetricGroup().getNumRecordsOutCounter(), physicalOutput, result.size());
            nativeInvocations.inc();
            updateStatistics();
        } catch (Throwable failure) {
            processingFailures.inc();
            throw failure;
        }
    }

    private void updateStatistics() {
        long[] current = NativeBoundedRankBridge.statistics(nativeHandle);
        if (current.length != 2 || observedStatistics.length != 2) {
            throw new IllegalStateException("Native bounded rank statistics have an incompatible shape");
        }
        comparatorCalls.inc(current[0] - observedStatistics[0]);
        emittedRows.inc(current[1] - observedStatistics[1]);
        observedStatistics = current;
    }

    @Override
    public void close() throws Exception {
        RuntimeException failure = null;
        try {
            if (nativeHandle != 0) {
                NativeBoundedRankBridge.destroy(nativeHandle);
                nativeHandle = 0;
            }
        } catch (RuntimeException closeFailure) {
            failure = closeFailure;
        }
        try {
            if (managedMemory != null) {
                managedMemory.close();
                managedMemory = null;
            }
        } catch (RuntimeException closeFailure) {
            if (failure == null) {
                failure = closeFailure;
            } else {
                failure.addSuppressed(closeFailure);
            }
        } finally {
            super.close();
        }
        if (failure != null) {
            throw failure;
        }
    }
}
