/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.over;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.OperatorAttributes;
import org.apache.flink.streaming.api.operators.OperatorAttributesBuilder;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.arrow.ArrowExchangeInputCDataBridge;
import tech.streamfusion.flink.arrow.ArrowOverAggregateCDataBridge;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.exchange.ArrowExchangeInputBatch;
import tech.streamfusion.flink.exchange.NativeExchangeFrame;
import tech.streamfusion.flink.metrics.FlinkMetricParity;
import tech.streamfusion.flink.state.AbstractStreamFusionArrowKeyedStateOperator;
import tech.streamfusion.nativebridge.NativeOverAggregateBridge;

/** Hash-partitioned bounded OVER with its required sort fused behind one native boundary. */
public final class StreamFusionArrowBoundedOverAggregateOperator extends AbstractStreamFusionArrowKeyedStateOperator
        implements OneInputStreamOperator<NativeExchangeFrame, ArrowRowDataBatch>, BoundedOneInput {
    private final RowType inputType;
    private final RowType outputType;
    private final byte[] exchangePlan;

    private transient long[] observedStatistics;
    private transient Counter idsNotFound;
    private transient Counter sortKeysNotFound;
    private transient Counter nativeInvocations;
    private transient boolean finished;

    public StreamFusionArrowBoundedOverAggregateOperator(
            RowType inputType, RowType outputType, byte[] plan, byte[] exchangePlan) {
        super(plan, "bounded over aggregate", NativeOverAggregateBridge.keyedStateBridge());
        this.inputType = inputType;
        this.outputType = outputType;
        this.exchangePlan = exchangePlan.clone();
    }

    @Override
    public void open() throws Exception {
        super.open();
        observedStatistics = NativeOverAggregateBridge.statistics(nativeHandle());
        idsNotFound = getMetricGroup().counter("numOfIdsNotFound");
        sortKeysNotFound = getMetricGroup().counter("numOfSortKeysNotFound");
        MetricGroup diagnostics = getMetricGroup().addGroup("StreamFusion");
        nativeInvocations = diagnostics.counter("nativeInvocations");
        // Preserve the public metric surface of the absorbed BatchExecSort. The current native
        // algorithm is managed-memory/direct-Rocks backed and does not create Flink spill files.
        getMetricGroup().gauge("memoryUsedSizeInBytes", this::managedMemoryUsed);
        getMetricGroup().gauge("numSpillFiles", () -> 0L);
        getMetricGroup().gauge("spillInBytes", () -> 0L);
    }

    @Override
    public void processElement(StreamRecord<NativeExchangeFrame> element) throws Exception {
        try (ArrowExchangeInputBatch decoded = ArrowExchangeInputCDataBridge.decode(
                        exchangePlan, element.getValue(), inputType, allocator(), memoryManager());
                ArrowRowDataBatch result = ArrowOverAggregateCDataBridge.process(
                        nativeHandle(),
                        decoded.arrowBatch(),
                        decoded.routingKeys(),
                        true,
                        0,
                        outputType,
                        allocator(),
                        memoryManager())) {
            if (result.size() != 0) {
                throw new IllegalStateException("Bounded OVER emitted before end-of-input");
            }
            FlinkMetricParity.replacePhysicalRecords(
                    getMetricGroup().getIOMetricGroup().getNumRecordsInCounter(), 1, decoded.size());
            recordProcessedWithoutStateCalls(decoded.arrowBatch());
            nativeInvocations.inc();
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
                try (ArrowRowDataBatch result = ArrowOverAggregateCDataBridge.finish(
                        nativeHandle(), outputType, allocator(), memoryManager())) {
                    nativeInvocations.inc();
                    if (result.size() == 0) {
                        break;
                    }
                    output.collect(new StreamRecord<>(result));
                    FlinkMetricParity.replacePhysicalRecords(
                            getMetricGroup().getIOMetricGroup().getNumRecordsOutCounter(), 1, result.size());
                    recordProcessedWithoutStateCalls(0, result);
                }
            }
            updateStatistics();
        } catch (Throwable failure) {
            recordProcessingFailure();
            throw failure;
        }
    }

    private void updateStatistics() {
        long[] current = NativeOverAggregateBridge.statistics(nativeHandle());
        if (current.length != 10 || observedStatistics.length != 10) {
            throw new IllegalStateException("Native bounded OVER statistics have an incompatible shape");
        }
        recordNativeWindowStatistics(current[0] - observedStatistics[0], current[1] - observedStatistics[1], 0, 0, 0);
        idsNotFound.inc(current[2] - observedStatistics[2]);
        sortKeysNotFound.inc(current[3] - observedStatistics[3]);
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
