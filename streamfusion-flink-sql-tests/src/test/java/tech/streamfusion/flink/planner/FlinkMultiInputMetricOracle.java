/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.groups.InternalOperatorMetricGroup;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.metrics.MinWatermarkGauge;
import org.apache.flink.streaming.runtime.metrics.WatermarkGauge;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.streaming.util.KeyedMultiInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;

/** Adds the task-level counting wrappers/gauges omitted by Flink's bare multiple-input harness. */
final class FlinkMultiInputMetricOracle implements AutoCloseable {
    final Harness harness;
    private final WatermarkGauge[] inputs;
    private final WatermarkGauge output = new WatermarkGauge();

    FlinkMultiInputMetricOracle(Harness harness, int arity) {
        this.harness = harness;
        inputs = new WatermarkGauge[arity];
        for (int port = 0; port < arity; port++) {
            inputs[port] = new WatermarkGauge();
            group().gauge(MetricNames.currentInputWatermarkName(port + 1), inputs[port]);
        }
        group().gauge(MetricNames.IO_CURRENT_INPUT_WATERMARK, new MinWatermarkGauge(inputs));
        group().gauge(MetricNames.IO_CURRENT_OUTPUT_WATERMARK, output);
    }

    InternalOperatorMetricGroup group() {
        return (InternalOperatorMetricGroup) harness.region().getMetricGroup();
    }

    @SuppressWarnings("unchecked")
    void accept(int port, StreamElement event) throws Exception {
        if (event instanceof StreamRecord) {
            group().getIOMetricGroup().getNumRecordsInCounter().inc();
            harness.processElement(port, (StreamRecord<RowData>) event);
        } else if (event instanceof Watermark) {
            inputs[port].setCurrentWatermark(((Watermark) event).getTimestamp());
            harness.processWatermark(port, (Watermark) event);
        } else if (event instanceof WatermarkStatus) harness.processWatermarkStatus(port, (WatermarkStatus) event);
        else if (event instanceof LatencyMarker)
            harness.region().getInputs().get(port).processLatencyMarker((LatencyMarker) event);
        else throw new AssertionError("Uncovered control " + event);
    }

    List<StreamElement> drain() {
        var events = new ArrayList<StreamElement>();
        for (var value : harness.getOutput()) {
            var event = (StreamElement) value;
            if (event instanceof StreamRecord)
                group().getIOMetricGroup().getNumRecordsOutCounter().inc();
            if (event instanceof Watermark) output.setCurrentWatermark(((Watermark) event).getTimestamp());
            events.add(event);
        }
        harness.getOutput().clear();
        return events;
    }

    @Override
    public void close() throws Exception {
        harness.close();
    }

    static final class Harness extends KeyedMultiInputStreamOperatorTestHarness<RowData, RowData> {
        Harness(org.apache.flink.streaming.api.operators.StreamOperatorFactory<RowData> factory) throws Exception {
            super(factory, 16, 1, 0);
        }

        org.apache.flink.streaming.api.operators.MultipleInputStreamOperator<RowData> region() {
            return getCastedOperator();
        }
    }
}
