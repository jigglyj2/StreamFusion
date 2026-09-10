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
import org.apache.flink.table.data.RowData;

/** Adds the task-level counting wrappers/gauges omitted by Flink's bare two-input harness. */
final class FlinkRegularJoinMetricOracle implements FlinkJoinMetricOracle {
    final org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData>
            harness;
    private final WatermarkGauge[] inputs;
    private final WatermarkGauge output = new WatermarkGauge();

    FlinkRegularJoinMetricOracle(
            org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData>
                    harness) {
        this.harness = harness;
        inputs = new WatermarkGauge[2];
        for (int port = 0; port < 2; port++) {
            inputs[port] = new WatermarkGauge();
            group().gauge(MetricNames.currentInputWatermarkName(port + 1), inputs[port]);
        }
        group().gauge(MetricNames.IO_CURRENT_INPUT_WATERMARK, new MinWatermarkGauge(inputs));
        group().gauge(MetricNames.IO_CURRENT_OUTPUT_WATERMARK, output);
    }

    public InternalOperatorMetricGroup group() {
        return (InternalOperatorMetricGroup) harness.getOperator().getMetricGroup();
    }

    @SuppressWarnings("unchecked")
    public void accept(int port, StreamElement event) throws Exception {
        if (event instanceof StreamRecord) {
            group().getIOMetricGroup().getNumRecordsInCounter().inc();
            if (port == 0) harness.processElement1((StreamRecord<RowData>) event);
            else harness.processElement2((StreamRecord<RowData>) event);
        } else if (event instanceof Watermark) {
            inputs[port].setCurrentWatermark(((Watermark) event).getTimestamp());
            if (port == 0) harness.processWatermark1((Watermark) event);
            else harness.processWatermark2((Watermark) event);
        } else if (event instanceof WatermarkStatus) {
            if (port == 0) harness.processWatermarkStatus1((WatermarkStatus) event);
            else harness.processWatermarkStatus2((WatermarkStatus) event);
        } else if (event instanceof LatencyMarker) {
            if (port == 0)
                ((org.apache.flink.table.runtime.operators.join.stream.StreamingJoinOperator) harness.getOperator())
                        .processLatencyMarker1((LatencyMarker) event);
            else
                ((org.apache.flink.table.runtime.operators.join.stream.StreamingJoinOperator) harness.getOperator())
                        .processLatencyMarker2((LatencyMarker) event);
        } else throw new AssertionError("Uncovered control " + event);
    }

    public List<StreamElement> drain() {
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
    public void prepareSnapshotPreBarrier(long checkpoint) throws Exception {
        harness.getOperator().prepareSnapshotPreBarrier(checkpoint);
    }

    @Override
    public void close() throws Exception {
        harness.close();
    }
}
