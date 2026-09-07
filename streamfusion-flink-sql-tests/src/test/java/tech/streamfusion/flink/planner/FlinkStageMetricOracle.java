/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.runtime.metrics.groups.InternalOperatorMetricGroup;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.metrics.WatermarkGauge;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;

/** Supplies only the task-level counters/gauges omitted by Flink's bare operator harness. */
class FlinkStageMetricOracle implements AutoCloseable {
    final OneInputStreamOperatorTestHarness<RowData, RowData> harness;
    final WatermarkGauge inputWatermark = new WatermarkGauge();
    final WatermarkGauge outputWatermark = new WatermarkGauge();

    FlinkStageMetricOracle(OneInputStreamOperatorTestHarness<RowData, RowData> harness) {
        this.harness = harness;
        group().gauge("currentInputWatermark", inputWatermark);
        group().gauge("currentOutputWatermark", outputWatermark);
    }

    InternalOperatorMetricGroup group() {
        return (InternalOperatorMetricGroup) harness.getOperator().getMetricGroup();
    }

    @SuppressWarnings("unchecked")
    void accept(StreamElement event) throws Exception {
        if (event instanceof StreamRecord) {
            group().getIOMetricGroup().getNumRecordsInCounter().inc();
            harness.processElement((StreamRecord<RowData>) event);
        } else if (event instanceof Watermark) {
            inputWatermark.setCurrentWatermark(((Watermark) event).getTimestamp());
            harness.processWatermark((Watermark) event);
        } else if (event instanceof WatermarkStatus) harness.processWatermarkStatus((WatermarkStatus) event);
        else if (event instanceof LatencyMarker) harness.getOperator().processLatencyMarker((LatencyMarker) event);
        else throw new AssertionError("Uncovered control " + event);
    }

    List<StreamElement> drain() {
        var result = new ArrayList<StreamElement>();
        for (Object event : harness.getOutput()) {
            if (event instanceof StreamRecord)
                group().getIOMetricGroup().getNumRecordsOutCounter().inc();
            if (event instanceof Watermark) outputWatermark.setCurrentWatermark(((Watermark) event).getTimestamp());
            result.add((StreamElement) event);
        }
        harness.getOutput().clear();
        return result;
    }

    @Override
    public void close() throws Exception {
        harness.close();
    }
}
