/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.window;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.RecordAttributes;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import tech.streamfusion.flink.arrow.ArrowCDataBridge;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.NativeCalcResult;
import tech.streamfusion.flink.memory.StreamFusionTaskMemory;
import tech.streamfusion.proto.plan.v1.Input;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;
import tech.streamfusion.proto.plan.v1.WindowTableFunction;

/** Batched native implementation of Flink's aligned Window TVFs. */
final class StreamFusionWindowTableFunctionOperator extends AbstractStreamOperator<RowData>
        implements OneInputStreamOperator<RowData, RowData>, BoundedOneInput {
    private static final int BATCH_SIZE = 1024;

    private final RowType inputType;
    private final RowType outputType;
    private final RowDataSerializer serializer;
    private final byte[] plan;
    private final List<BufferedRow> rows = new ArrayList<>(BATCH_SIZE);
    private transient StreamFusionTaskMemory taskMemory;

    StreamFusionWindowTableFunctionOperator(
            RowType inputType,
            RowType outputType,
            int timeAttributeIndex,
            StreamFusionWindowTableFunctionTranslator.WindowParameters parameters) {
        this.inputType = inputType;
        this.outputType = outputType;
        this.serializer = new RowDataSerializer(inputType);
        this.plan = createPlan(timeAttributeIndex, parameters);
    }

    @Override
    public void open() throws Exception {
        super.open();
        taskMemory = StreamFusionTaskMemory.create(
                getContainingTask().getEnvironment(),
                getOperatorConfig(),
                getMetricGroup(),
                "streamfusion-window-table-function",
                plan);
    }

    @Override
    public void processElement(StreamRecord<RowData> element) {
        RowData value = element.getValue();
        rows.add(new BufferedRow(serializer.copy(value), value.getRowKind()));
        if (rows.size() == BATCH_SIZE) {
            flushBatch();
        }
    }

    @Override
    public void endInput() {
        flushBatch();
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) {
        flushBatch();
    }

    @Override
    public void processWatermark(Watermark watermark) throws Exception {
        flushBatch();
        super.processWatermark(watermark);
    }

    @Override
    public void processWatermarkStatus(WatermarkStatus watermarkStatus) throws Exception {
        flushBatch();
        super.processWatermarkStatus(watermarkStatus);
    }

    @Override
    public void processLatencyMarker(LatencyMarker latencyMarker) throws Exception {
        flushBatch();
        super.processLatencyMarker(latencyMarker);
    }

    @Override
    public void processRecordAttributes(RecordAttributes recordAttributes) throws Exception {
        flushBatch();
        super.processRecordAttributes(recordAttributes);
    }

    private void flushBatch() {
        if (rows.isEmpty()) {
            return;
        }
        List<RowData> values = new ArrayList<>(rows.size());
        rows.forEach(row -> values.add(row.value));
        try (ArrowRowDataBatch input = ArrowRowDataBatch.transpose(values, inputType, taskMemory.allocator());
                NativeCalcResult result = ArrowCDataBridge.executeWithSelection(
                        taskMemory.executionContext(), input, outputType, taskMemory.allocator())) {
            for (int index = 0; index < result.batch().size(); index++) {
                BufferedRow source = rows.get(result.inputRow(index));
                RowData outputRow = result.batch().rowView(index);
                outputRow.setRowKind(source.kind);
                output.collect(new StreamRecord<>(outputRow));
            }
        }
        rows.clear();
    }

    @Override
    public void close() throws Exception {
        try {
            if (taskMemory != null) {
                taskMemory.close();
                taskMemory = null;
            }
        } finally {
            super.close();
        }
    }

    static byte[] createPlan(
            int timeAttributeIndex, StreamFusionWindowTableFunctionTranslator.WindowParameters parameters) {
        WindowTableFunction window = WindowTableFunction.newBuilder()
                .setInput(Operator.newBuilder().setInput(Input.newBuilder()))
                .setTimeAttributeIndex(timeAttributeIndex)
                .setKind(parameters.kind)
                .setSizeMillis(parameters.sizeMillis)
                .setSlideOrStepMillis(parameters.slideOrStepMillis)
                .setOffsetMillis(parameters.offsetMillis)
                .build();
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setWindowTableFunction(window))
                .build()
                .toByteArray();
    }

    private static final class BufferedRow {
        private final RowData value;
        private final RowKind kind;

        private BufferedRow(RowData value, RowKind kind) {
            this.value = value;
            this.kind = kind;
        }
    }
}
