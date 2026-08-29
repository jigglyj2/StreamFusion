/*
 * Copyright 2026 StreamFusion Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tech.streamfusion.flink.expand;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
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
import tech.streamfusion.proto.plan.v1.Expand;
import tech.streamfusion.proto.plan.v1.ExpandProjection;
import tech.streamfusion.proto.plan.v1.Expression;
import tech.streamfusion.proto.plan.v1.Input;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;

/** Vectorized Flink Expand implementation backed by a native DataFusion plan. */
final class StreamFusionExpandOperator extends AbstractStreamOperator<RowData>
        implements OneInputStreamOperator<RowData, RowData>, BoundedOneInput {
    private static final int BATCH_SIZE = 1024;

    private final RowType inputType;
    private final RowType outputType;
    private final RowDataSerializer serializer;
    private final byte[] serializedPlan;
    private final List<BufferedRow> rows = new ArrayList<>(BATCH_SIZE);

    StreamFusionExpandOperator(RowType inputType, RowType outputType, List<List<Expression>> projections) {
        this.inputType = inputType;
        this.outputType = outputType;
        this.serializer = new RowDataSerializer(inputType);
        this.serializedPlan = createPlan(projections);
    }

    @Override
    public void processElement(StreamRecord<RowData> element) {
        RowData row = element.getValue();
        rows.add(new BufferedRow(
                serializer.copy(row), row.getRowKind(), element.hasTimestamp(), element.getTimestamp()));
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
        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                ArrowRowDataBatch inputBatch = ArrowRowDataBatch.transpose(values, inputType, allocator);
                NativeCalcResult nativeResult =
                        ArrowCDataBridge.executeWithSelection(serializedPlan, inputBatch, outputType, allocator)) {
            emit(nativeResult);
        }
        rows.clear();
    }

    private void emit(NativeCalcResult nativeResult) {
        ArrowRowDataBatch outputBatch = nativeResult.batch();
        for (int index = 0; index < outputBatch.size(); index++) {
            int inputRow = nativeResult.inputRow(index);
            if (inputRow < 0 || inputRow >= rows.size()) {
                throw new IllegalStateException("Native Expand returned invalid input-row ordinal " + inputRow);
            }
            BufferedRow source = rows.get(inputRow);
            RowData outputRow = outputBatch.rowView(index);
            outputRow.setRowKind(source.rowKind);
            output.collect(
                    source.hasTimestamp
                            ? new StreamRecord<>(outputRow, source.timestamp)
                            : new StreamRecord<>(outputRow));
        }
    }

    static byte[] createPlan(List<List<Expression>> projections) {
        Expand.Builder expand =
                Expand.newBuilder().setInput(Operator.newBuilder().setInput(Input.newBuilder()));
        for (List<Expression> projection : projections) {
            expand.addProjections(ExpandProjection.newBuilder().addAllExpressions(projection));
        }
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setExpand(expand))
                .build()
                .toByteArray();
    }

    private static final class BufferedRow {
        private final RowData value;
        private final RowKind rowKind;
        private final boolean hasTimestamp;
        private final long timestamp;

        private BufferedRow(RowData value, RowKind rowKind, boolean hasTimestamp, long timestamp) {
            this.value = value;
            this.rowKind = rowKind;
            this.hasTimestamp = hasTimestamp;
            this.timestamp = timestamp;
        }
    }
}
