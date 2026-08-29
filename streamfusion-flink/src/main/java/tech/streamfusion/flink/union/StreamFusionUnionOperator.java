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
package tech.streamfusion.flink.union;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.streaming.api.operators.AbstractInput;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorV2;
import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.MultipleInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.RecordAttributes;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowUnionCDataBridge;
import tech.streamfusion.flink.arrow.NativeCalcResult;
import tech.streamfusion.flink.memory.StreamFusionTaskMemory;
import tech.streamfusion.proto.plan.v1.Input.Builder;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;
import tech.streamfusion.proto.plan.v1.Union;

/** Vectorized multi-input UNION ALL that leaves scheduling and control events with Flink. */
final class StreamFusionUnionOperator extends AbstractStreamOperatorV2<RowData>
        implements MultipleInputStreamOperator<RowData>, BoundedMultiInput {
    private static final int BATCH_SIZE = 1024;

    private final int inputCount;
    private final RowType rowType;
    private final RowDataSerializer serializer;
    private final byte[] serializedPlan;
    private final List<List<BufferedRow>> rowsByInput;
    private final transient org.apache.flink.runtime.execution.Environment environment;
    private int bufferedRows;
    private transient StreamFusionTaskMemory taskMemory;

    StreamFusionUnionOperator(StreamOperatorParameters<RowData> parameters, int inputCount, RowType rowType) {
        super(parameters, inputCount);
        if (inputCount < 2) {
            throw new IllegalArgumentException("StreamFusion UNION ALL requires at least two inputs");
        }
        this.inputCount = inputCount;
        this.rowType = rowType;
        this.environment = parameters.getContainingTask().getEnvironment();
        this.serializer = new RowDataSerializer(rowType);
        this.serializedPlan = createPlan(inputCount);
        this.rowsByInput = new ArrayList<>(inputCount);
        for (int index = 0; index < inputCount; index++) {
            rowsByInput.add(new ArrayList<>());
        }
    }

    @Override
    public void open() throws Exception {
        super.open();
        taskMemory = StreamFusionTaskMemory.create(
                environment, getOperatorConfig(), getMetricGroup(), "streamfusion-union", serializedPlan);
    }

    @Override
    public List<Input> getInputs() {
        List<Input> inputs = new ArrayList<>(inputCount);
        for (int index = 0; index < inputCount; index++) {
            inputs.add(new UnionInput(this, index + 1));
        }
        return inputs;
    }

    @Override
    public void endInput(int inputId) {
        flushBatch();
    }

    @Override
    public void finish() {
        flushBatch();
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) {
        flushBatch();
    }

    private void buffer(int inputIndex, StreamRecord<RowData> element) {
        RowData row = element.getValue();
        rowsByInput
                .get(inputIndex)
                .add(new BufferedRow(
                        serializer.copy(row),
                        row.getRowKind(),
                        element.hasTimestamp(),
                        element.getTimestamp(),
                        bufferedRows));
        bufferedRows++;
        if (bufferedRows == BATCH_SIZE) {
            flushBatch();
        }
    }

    private void flushBatch() {
        if (bufferedRows == 0) {
            return;
        }
        List<BufferedRow> metadata = new ArrayList<>(bufferedRows);
        List<ArrowRowDataBatch> inputs = new ArrayList<>(inputCount);
        try {
            for (List<BufferedRow> inputRows : rowsByInput) {
                List<RowData> values = new ArrayList<>(inputRows.size());
                for (BufferedRow row : inputRows) {
                    values.add(row.value);
                    metadata.add(row);
                }
                inputs.add(ArrowRowDataBatch.transpose(values, rowType, taskMemory.allocator()));
            }
            try (NativeCalcResult nativeResult = ArrowUnionCDataBridge.executeWithSelection(
                    taskMemory.executionContext(), inputs, rowType, taskMemory.allocator())) {
                emit(nativeResult, metadata);
            }
        } finally {
            inputs.forEach(ArrowRowDataBatch::close);
        }
        rowsByInput.forEach(List::clear);
        bufferedRows = 0;
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

    private void emit(NativeCalcResult nativeResult, List<BufferedRow> metadata) {
        ArrowRowDataBatch outputBatch = nativeResult.batch();
        int[] inputRows = new int[outputBatch.size()];
        int[] arrivalIndexes = new int[metadata.size()];
        for (int index = 0; index < outputBatch.size(); index++) {
            inputRows[index] = nativeResult.inputRow(index);
        }
        for (int index = 0; index < metadata.size(); index++) {
            arrivalIndexes[index] = metadata.get(index).arrivalIndex;
        }
        for (int index : UnionEmissionOrder.restore(inputRows, arrivalIndexes)) {
            int inputRow = inputRows[index];
            if (inputRow < 0 || inputRow >= metadata.size()) {
                throw new IllegalStateException("Native UNION ALL returned invalid input-row ordinal " + inputRow);
            }
            BufferedRow source = metadata.get(inputRow);
            RowData outputRow = outputBatch.rowView(index);
            outputRow.setRowKind(source.rowKind);
            StreamRecord<RowData> outputRecord = source.hasTimestamp
                    ? new StreamRecord<>(outputRow, source.timestamp)
                    : new StreamRecord<>(outputRow);
            output.collect(outputRecord);
        }
    }

    static byte[] createPlan(int inputCount) {
        Union.Builder union = Union.newBuilder();
        for (int inputIndex = 0; inputIndex < inputCount; inputIndex++) {
            Builder input = tech.streamfusion.proto.plan.v1.Input.newBuilder().setInputIndex(inputIndex);
            union.addInputs(Operator.newBuilder().setInput(input));
        }
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setUnion(union))
                .build()
                .toByteArray();
    }

    private final class UnionInput extends AbstractInput<RowData, RowData> {
        private UnionInput(AbstractStreamOperatorV2<RowData> owner, int inputId) {
            super(owner, inputId);
        }

        @Override
        public void processElement(StreamRecord<RowData> element) {
            buffer(inputId - 1, element);
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
    }

    private static final class BufferedRow {
        private final RowData value;
        private final RowKind rowKind;
        private final boolean hasTimestamp;
        private final long timestamp;
        private final int arrivalIndex;

        private BufferedRow(RowData value, RowKind rowKind, boolean hasTimestamp, long timestamp, int arrivalIndex) {
            this.value = value;
            this.rowKind = rowKind;
            this.hasTimestamp = hasTimestamp;
            this.timestamp = timestamp;
            this.arrivalIndex = arrivalIndex;
        }
    }
}
