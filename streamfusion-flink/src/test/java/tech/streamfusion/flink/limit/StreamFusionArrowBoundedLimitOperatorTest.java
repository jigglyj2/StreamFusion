/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.limit;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.event.WatermarkEvent;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.metrics.groups.InternalOperatorMetricGroup;
import org.apache.flink.runtime.metrics.util.InterceptingOperatorMetricGroup;
import org.apache.flink.runtime.metrics.util.InterceptingTaskMetricGroup;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.RecordAttributes;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchSerializer;

class StreamFusionArrowBoundedLimitOperatorTest {
    private static final RowType ROW_TYPE =
            RowType.of(false, new LogicalType[] {new IntType()}, new String[] {"value"});

    @Test
    void globalOffsetSlicesAcrossBatchesAndPreservesEveryPhysicalRowKind() throws Exception {
        InterceptingOperatorMetricGroup metrics = new InterceptingOperatorMetricGroup() {
            @Override
            public org.apache.flink.metrics.MetricGroup addGroup(String name) {
                return this;
            }

            @Override
            public org.apache.flink.metrics.MetricGroup addGroup(String key, String value) {
                return this;
            }
        };
        InterceptingTaskMetricGroup taskMetrics = new InterceptingTaskMetricGroup() {
            @Override
            public InternalOperatorMetricGroup getOrAddOperator(
                    OperatorID id, String name, java.util.Map<String, String> additionalVariables) {
                return metrics;
            }
        };
        List<String> output = new ArrayList<>();
        try (RootAllocator inputs = new RootAllocator(64L << 20);
                MockEnvironment environment = new MockEnvironmentBuilder()
                        .setTaskName("Bounded LIMIT metric parity")
                        .setManagedMemorySize(64L << 20)
                        .setInputSplitProvider(new MockInputSplitProvider())
                        .setBufferSize(32 * 1024)
                        .setParallelism(1)
                        .setSubtaskIndex(0)
                        .setMetricGroup(taskMetrics)
                        .build();
                OneInputStreamOperatorTestHarness<ArrowRowDataBatch, ArrowRowDataBatch> harness =
                        harness(new StreamFusionArrowBoundedLimitOperator(true, 2, 5), environment, output)) {
            // The task wrappers account one physical Arrow message before invoking the operator.
            ((Counter) metrics.get("numRecordsIn")).inc();
            ((Counter) metrics.get("numRecordsOut")).inc();
            process(harness, inputs, row(0, RowKind.INSERT), row(1, RowKind.DELETE), row(2, RowKind.UPDATE_BEFORE));
            ((Counter) metrics.get("numRecordsIn")).inc();
            ((Counter) metrics.get("numRecordsOut")).inc();
            process(harness, inputs, row(3, RowKind.UPDATE_AFTER), row(4, RowKind.INSERT), row(5, RowKind.DELETE));

            assertThat(output).containsExactly("-U:2", "+U:3", "+I:4");
            assertThat(((Counter) metrics.get("numRecordsIn")).getCount()).isEqualTo(6);
            assertThat(((Counter) metrics.get("numRecordsOut")).getCount()).isEqualTo(3);
        }
    }

    @Test
    void localLimitIgnoresTheGlobalOffsetAndStopsAtLimitEnd() throws Exception {
        List<String> output = new ArrayList<>();
        try (RootAllocator inputs = new RootAllocator(64L << 20);
                OneInputStreamOperatorTestHarness<ArrowRowDataBatch, ArrowRowDataBatch> harness =
                        harness(new StreamFusionArrowBoundedLimitOperator(false, 2, 3), null, output)) {
            process(harness, inputs, row(0, RowKind.INSERT), row(1, RowKind.DELETE));
            process(harness, inputs, row(2, RowKind.UPDATE_BEFORE), row(3, RowKind.UPDATE_AFTER));
            assertThat(output).containsExactly("+I:0", "-D:1", "-U:2");
        }
    }

    private static OneInputStreamOperatorTestHarness<ArrowRowDataBatch, ArrowRowDataBatch> harness(
            StreamFusionArrowBoundedLimitOperator operator, MockEnvironment environment, List<String> output)
            throws Exception {
        OneInputStreamOperatorTestHarness<ArrowRowDataBatch, ArrowRowDataBatch> harness = environment == null
                ? new OneInputStreamOperatorTestHarness<>(operator)
                : new OneInputStreamOperatorTestHarness<>(operator, environment);
        harness.setOutputCreator(ignored -> new CapturingOutput(output));
        harness.setup(ArrowRowDataBatchSerializer.INSTANCE);
        harness.open();
        return harness;
    }

    private static void process(
            OneInputStreamOperatorTestHarness<ArrowRowDataBatch, ArrowRowDataBatch> harness,
            RootAllocator allocator,
            GenericRowData... rows)
            throws Exception {
        RowKind[] kinds =
                java.util.Arrays.stream(rows).map(GenericRowData::getRowKind).toArray(RowKind[]::new);
        try (ArrowRowDataBatch batch =
                ArrowRowDataBatch.transpose(List.of(rows), ROW_TYPE, allocator).withRowKinds(kinds)) {
            harness.processElement(new StreamRecord<>(batch));
        }
    }

    private static GenericRowData row(int value, RowKind kind) {
        GenericRowData row = GenericRowData.of(value);
        row.setRowKind(kind);
        return row;
    }

    private static final class CapturingOutput implements Output<StreamRecord<ArrowRowDataBatch>> {
        private final List<String> output;

        private CapturingOutput(List<String> output) {
            this.output = output;
        }

        @Override
        public void collect(StreamRecord<ArrowRowDataBatch> record) {
            ArrowRowDataBatch batch = record.getValue();
            for (int row = 0; row < batch.size(); row++) {
                RowData value = batch.rowView(row);
                output.add(batch.rowKind(row).shortString() + ":" + value.getInt(0));
            }
        }

        @Override
        public void close() {}

        @Override
        public void emitWatermark(Watermark mark) {}

        @Override
        public void emitWatermarkStatus(WatermarkStatus watermarkStatus) {}

        @Override
        public <X> void collect(OutputTag<X> outputTag, StreamRecord<X> record) {}

        @Override
        public void emitLatencyMarker(LatencyMarker latencyMarker) {}

        @Override
        public void emitRecordAttributes(RecordAttributes recordAttributes) {}

        @Override
        public void emitWatermark(WatermarkEvent watermarkEvent) {}
    }
}
