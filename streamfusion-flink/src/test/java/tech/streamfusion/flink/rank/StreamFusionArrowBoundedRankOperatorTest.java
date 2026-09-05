/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.rank;

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
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchSerializer;

class StreamFusionArrowBoundedRankOperatorTest {
    private static final RowType INPUT_TYPE = RowType.of(
            false, new LogicalType[] {new IntType(), new IntType()}, new String[] {"partition_value", "order_value"});
    private static final RowType OUTPUT_TYPE =
            RowType.of(false, new LogicalType[] {new IntType(), new IntType(), new BigIntType(false)}, new String[] {
                "partition_value", "order_value", "rank_value"
            });

    @Test
    void ranksAcrossArrowBatchBoundariesAndPreservesPhysicalRowKindsAndMetrics() throws Exception {
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
                        .setTaskName("Bounded RANK metric parity")
                        .setManagedMemorySize(64L << 20)
                        .setInputSplitProvider(new MockInputSplitProvider())
                        .setBufferSize(32 * 1024)
                        .setParallelism(1)
                        .setSubtaskIndex(0)
                        .setMetricGroup(taskMetrics)
                        .build();
                OneInputStreamOperatorTestHarness<ArrowRowDataBatch, ArrowRowDataBatch> harness =
                        harness(environment, output)) {
            // The task wrappers account one physical Arrow message before invoking the operator.
            ((Counter) metrics.get("numRecordsIn")).inc();
            ((Counter) metrics.get("numRecordsOut")).inc();
            process(
                    harness,
                    inputs,
                    row(0, 9, RowKind.INSERT),
                    row(0, 9, RowKind.DELETE),
                    row(0, 8, RowKind.UPDATE_BEFORE));
            ((Counter) metrics.get("numRecordsIn")).inc();
            ((Counter) metrics.get("numRecordsOut")).inc();
            process(
                    harness,
                    inputs,
                    row(0, 7, RowKind.UPDATE_AFTER),
                    row(1, 5, RowKind.INSERT),
                    row(1, 4, RowKind.DELETE));

            assertThat(output).containsExactly("-U:0:8:3", "-D:1:4:2");
            assertThat(((Counter) metrics.get("numRecordsIn")).getCount()).isEqualTo(6);
            assertThat(((Counter) metrics.get("numRecordsOut")).getCount()).isEqualTo(2);
            assertThat(((Counter) metrics.get("boundedRankComparatorCalls")).getCount())
                    .isEqualTo(9);
            assertThat(((Counter) metrics.get("boundedRankEmittedRows")).getCount())
                    .isEqualTo(2);
        }
    }

    private static OneInputStreamOperatorTestHarness<ArrowRowDataBatch, ArrowRowDataBatch> harness(
            MockEnvironment environment, List<String> output) throws Exception {
        StreamFusionArrowBoundedRankOperator operator = new StreamFusionArrowBoundedRankOperator(
                StreamFusionBoundedRankPlan.create(INPUT_TYPE, OUTPUT_TYPE, new int[] {0}, new int[] {1}, 2, 3, true),
                OUTPUT_TYPE);
        OneInputStreamOperatorTestHarness<ArrowRowDataBatch, ArrowRowDataBatch> harness =
                new OneInputStreamOperatorTestHarness<>(operator, environment);
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
        try (ArrowRowDataBatch batch = ArrowRowDataBatch.transpose(List.of(rows), INPUT_TYPE, allocator)
                .withRowKinds(kinds)) {
            harness.processElement(new StreamRecord<>(batch));
        }
    }

    private static GenericRowData row(int partition, int order, RowKind kind) {
        GenericRowData row = GenericRowData.of(partition, order);
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
                output.add(batch.rowKind(row).shortString()
                        + ":"
                        + value.getInt(0)
                        + ":"
                        + value.getInt(1)
                        + ":"
                        + value.getLong(2));
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
