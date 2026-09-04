/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.deduplicate;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.runtime.event.WatermarkEvent;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.metrics.groups.InternalOperatorMetricGroup;
import org.apache.flink.runtime.metrics.util.InterceptingOperatorMetricGroup;
import org.apache.flink.runtime.metrics.util.InterceptingTaskMetricGroup;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;
import org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFinalizer;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.RecordAttributes;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchSerializer;
import tech.streamfusion.flink.state.StreamFusionStateBackend;

class StreamFusionArrowDeduplicateOperatorTest {
    private static final int MAX_PARALLELISM = 128;
    private static final int[] KEYS = {0};
    private static final int ORDER_INDEX = 2;
    private static final RowType ROW_TYPE = RowType.of(
            new LogicalType[] {
                new BigIntType(false),
                new VarCharType(false, VarCharType.MAX_LENGTH),
                new TimestampType(false, TimestampKind.ROWTIME, 3)
            },
            new String[] {"id", "payload", "event_time"});

    @Test
    void supportsEverySynchronousOrderingModeOnBothBackends() throws Exception {
        try (RootAllocator inputs = new RootAllocator(64L << 20)) {
            for (boolean rocksDb : new boolean[] {false, true}) {
                try (DeduplicateHarness rowtime = harness(false, false, true, false, null, rocksDb)) {
                    process(
                            rowtime,
                            inputs,
                            row(7, "later", 2_000),
                            row(7, "earliest", 1_000),
                            row(7, "middle", 1_500));
                    assertThat(take(rowtime)).containsExactly("+I:7:later:2000", "+U:7:earliest:1000");
                }

                try (DeduplicateHarness first = harness(true, false, false, false, null, rocksDb)) {
                    process(first, inputs, row(7, "first", 2_000), row(7, "ignored", 1_000));
                    assertThat(take(first)).containsExactly("+I:7:first:2000");
                }

                try (DeduplicateHarness last = harness(true, true, true, true, null, rocksDb)) {
                    process(last, inputs, row(7, "first", 1_000), row(7, "first", 1_000), row(7, "last", 2_000));
                    assertThat(take(last)).containsExactly("+I:7:first:1000", "-U:7:first:1000", "+U:7:last:2000");
                }
            }
        }
    }

    @Test
    void processingTimeArrowRowsRestoreThroughCanonicalSavepointsAcrossBackendPairs() throws Exception {
        try (RootAllocator inputs = new RootAllocator(64L << 20)) {
            for (boolean sourceRocks : new boolean[] {false, true}) {
                for (boolean targetRocks : new boolean[] {false, true}) {
                    OperatorSubtaskState savepoint;
                    try (DeduplicateHarness source = harness(true, true, true, true, null, sourceRocks)) {
                        process(source, inputs, row(7, "source", 1_000));
                        take(source);
                        savepoint = source.snapshotWithLocalState(
                                        51L, 51L, SavepointType.savepoint(SavepointFormatType.CANONICAL))
                                .getJobManagerOwnedState();
                        assertThat(savepoint.getRawKeyedState()).hasSize(1);
                    }

                    try (DeduplicateHarness target = harness(true, true, true, true, savepoint, targetRocks)) {
                        process(target, inputs, row(7, "target", 2_000));
                        assertThat(take(target)).containsExactly("-U:7:source:1000", "+U:7:target:2000");
                    }
                }
            }
        }
    }

    @Test
    void rowtimeUpdatingStateRestoresAcrossBackendPairsAndCheckpointModes() throws Exception {
        try (RootAllocator inputs = new RootAllocator(64L << 20)) {
            for (boolean sourceRocks : new boolean[] {false, true}) {
                for (boolean targetRocks : new boolean[] {false, true}) {
                    for (boolean unaligned : new boolean[] {false, true}) {
                        OperatorSubtaskState checkpoint;
                        try (DeduplicateHarness source = harness(false, true, true, true, null, sourceRocks)) {
                            process(source, inputs, row(7, "source", 1_000));
                            take(source);
                            checkpoint = snapshot(source, unaligned ? 62L : 61L, unaligned);
                        }

                        try (DeduplicateHarness target = harness(false, true, true, true, checkpoint, targetRocks)) {
                            process(target, inputs, row(7, "target", 2_000));
                            assertThat(take(target)).containsExactly("-U:7:source:1000", "+U:7:target:2000");
                        }
                    }
                }
            }
        }
    }

    @Test
    void exposesLogicalFlinkIoAndCompleteTimerFreeStateMetrics() throws Exception {
        InterceptingOperatorMetricGroup metrics = new InterceptingOperatorMetricGroup() {
            @Override
            public MetricGroup addGroup(String name) {
                return this;
            }

            @Override
            public MetricGroup addGroup(String key, String value) {
                return this;
            }
        };
        InterceptingTaskMetricGroup taskMetrics = new InterceptingTaskMetricGroup() {
            @Override
            public InternalOperatorMetricGroup getOrAddOperator(
                    OperatorID id, String name, Map<String, String> additionalVariables) {
                return metrics;
            }
        };
        RowDataKeySelector selector = KeySelectorUtil.getRowDataSelector(
                StreamFusionArrowDeduplicateOperatorTest.class.getClassLoader(), KEYS, InternalTypeInfo.of(ROW_TYPE));
        StreamFusionArrowDeduplicateOperator operator =
                new StreamFusionArrowDeduplicateOperator(ROW_TYPE, KEYS, ORDER_INDEX, true, true, true, true, selector);
        List<String> captured = new ArrayList<>();
        try (RootAllocator inputs = new RootAllocator(64L << 20);
                MockEnvironment environment = new MockEnvironmentBuilder()
                        .setTaskName("Synchronous deduplicate metric parity")
                        .setManagedMemorySize(64L << 20)
                        .setInputSplitProvider(new MockInputSplitProvider())
                        .setBufferSize(32 * 1024)
                        .setMaxParallelism(MAX_PARALLELISM)
                        .setParallelism(1)
                        .setSubtaskIndex(0)
                        .setMetricGroup(taskMetrics)
                        .build();
                KeyedOneInputStreamOperatorTestHarness<RowData, ArrowRowDataBatch, ArrowRowDataBatch> harness =
                        new KeyedOneInputStreamOperatorTestHarness<>(
                                operator,
                                new ArrowBatchKeySelector(selector),
                                selector.getProducedType(),
                                environment)) {
            harness.setOutputCreator(ignored -> new CapturingOutput(captured));
            harness.setStateBackend(new StreamFusionStateBackend(new HashMapStateBackend()));
            harness.setup(ArrowRowDataBatchSerializer.INSTANCE);
            harness.open();

            assertThat(metrics.get("rocksDbBackend")).isInstanceOf(Gauge.class);
            ((Counter) metrics.get("numRecordsIn")).inc();
            ((Counter) metrics.get("numRecordsOut")).inc();
            try (ArrowRowDataBatch batch = ArrowRowDataBatch.transpose(
                    List.of(row(7, "source", 1_000), row(7, "target", 2_000)), ROW_TYPE, inputs)) {
                harness.processElement(new StreamRecord<>(batch));
            }
            assertThat(captured).containsExactly("+I:7:source:1000", "-U:7:source:1000", "+U:7:target:2000");

            assertThat(((Counter) metrics.get("numRecordsIn")).getCount()).isEqualTo(2L);
            assertThat(((Counter) metrics.get("numRecordsOut")).getCount()).isEqualTo(3L);
            assertThat(((Counter) metrics.get("processedBatches")).getCount()).isEqualTo(1L);
            assertThat(((Counter) metrics.get("processedRows")).getCount()).isEqualTo(2L);
            assertThat(((Counter) metrics.get("emittedRows")).getCount()).isEqualTo(3L);
            assertThat(((Counter) metrics.get("emittedInserts")).getCount()).isEqualTo(1L);
            assertThat(((Counter) metrics.get("emittedUpdateBefores")).getCount())
                    .isEqualTo(1L);
            assertThat(((Counter) metrics.get("emittedUpdateAfters")).getCount())
                    .isEqualTo(1L);
            assertThat(((Counter) metrics.get("emittedDeletes")).getCount()).isZero();
            assertThat(((Counter) metrics.get("stateReadBatches")).getCount()).isEqualTo(1L);
            assertThat(((Counter) metrics.get("stateWriteBatches")).getCount()).isEqualTo(1L);
            assertThat(((Counter) metrics.get("processingFailures")).getCount()).isZero();
            assertThat(((Counter) metrics.get("watermarksAdvanced")).getCount()).isZero();
            assertThat(((Counter) metrics.get("eventTimeTimersFired")).getCount())
                    .isZero();
            assertThat(((Counter) metrics.get("processingTimeTimersFired")).getCount())
                    .isZero();
            assertThat(((Counter) metrics.get("timersRegistered")).getCount()).isZero();
            assertThat(((Counter) metrics.get("timersDeleted")).getCount()).isZero();
            assertThat(((Counter) metrics.get("timersFired")).getCount()).isZero();
            assertThat(((Gauge<?>) metrics.get("rocksDbBackend")).getValue()).isEqualTo(0);
        }
    }

    private static OperatorSubtaskState snapshot(DeduplicateHarness harness, long checkpointId, boolean unaligned)
            throws Exception {
        CheckpointStorageLocationReference location = CheckpointStorageLocationReference.getDefault();
        CheckpointOptions options = unaligned
                ? CheckpointOptions.unaligned(CheckpointType.CHECKPOINT, location)
                : CheckpointOptions.alignedNoTimeout(CheckpointType.CHECKPOINT, location);
        return OperatorSnapshotFinalizer.create(harness.getOperator()
                        .snapshotState(checkpointId, checkpointId, options, new MemCheckpointStreamFactory(64 << 20)))
                .getJobManagerOwnedState();
    }

    private static DeduplicateHarness harness(
            boolean processingTime,
            boolean keepLast,
            boolean generateInsert,
            boolean generateUpdateBefore,
            OperatorSubtaskState state,
            boolean rocksDb)
            throws Exception {
        RowDataKeySelector selector = KeySelectorUtil.getRowDataSelector(
                StreamFusionArrowDeduplicateOperatorTest.class.getClassLoader(), KEYS, InternalTypeInfo.of(ROW_TYPE));
        StreamFusionArrowDeduplicateOperator operator = new StreamFusionArrowDeduplicateOperator(
                ROW_TYPE, KEYS, ORDER_INDEX, !processingTime, keepLast, generateInsert, generateUpdateBefore, selector);
        DeduplicateHarness harness = new DeduplicateHarness(operator, selector);
        harness.setStateBackend(new StreamFusionStateBackend(
                rocksDb ? new EmbeddedRocksDBStateBackend(true) : new HashMapStateBackend()));
        harness.setup(ArrowRowDataBatchSerializer.INSTANCE);
        if (state != null) {
            harness.initializeState(state);
        }
        harness.open();
        return harness;
    }

    private static void process(DeduplicateHarness harness, RootAllocator allocator, GenericRowData... rows)
            throws Exception {
        try (ArrowRowDataBatch batch = ArrowRowDataBatch.transpose(List.of(rows), ROW_TYPE, allocator)) {
            harness.processElement(new StreamRecord<>(batch));
        }
    }

    private static List<String> take(DeduplicateHarness harness) {
        return harness.take();
    }

    private static GenericRowData row(long id, String payload, long timestamp) {
        return GenericRowData.of(id, StringData.fromString(payload), TimestampData.fromEpochMillis(timestamp));
    }

    /** Captures Arrow values synchronously, matching the runtime's fused, borrowed-batch contract. */
    private static final class DeduplicateHarness
            extends KeyedOneInputStreamOperatorTestHarness<RowData, ArrowRowDataBatch, ArrowRowDataBatch> {
        private final List<String> captured = new ArrayList<>();

        private DeduplicateHarness(StreamFusionArrowDeduplicateOperator operator, RowDataKeySelector selector)
                throws Exception {
            super(operator, new ArrowBatchKeySelector(selector), selector.getProducedType(), MAX_PARALLELISM, 1, 0);
            setOutputCreator(ignored -> new CapturingOutput(captured));
        }

        private List<String> take() {
            List<String> result = List.copyOf(captured);
            captured.clear();
            return result;
        }
    }

    private static final class CapturingOutput implements Output<StreamRecord<ArrowRowDataBatch>> {
        private final List<String> captured;

        private CapturingOutput(List<String> captured) {
            this.captured = captured;
        }

        @Override
        public void collect(StreamRecord<ArrowRowDataBatch> record) {
            ArrowRowDataBatch batch = record.getValue();
            for (int index = 0; index < batch.size(); index++) {
                RowData row = batch.rowView(index);
                captured.add(batch.rowKind(index).shortString()
                        + ":"
                        + row.getLong(0)
                        + ":"
                        + row.getString(1)
                        + ":"
                        + row.getTimestamp(2, 3).getMillisecond());
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
