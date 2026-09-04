/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.sort;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.metrics.groups.InternalOperatorMetricGroup;
import org.apache.flink.runtime.metrics.util.InterceptingOperatorMetricGroup;
import org.apache.flink.runtime.metrics.util.InterceptingTaskMetricGroup;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
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
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.planner.plan.nodes.exec.spec.SortSpec;
import org.apache.flink.table.runtime.keyselector.EmptyRowDataKeySelector;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.NullType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchSerializer;
import tech.streamfusion.flink.deduplicate.ArrowBatchKeySelector;
import tech.streamfusion.flink.state.StreamFusionStateBackend;

class StreamFusionArrowTemporalSortOperatorTest {
    private static final RowType ROW_TYPE = RowType.of(
            new LogicalType[] {
                new TimestampType(false, TimestampKind.ROWTIME, 3),
                new IntType(false),
                new VarCharType(false, VarCharType.MAX_LENGTH),
                new TinyIntType(),
                new SmallIntType(),
                new BigIntType(),
                new BooleanType(),
                new FloatType(),
                new DoubleType(),
                new BinaryType(3),
                new VarBinaryType(),
                new DecimalType(10, 3),
                new DateType(),
                new TimeType(3),
                new TimestampType(6),
                new LocalZonedTimestampType(6),
                new ArrayType(new VarCharType()),
                new MapType(new VarCharType(false, VarCharType.MAX_LENGTH), new IntType()),
                RowType.of(new IntType(), new VarCharType()),
                new NullType()
            },
            new String[] {
                "rowtime",
                "number",
                "payload",
                "tiny",
                "small",
                "big",
                "flag",
                "float_value",
                "double_value",
                "fixed_binary",
                "binary_value",
                "decimal_value",
                "date_value",
                "time_value",
                "timestamp_value",
                "timestamp_ltz_value",
                "array_value",
                "map_value",
                "row_value",
                "null_value"
            });
    private static final SortSpec SORT_SPEC =
            SortSpec.builder().addField(0, true, true).addField(1, true, true).build();

    @Test
    void exposesFlinkLogicalIoAndNativeStateTimerMetrics() throws Exception {
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
                    OperatorID id, String name, Map<String, String> additionalVariables) {
                return metrics;
            }
        };
        StreamFusionArrowTemporalSortOperator operator = new StreamFusionArrowTemporalSortOperator(
                ROW_TYPE, false, StreamFusionTemporalSortPlan.create(ROW_TYPE, SORT_SPEC, false));
        List<String> output = new ArrayList<>();
        try (RootAllocator inputs = new RootAllocator(64L << 20);
                MockEnvironment environment = new MockEnvironmentBuilder()
                        .setTaskName("Temporal sort metric parity")
                        .setManagedMemorySize(64L << 20)
                        .setInputSplitProvider(new MockInputSplitProvider())
                        .setBufferSize(32 * 1024)
                        .setMaxParallelism(1)
                        .setParallelism(1)
                        .setSubtaskIndex(0)
                        .setMetricGroup(taskMetrics)
                        .build();
                KeyedOneInputStreamOperatorTestHarness<RowData, ArrowRowDataBatch, ArrowRowDataBatch> harness =
                        new KeyedOneInputStreamOperatorTestHarness<>(
                                operator,
                                new ArrowBatchKeySelector(EmptyRowDataKeySelector.INSTANCE),
                                EmptyRowDataKeySelector.INSTANCE.getProducedType(),
                                environment)) {
            harness.setOutputCreator(ignored -> new CapturingOutput(output));
            harness.setStateBackend(new StreamFusionStateBackend(new HashMapStateBackend()));
            harness.setup(ArrowRowDataBatchSerializer.INSTANCE);
            harness.open();

            assertThat(metrics.get("numRecordsIn")).isInstanceOf(Counter.class);
            assertThat(metrics.get("numRecordsOut")).isInstanceOf(Counter.class);
            assertThat(metrics.get("pendingEventTimeTimers")).isInstanceOf(Gauge.class);
            assertThat(metrics.get("pendingProcessingTimeTimers")).isInstanceOf(Gauge.class);
            assertThat(metrics.get("watermarkLatency")).isInstanceOf(Gauge.class);
            assertThat(metrics.get("rocksDbBackend")).isInstanceOf(Gauge.class);

            ((Counter) metrics.get("numRecordsIn")).inc();
            process(
                    harness,
                    inputs,
                    row(1_000, 2, "second", RowKind.UPDATE_AFTER),
                    row(1_000, 1, "first", RowKind.DELETE));
            assertThat(((Counter) metrics.get("numRecordsIn")).getCount()).isEqualTo(2L);
            assertThat(((Gauge<?>) metrics.get("pendingEventTimeTimers")).getValue())
                    .isEqualTo(1L);

            ((Counter) metrics.get("numRecordsOut")).inc();
            harness.processWatermark(new Watermark(1_000));
            assertThat(((Counter) metrics.get("numRecordsOut")).getCount()).isEqualTo(2L);
            assertThat(((Counter) metrics.get("processedBatches")).getCount()).isEqualTo(1L);
            assertThat(((Counter) metrics.get("processedRows")).getCount()).isEqualTo(2L);
            assertThat(((Counter) metrics.get("emittedRows")).getCount()).isEqualTo(2L);
            assertThat(((Counter) metrics.get("emittedUpdateAfters")).getCount())
                    .isEqualTo(1L);
            assertThat(((Counter) metrics.get("emittedDeletes")).getCount()).isEqualTo(1L);
            assertThat(((Counter) metrics.get("stateReadBatches")).getCount()).isEqualTo(2L);
            assertThat(((Counter) metrics.get("stateWriteBatches")).getCount()).isEqualTo(2L);
            assertThat(((Counter) metrics.get("timersRegistered")).getCount()).isEqualTo(1L);
            assertThat(((Counter) metrics.get("timersFired")).getCount()).isEqualTo(1L);
            assertThat(((Counter) metrics.get("eventTimeTimersFired")).getCount())
                    .isEqualTo(2L);
            assertThat(((Counter) metrics.get("watermarksAdvanced")).getCount()).isEqualTo(1L);
            assertThat(((Gauge<?>) metrics.get("pendingEventTimeTimers")).getValue())
                    .isEqualTo(0L);
            assertThat(((Gauge<?>) metrics.get("pendingProcessingTimeTimers")).getValue())
                    .isEqualTo(0L);
            assertThat(((Gauge<?>) metrics.get("rocksDbBackend")).getValue()).isEqualTo(0);
        }
    }

    @Test
    void eventTimePreservesEveryChangelogKindAndSortsOnBothBackends() throws Exception {
        try (RootAllocator inputs = new RootAllocator(64L << 20)) {
            for (boolean rocks : new boolean[] {false, true}) {
                try (Harness harness = harness(false, null, rocks)) {
                    process(
                            harness,
                            inputs,
                            row(1_000, 2, "second", RowKind.UPDATE_AFTER),
                            row(1_000, 1, "first", RowKind.DELETE),
                            row(2_000, 9, "later", RowKind.INSERT));
                    assertThat(harness.take()).isEmpty();
                    harness.processWatermark(new Watermark(1_000));
                    assertThat(harness.take()).containsExactly("-D:1:first", "+U:2:second");
                    process(harness, inputs, row(1_000, 0, "late", RowKind.UPDATE_BEFORE));
                    harness.processWatermark(new Watermark(2_000));
                    assertThat(harness.take()).containsExactly("+I:9:later");
                }
            }
        }
    }

    @Test
    void processingTimeGroupsOneMillisecondAndSortsAllChangelogKinds() throws Exception {
        try (RootAllocator inputs = new RootAllocator(64L << 20);
                Harness harness = harness(true, null, false)) {
            harness.setProcessingTime(40);
            process(
                    harness,
                    inputs,
                    row(0, 3, "third", RowKind.INSERT),
                    row(0, 1, "first", RowKind.UPDATE_BEFORE),
                    row(0, 2, "second", RowKind.UPDATE_AFTER));
            assertThat(harness.take()).isEmpty();
            harness.setProcessingTime(41);
            assertThat(harness.take()).containsExactly("-U:1:first", "+U:2:second", "+I:3:third");
        }
    }

    @Test
    void boundedInputDrainsPendingRowsOnceAndTerminatesAtTheNoTimerSentinel() throws Exception {
        try (RootAllocator inputs = new RootAllocator(64L << 20);
                Harness harness = harness(false, null, false)) {
            process(harness, inputs, row(1_000, 2, "second", RowKind.INSERT));
            harness.endInput();
            assertThat(harness.take()).containsExactly("+I:2:second");
        }
    }

    @Test
    void restoresTimersAndArrowRowsAcrossBackendsAndCheckpointKinds() throws Exception {
        try (RootAllocator inputs = new RootAllocator(64L << 20)) {
            for (boolean sourceRocks : new boolean[] {false, true}) {
                for (boolean targetRocks : new boolean[] {false, true}) {
                    for (SnapshotKind kind : SnapshotKind.values()) {
                        if (kind != SnapshotKind.CANONICAL && sourceRocks != targetRocks) {
                            continue;
                        }
                        OperatorSubtaskState state;
                        try (Harness source = harness(false, null, sourceRocks)) {
                            process(source, inputs, row(1_000, 2, "second", RowKind.UPDATE_AFTER));
                            state = snapshot(source, kind);
                        }
                        try (Harness target = harness(false, state, targetRocks)) {
                            process(target, inputs, row(1_000, 1, "first", RowKind.UPDATE_BEFORE));
                            target.processWatermark(new Watermark(1_000));
                            assertThat(target.take()).containsExactly("-U:1:first", "+U:2:second");
                        }
                    }
                }
            }
        }
    }

    @Test
    void rocksCheckpointsReuseSstsAndRestoreBufferedRowsAndTimers() throws Exception {
        try (RootAllocator inputs = new RootAllocator(64L << 20)) {
            OperatorSubtaskState second;
            try (Harness source = harness(false, null, true)) {
                process(source, inputs, row(1_000, 2, "second", RowKind.UPDATE_AFTER));
                OperatorSubtaskState first = source.snapshot(20, 20);
                IncrementalRemoteKeyedStateHandle firstHandle = incremental(first);
                source.notifyOfCompletedCheckpoint(20);
                second = source.snapshot(21, 21);
                IncrementalRemoteKeyedStateHandle secondHandle = incremental(second);
                assertThat(secondHandle.getSharedState()).hasSameSizeAs(firstHandle.getSharedState());
                assertThat(secondHandle.getCheckpointedSize()).isLessThan(firstHandle.getCheckpointedSize());
            }
            try (Harness restored = harness(false, second, true)) {
                restored.processWatermark(new Watermark(1_000));
                assertThat(restored.take()).containsExactly("+U:2:second");
            }
        }
    }

    private static IncrementalRemoteKeyedStateHandle incremental(OperatorSubtaskState state) {
        assertThat(state.getRawKeyedState()).isEmpty();
        assertThat(state.getManagedKeyedState()).hasSize(1);
        return (IncrementalRemoteKeyedStateHandle)
                state.getManagedKeyedState().iterator().next();
    }

    private static OperatorSubtaskState snapshot(Harness harness, SnapshotKind kind) throws Exception {
        if (kind == SnapshotKind.CANONICAL) {
            return harness.snapshotWithLocalState(12, 12, SavepointType.savepoint(SavepointFormatType.CANONICAL))
                    .getJobManagerOwnedState();
        }
        CheckpointStorageLocationReference location = CheckpointStorageLocationReference.getDefault();
        CheckpointOptions options = kind == SnapshotKind.UNALIGNED
                ? CheckpointOptions.unaligned(CheckpointType.CHECKPOINT, location)
                : CheckpointOptions.alignedNoTimeout(CheckpointType.CHECKPOINT, location);
        return OperatorSnapshotFinalizer.create(
                        harness.getOperator().snapshotState(11, 11, options, new MemCheckpointStreamFactory(64 << 20)))
                .getJobManagerOwnedState();
    }

    private static Harness harness(boolean processingTime, OperatorSubtaskState state, boolean rocks) throws Exception {
        byte[] plan = StreamFusionTemporalSortPlan.create(ROW_TYPE, SORT_SPEC, processingTime);
        StreamFusionArrowTemporalSortOperator operator =
                new StreamFusionArrowTemporalSortOperator(ROW_TYPE, processingTime, plan);
        Harness harness = new Harness(operator);
        harness.setStateBackend(new StreamFusionStateBackend(
                rocks ? new EmbeddedRocksDBStateBackend(true) : new HashMapStateBackend()));
        harness.setup(ArrowRowDataBatchSerializer.INSTANCE);
        if (state != null) {
            harness.initializeState(state);
        }
        harness.open();
        return harness;
    }

    private static void process(
            KeyedOneInputStreamOperatorTestHarness<RowData, ArrowRowDataBatch, ArrowRowDataBatch> harness,
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

    private static GenericRowData row(long timestamp, int number, String payload, RowKind kind) {
        Map<StringData, Integer> map = new LinkedHashMap<>();
        map.put(StringData.fromString("one"), number);
        GenericRowData row = GenericRowData.of(
                TimestampData.fromEpochMillis(timestamp),
                number,
                StringData.fromString(payload),
                (byte) number,
                (short) number,
                (long) number,
                number % 2 == 0,
                (float) number + 0.25f,
                (double) number + 0.5d,
                new byte[] {(byte) number, 2, 3},
                new byte[] {4, (byte) number},
                DecimalData.fromUnscaledLong(number * 1_000L, 10, 3),
                number,
                number,
                TimestampData.fromEpochMillis(timestamp, 456_000),
                TimestampData.fromEpochMillis(timestamp, 456_000),
                new GenericArrayData(new StringData[] {StringData.fromString(payload), null}),
                new GenericMapData(map),
                GenericRowData.of(number, StringData.fromString(payload)),
                null);
        row.setRowKind(kind);
        return row;
    }

    private enum SnapshotKind {
        ALIGNED,
        UNALIGNED,
        CANONICAL
    }

    private static final class Harness
            extends KeyedOneInputStreamOperatorTestHarness<RowData, ArrowRowDataBatch, ArrowRowDataBatch> {
        private final List<String> captured = new ArrayList<>();

        private Harness(StreamFusionArrowTemporalSortOperator operator) throws Exception {
            super(
                    operator,
                    new ArrowBatchKeySelector(EmptyRowDataKeySelector.INSTANCE),
                    EmptyRowDataKeySelector.INSTANCE.getProducedType(),
                    1,
                    1,
                    0);
            setOutputCreator(ignored -> new CapturingOutput(captured));
        }

        private List<String> take() {
            List<String> result = List.copyOf(captured);
            captured.clear();
            return result;
        }
    }

    /** Captures borrowed Arrow output synchronously before its producer-owned release callback. */
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
                captured.add(batch.rowKind(index).shortString() + ":" + row.getInt(1) + ":" + row.getString(2));
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
        public void emitWatermark(org.apache.flink.runtime.event.WatermarkEvent watermarkEvent) {}
    }
}
