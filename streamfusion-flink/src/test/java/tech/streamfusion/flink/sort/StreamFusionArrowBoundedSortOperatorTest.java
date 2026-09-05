/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.sort;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.api.common.typeinfo.Types;
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
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.planner.plan.nodes.exec.spec.SortSpec;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.arrow.ArrowExchangeCDataBridge;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchSerializer;
import tech.streamfusion.flink.exchange.ArrowExchangeBatch;
import tech.streamfusion.flink.exchange.NativeExchangeFrame;
import tech.streamfusion.flink.exchange.NativeExchangeFrameKeySelector;
import tech.streamfusion.flink.exchange.NativeExchangePlanSerializer;
import tech.streamfusion.flink.state.StreamFusionStateBackend;

class StreamFusionArrowBoundedSortOperatorTest {
    private static final RowType ROW_TYPE =
            RowType.of(false, new LogicalType[] {new IntType(), new VarCharType()}, new String[] {"number", "label"});
    private static final SortSpec SORT_SPEC =
            SortSpec.builder().addField(0, true, true).addField(1, false, false).build();
    private static final byte[] EXCHANGE_PLAN = NativeExchangePlanSerializer.singleton(ROW_TYPE);

    @Test
    void ownsItsInternalSortAndEmitsOnlyAfterEndOfInput() {
        byte[] plan = StreamFusionBoundedSortPlan.create(ROW_TYPE, SORT_SPEC);
        StreamFusionArrowBoundedSortOperator operator =
                new StreamFusionArrowBoundedSortOperator(ROW_TYPE, plan, EXCHANGE_PLAN);

        assertThat(operator.getOperatorAttributes().isInternalSorterSupported()).isTrue();
        assertThat(operator.getOperatorAttributes().isOutputOnlyAfterEndOfStream())
                .isTrue();
    }

    @Test
    void retractionsDuplicatesAndTerminalOutputMatchFlink() throws Exception {
        try (RootAllocator inputs = new RootAllocator(64L << 20)) {
            for (boolean rocks : new boolean[] {false, true}) {
                try (Harness harness = harness(null, rocks)) {
                    process(
                            harness,
                            inputs,
                            row(2, "b", RowKind.INSERT),
                            row(1, "a", RowKind.INSERT),
                            row(1, "a", RowKind.UPDATE_AFTER),
                            row(2, "b", RowKind.DELETE));
                    assertThat(harness.take()).isEmpty();
                    harness.endInput();
                    assertThat(harness.take()).containsExactly("+I:1:a", "+I:1:a");
                }
            }
        }
    }

    @Test
    void restoresAcrossBackendsAndAlignedUnalignedAndCanonicalSnapshots() throws Exception {
        try (RootAllocator inputs = new RootAllocator(64L << 20)) {
            for (boolean sourceRocks : new boolean[] {false, true}) {
                for (boolean targetRocks : new boolean[] {false, true}) {
                    for (SnapshotKind kind : SnapshotKind.values()) {
                        if (kind != SnapshotKind.CANONICAL && sourceRocks != targetRocks) {
                            continue;
                        }
                        OperatorSubtaskState state;
                        try (Harness source = harness(null, sourceRocks)) {
                            process(source, inputs, row(3, "c", RowKind.INSERT));
                            state = snapshot(source, kind);
                        }
                        try (Harness target = harness(state, targetRocks)) {
                            process(target, inputs, row(1, "a", RowKind.INSERT));
                            target.endInput();
                            assertThat(target.take()).containsExactly("+I:1:a", "+I:3:c");
                        }
                    }
                }
            }
        }
    }

    @Test
    void rocksCheckpointsReuseUnchangedSsts() throws Exception {
        try (RootAllocator inputs = new RootAllocator(64L << 20);
                Harness source = harness(null, true)) {
            process(source, inputs, row(3, "c", RowKind.INSERT));
            OperatorSubtaskState first = source.snapshot(20, 20);
            IncrementalRemoteKeyedStateHandle firstHandle = incremental(first);
            source.notifyOfCompletedCheckpoint(20);
            OperatorSubtaskState second = source.snapshot(21, 21);
            IncrementalRemoteKeyedStateHandle secondHandle = incremental(second);
            assertThat(secondHandle.getSharedState()).hasSameSizeAs(firstHandle.getSharedState());
            assertThat(secondHandle.getCheckpointedSize()).isLessThan(firstHandle.getCheckpointedSize());
        }
    }

    @Test
    void exposesFlinkLogicalIoAndNativeStateMetrics() throws Exception {
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
        try (RootAllocator inputs = new RootAllocator(64L << 20);
                MockEnvironment environment = new MockEnvironmentBuilder()
                        .setTaskName("Bounded sort metric parity")
                        .setManagedMemorySize(64L << 20)
                        .setInputSplitProvider(new MockInputSplitProvider())
                        .setBufferSize(32 * 1024)
                        .setMaxParallelism(1)
                        .setParallelism(1)
                        .setSubtaskIndex(0)
                        .setMetricGroup(taskMetrics)
                        .build();
                Harness harness = metricHarness(environment)) {
            assertThat(metrics.get("numRecordsIn")).isInstanceOf(Counter.class);
            assertThat(metrics.get("numRecordsOut")).isInstanceOf(Counter.class);
            assertThat(metrics.get("rocksDbBackend")).isInstanceOf(Gauge.class);

            process(harness, inputs, row(2, "b", RowKind.INSERT), row(1, "a", RowKind.INSERT));
            harness.endInput();
            // One input batch plus the terminal output batch cross the native boundary.
            assertThat(((Counter) metrics.get("processedBatches")).getCount()).isEqualTo(2);
            assertThat(((Counter) metrics.get("processedRows")).getCount()).isEqualTo(2);
            assertThat(((Counter) metrics.get("stateReadBatches")).getCount()).isEqualTo(1);
            assertThat(((Counter) metrics.get("stateWriteBatches")).getCount()).isEqualTo(1);
            assertThat(((Counter) metrics.get("boundedSortRowsLoaded")).getCount())
                    .isEqualTo(2);
            assertThat(((Counter) metrics.get("boundedSortRowsCommitted")).getCount())
                    .isEqualTo(2);
            assertThat(((Counter) metrics.get("boundedSortEmittedRows")).getCount())
                    .isEqualTo(2);
            assertThat(((Gauge<?>) metrics.get("rocksDbBackend")).getValue()).isEqualTo(0);
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

    private static Harness harness(OperatorSubtaskState state, boolean rocks) throws Exception {
        byte[] plan = StreamFusionBoundedSortPlan.create(ROW_TYPE, SORT_SPEC);
        Harness harness = new Harness(new StreamFusionArrowBoundedSortOperator(ROW_TYPE, plan, EXCHANGE_PLAN));
        harness.setStateBackend(new StreamFusionStateBackend(
                rocks ? new EmbeddedRocksDBStateBackend(true) : new HashMapStateBackend()));
        harness.setup(ArrowRowDataBatchSerializer.INSTANCE);
        if (state != null) {
            harness.initializeState(state);
        }
        harness.open();
        return harness;
    }

    private static Harness metricHarness(MockEnvironment environment) throws Exception {
        byte[] plan = StreamFusionBoundedSortPlan.create(ROW_TYPE, SORT_SPEC);
        Harness harness =
                new Harness(new StreamFusionArrowBoundedSortOperator(ROW_TYPE, plan, EXCHANGE_PLAN), environment);
        harness.setStateBackend(new StreamFusionStateBackend(new HashMapStateBackend()));
        harness.setup(ArrowRowDataBatchSerializer.INSTANCE);
        harness.open();
        return harness;
    }

    private static void process(Harness harness, RootAllocator allocator, GenericRowData... rows) throws Exception {
        RowKind[] kinds =
                java.util.Arrays.stream(rows).map(GenericRowData::getRowKind).toArray(RowKind[]::new);
        try (ArrowRowDataBatch batch = ArrowRowDataBatch.transpose(List.of(rows), ROW_TYPE, allocator)
                        .withRowKinds(kinds);
                ArrowExchangeBatch.EnvelopeBatch envelope = ArrowExchangeBatch.withEnvelope(batch, ROW_TYPE)) {
            for (NativeExchangeFrame frame : ArrowExchangeCDataBridge.route(
                    EXCHANGE_PLAN,
                    envelope.batch(),
                    allocator,
                    tech.streamfusion.flink.TestingNativeMemoryManager.create())) {
                harness.processElement(new StreamRecord<>(frame));
            }
        }
    }

    private static GenericRowData row(int number, String label, RowKind kind) {
        GenericRowData row = GenericRowData.of(number, StringData.fromString(label));
        row.setRowKind(kind);
        return row;
    }

    private enum SnapshotKind {
        ALIGNED,
        UNALIGNED,
        CANONICAL
    }

    private static final class Harness
            extends KeyedOneInputStreamOperatorTestHarness<Integer, NativeExchangeFrame, ArrowRowDataBatch> {
        private final List<String> captured = new ArrayList<>();

        private Harness(StreamFusionArrowBoundedSortOperator operator) throws Exception {
            super(operator, new NativeExchangeFrameKeySelector(1), Types.INT, 1, 1, 0);
            setOutputCreator(ignored -> new CapturingOutput(captured));
        }

        private Harness(StreamFusionArrowBoundedSortOperator operator, MockEnvironment environment) throws Exception {
            super(operator, new NativeExchangeFrameKeySelector(1), Types.INT, environment);
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
                captured.add(batch.rowKind(index).shortString() + ":" + row.getInt(0) + ":" + row.getString(1));
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
