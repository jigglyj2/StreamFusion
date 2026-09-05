/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.sort;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
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
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.planner.plan.nodes.exec.spec.SortSpec;
import org.apache.flink.table.runtime.keyselector.EmptyRowDataKeySelector;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchSerializer;
import tech.streamfusion.flink.deduplicate.ArrowBatchKeySelector;
import tech.streamfusion.flink.state.StreamFusionStateBackend;

class StreamFusionArrowBoundedSortLimitOperatorTest {
    private static final int MAX_PARALLELISM = 128;
    private static final RowType ROW_TYPE =
            RowType.of(false, new LogicalType[] {new IntType(), new VarCharType()}, new String[] {"number", "label"});
    private static final SortSpec SORT_SPEC =
            SortSpec.builder().addField(0, true, true).build();

    @Test
    void preservesPhysicalRowsAndKindsAtAnEqualKeyCutoffOnBothBackends() throws Exception {
        try (RootAllocator inputs = new RootAllocator(64L << 20)) {
            for (boolean rocks : new boolean[] {false, true}) {
                try (Harness harness = harness(1, 0, null, rocks)) {
                    process(
                            harness,
                            inputs,
                            row(1, "first", RowKind.INSERT),
                            row(1, "second", RowKind.DELETE),
                            row(1, "third", RowKind.UPDATE_BEFORE),
                            row(1, "fourth", RowKind.UPDATE_AFTER));
                    assertThat(harness.take()).isEmpty();
                    harness.endInput();
                    assertThat(harness.take()).containsExactly("+I:1:first", "-D:1:second");
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
                        try (Harness source = harness(1, 0, null, sourceRocks)) {
                            process(source, inputs, row(3, "before", RowKind.UPDATE_BEFORE));
                            state = snapshot(source, kind);
                        }
                        try (Harness target = harness(1, 0, state, targetRocks)) {
                            process(target, inputs, row(1, "after", RowKind.UPDATE_AFTER));
                            target.endInput();
                            // Flink's local SortLimit exposes its max-heap iteration order. The
                            // global stage performs the final stable sort.
                            assertThat(target.take()).containsExactly("-U:3:before", "+U:1:after");
                        }
                    }
                }
            }
        }
    }

    @Test
    void redistributesAndMergesLocalCandidateKeyGroupsOneToTwoToOne() throws Exception {
        try (RootAllocator inputs = new RootAllocator(64L << 20)) {
            for (boolean rocks : new boolean[] {false, true}) {
                OperatorSubtaskState initial;
                try (Harness source = harness(1, 0, null, rocks)) {
                    process(source, inputs, row(9, "initial", RowKind.INSERT));
                    initial = source.snapshot(30, 30);
                }
                OperatorSubtaskState packaged = AbstractStreamOperatorTestHarness.repackageState(initial);
                List<OperatorSubtaskState> scaled = new ArrayList<>();
                for (int subtask = 0; subtask < 2; subtask++) {
                    OperatorSubtaskState assigned = AbstractStreamOperatorTestHarness.repartitionOperatorState(
                            packaged, MAX_PARALLELISM, 1, 2, subtask);
                    try (Harness target = harness(2, subtask, assigned, rocks)) {
                        process(target, inputs, row(subtask == 0 ? 5 : 1, "scaled-" + subtask, RowKind.INSERT));
                        scaled.add(target.snapshot(31, 31));
                    }
                }
                OperatorSubtaskState merged =
                        AbstractStreamOperatorTestHarness.repackageState(scaled.toArray(new OperatorSubtaskState[0]));
                OperatorSubtaskState assignedBack =
                        AbstractStreamOperatorTestHarness.repartitionOperatorState(merged, MAX_PARALLELISM, 2, 1, 0);
                try (Harness target = harness(1, 0, assignedBack, rocks)) {
                    target.endInput();
                    assertThat(target.take()).containsExactly("+I:5:scaled-0", "+I:1:scaled-1");
                }
            }
        }
    }

    @Test
    void rocksCheckpointsReuseUnchangedSsts() throws Exception {
        try (RootAllocator inputs = new RootAllocator(64L << 20);
                Harness source = harness(1, 0, null, true)) {
            process(source, inputs, row(3, "value", RowKind.INSERT));
            OperatorSubtaskState first = source.snapshot(40, 40);
            IncrementalRemoteKeyedStateHandle firstHandle = incremental(first);
            source.notifyOfCompletedCheckpoint(40);
            OperatorSubtaskState second = source.snapshot(41, 41);
            IncrementalRemoteKeyedStateHandle secondHandle = incremental(second);
            assertThat(secondHandle.getSharedState()).hasSameSizeAs(firstHandle.getSharedState());
            assertThat(secondHandle.getCheckpointedSize()).isLessThan(firstHandle.getCheckpointedSize());
        }
    }

    @Test
    void exposesLogicalIoAndNativeDiagnosticsWithoutFullSortSpillGauges() throws Exception {
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
                        .setTaskName("Bounded SortLimit metric parity")
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
            assertThat(metrics.get("memoryUsedSizeInBytes")).isNull();
            assertThat(metrics.get("numSpillFiles")).isNull();
            assertThat(metrics.get("spillInBytes")).isNull();

            // The task wrappers account physical messages before the operator replaces them with
            // logical Flink record counts.
            ((Counter) metrics.get("numRecordsIn")).inc();
            process(
                    harness,
                    inputs,
                    row(4, "d", RowKind.INSERT),
                    row(1, "a", RowKind.INSERT),
                    row(3, "c", RowKind.DELETE),
                    row(2, "b", RowKind.UPDATE_AFTER));
            ((Counter) metrics.get("numRecordsOut")).inc();
            harness.endInput();
            assertThat(((Counter) metrics.get("numRecordsIn")).getCount()).isEqualTo(4);
            assertThat(((Counter) metrics.get("numRecordsOut")).getCount()).isEqualTo(2);
            assertThat(((Counter) metrics.get("stateReadBatches")).getCount()).isZero();
            assertThat(((Counter) metrics.get("stateWriteBatches")).getCount()).isEqualTo(1);
            assertThat(((Counter) metrics.get("boundedSortLimitRowsLoaded")).getCount())
                    .isZero();
            assertThat(((Counter) metrics.get("boundedSortLimitRowsCommitted")).getCount())
                    .isEqualTo(2);
            assertThat(((Counter) metrics.get("boundedSortLimitComparatorCalls")).getCount())
                    .isPositive();
            assertThat(((Counter) metrics.get("boundedSortLimitEmittedRows")).getCount())
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

    private static Harness harness(int parallelism, int subtask, OperatorSubtaskState state, boolean rocks)
            throws Exception {
        byte[] plan = StreamFusionBoundedSortPlan.create(ROW_TYPE, SORT_SPEC, 0L, 2L, true);
        Harness harness =
                new Harness(new StreamFusionArrowBoundedSortLimitOperator(ROW_TYPE, plan), parallelism, subtask);
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
        byte[] plan = StreamFusionBoundedSortPlan.create(ROW_TYPE, SORT_SPEC, 0L, 2L, true);
        Harness harness = new Harness(new StreamFusionArrowBoundedSortLimitOperator(ROW_TYPE, plan), environment);
        harness.setStateBackend(new StreamFusionStateBackend(new HashMapStateBackend()));
        harness.setup(ArrowRowDataBatchSerializer.INSTANCE);
        harness.open();
        return harness;
    }

    private static void process(Harness harness, RootAllocator allocator, GenericRowData... rows) throws Exception {
        RowKind[] kinds =
                java.util.Arrays.stream(rows).map(GenericRowData::getRowKind).toArray(RowKind[]::new);
        try (ArrowRowDataBatch batch =
                ArrowRowDataBatch.transpose(List.of(rows), ROW_TYPE, allocator).withRowKinds(kinds)) {
            harness.processElement(new StreamRecord<>(batch));
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
            extends KeyedOneInputStreamOperatorTestHarness<RowData, ArrowRowDataBatch, ArrowRowDataBatch> {
        private final List<String> captured = new ArrayList<>();

        private Harness(StreamFusionArrowBoundedSortLimitOperator operator, int parallelism, int subtask)
                throws Exception {
            super(
                    operator,
                    new ArrowBatchKeySelector(EmptyRowDataKeySelector.INSTANCE),
                    EmptyRowDataKeySelector.INSTANCE.getProducedType(),
                    MAX_PARALLELISM,
                    parallelism,
                    subtask);
            setOutputCreator(ignored -> new CapturingOutput(captured));
        }

        private Harness(StreamFusionArrowBoundedSortLimitOperator operator, MockEnvironment environment)
                throws Exception {
            super(
                    operator,
                    new ArrowBatchKeySelector(EmptyRowDataKeySelector.INSTANCE),
                    EmptyRowDataKeySelector.INSTANCE.getProducedType(),
                    environment);
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
        public void emitWatermark(WatermarkEvent watermarkEvent) {}
    }
}
