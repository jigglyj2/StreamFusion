/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.window;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Meter;
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
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;
import org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFinalizer;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.planner.plan.nodes.exec.spec.SortSpec;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchSerializer;
import tech.streamfusion.flink.deduplicate.ArrowBatchKeySelector;
import tech.streamfusion.flink.state.StreamFusionStateBackend;

class StreamFusionArrowWindowRankOperatorTest {
    private static final int MAX_PARALLELISM = 128;
    private static final RowType INPUT_TYPE = RowType.of(
            new LogicalType[] {
                new BigIntType(false),
                new BigIntType(false),
                new TimestampType(false, 3),
                new VarCharType(false, VarCharType.MAX_LENGTH)
            },
            new String[] {"key", "score", "window_end", "payload"});
    private static final RowType OUTPUT_TYPE = RowType.of(
            new LogicalType[] {
                new BigIntType(false),
                new BigIntType(false),
                new TimestampType(false, 3),
                new VarCharType(false, VarCharType.MAX_LENGTH),
                new BigIntType(false)
            },
            new String[] {"key", "score", "window_end", "payload", "row_num"});

    @Test
    void exposesFlinkWindowMetricsWithLogicalRecordSemantics() throws Exception {
        InterceptingOperatorMetricGroup metrics = new InterceptingOperatorMetricGroup();
        InterceptingTaskMetricGroup taskMetrics = new InterceptingTaskMetricGroup() {
            @Override
            public InternalOperatorMetricGroup getOrAddOperator(
                    OperatorID id, String name, Map<String, String> additionalVariables) {
                return metrics;
            }
        };
        RowDataKeySelector selector = selector();
        StreamFusionArrowWindowRankOperator operator = operator(selector);
        try (RootAllocator inputs = new RootAllocator(64L << 20);
                MockEnvironment environment = new MockEnvironmentBuilder()
                        .setTaskName("Window rank metric parity")
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
            harness.setStateBackend(new StreamFusionStateBackend(new HashMapStateBackend()));
            harness.setup(ArrowRowDataBatchSerializer.INSTANCE);
            harness.open();

            assertThat(metrics.get("numLateRecordsDropped")).isInstanceOf(Counter.class);
            assertThat(metrics.get("lateRecordsDroppedRate")).isInstanceOf(Meter.class);
            assertThat(metrics.get("pendingEventTimeTimers")).isInstanceOf(Gauge.class);
            assertThat(metrics.get("pendingProcessingTimeTimers")).isInstanceOf(Gauge.class);
            assertThat(metrics.get("watermarkLatency")).isInstanceOf(Gauge.class);

            ((Counter) metrics.get("numRecordsIn")).inc();
            process(harness, inputs, row(7, 10, 10_000, "winner", RowKind.INSERT));
            assertThat(((Gauge<?>) metrics.get("pendingEventTimeTimers")).getValue())
                    .isEqualTo(1L);
            assertThat(((Counter) metrics.get("numRecordsIn")).getCount()).isEqualTo(1L);

            harness.setProcessingTime(12_000);
            ((Counter) metrics.get("numRecordsOut")).inc();
            harness.processWatermark(new Watermark(9_999));
            assertThat(((Gauge<?>) metrics.get("pendingEventTimeTimers")).getValue())
                    .isEqualTo(0L);
            assertThat(((Gauge<?>) metrics.get("pendingProcessingTimeTimers")).getValue())
                    .isEqualTo(0L);
            assertThat(((Gauge<?>) metrics.get("watermarkLatency")).getValue()).isEqualTo(2_001L);
            assertThat(((Counter) metrics.get("numRecordsOut")).getCount()).isEqualTo(1L);

            ((Counter) metrics.get("numRecordsIn")).inc();
            process(harness, inputs, row(8, 10, 10_000, "late", RowKind.INSERT));
            assertThat(((Counter) metrics.get("numLateRecordsDropped")).getCount())
                    .isEqualTo(1L);
        }
    }

    @Test
    void retractsAndRestoresAcrossEveryBackendAndCheckpointFormat() throws Exception {
        try (RootAllocator inputs = new RootAllocator(64L << 20)) {
            for (boolean sourceRocks : new boolean[] {false, true}) {
                for (boolean targetRocks : new boolean[] {false, true}) {
                    for (SnapshotKind kind : SnapshotKind.values()) {
                        if (kind != SnapshotKind.CANONICAL && sourceRocks != targetRocks) {
                            continue;
                        }
                        OperatorSubtaskState snapshot;
                        try (Harness source = harness(1, 0, null, sourceRocks)) {
                            process(source.harness, inputs, row(7, 10, 10_000, "first", RowKind.INSERT));
                            process(source.harness, inputs, row(7, 20, 10_000, "second", RowKind.INSERT));
                            snapshot = snapshot(source.harness, kind);
                        }
                        try (Harness target = harness(1, 0, snapshot, targetRocks)) {
                            process(target.harness, inputs, row(7, 10, 10_000, "first", RowKind.DELETE));
                            target.harness.processWatermark(new Watermark(9_999));
                            assertThat(takeOutputRows(target.harness)).isEqualTo(1);
                        }
                    }
                }
            }
        }
    }

    @Test
    void redistributesArrowRowStateAndTimersOneToTwoAndBack() throws Exception {
        try (RootAllocator inputs = new RootAllocator(64L << 20)) {
            RowDataKeySelector selector = selector();
            Map<Integer, GenericRowData> rows = rowsForOwners(selector, 2);
            for (boolean rocks : new boolean[] {false, true}) {
                OperatorSubtaskState one;
                try (Harness initial = harness(1, 0, null, rocks)) {
                    for (GenericRowData row : rows.values()) {
                        process(initial.harness, inputs, row);
                    }
                    one = initial.harness.snapshot(30, 30);
                }
                OperatorSubtaskState packaged = AbstractStreamOperatorTestHarness.repackageState(one);
                List<OperatorSubtaskState> two = new ArrayList<>();
                int firstWindowRows = 0;
                for (int subtask = 0; subtask < 2; subtask++) {
                    OperatorSubtaskState assigned = AbstractStreamOperatorTestHarness.repartitionOperatorState(
                            packaged, MAX_PARALLELISM, 1, 2, subtask);
                    try (Harness scaled = harness(2, subtask, assigned, rocks)) {
                        scaled.harness.processWatermark(new Watermark(9_999));
                        firstWindowRows += takeOutputRows(scaled.harness);
                        long key = rows.get(subtask).getLong(0);
                        process(scaled.harness, inputs, row(key, 10, 20_000, "second-" + key, RowKind.INSERT));
                        two.add(scaled.harness.snapshot(31, 31));
                    }
                }
                assertThat(firstWindowRows).isEqualTo(2);

                OperatorSubtaskState packagedTwo =
                        AbstractStreamOperatorTestHarness.repackageState(two.toArray(new OperatorSubtaskState[0]));
                OperatorSubtaskState assignedBack = AbstractStreamOperatorTestHarness.repartitionOperatorState(
                        packagedTwo, MAX_PARALLELISM, 2, 1, 0);
                try (Harness back = harness(1, 0, assignedBack, rocks)) {
                    back.harness.processWatermark(new Watermark(19_999));
                    assertThat(takeOutputRows(back.harness)).isEqualTo(2);
                }
            }
        }
    }

    @Test
    void rocksCheckpointsReuseSstsAndRestoreArrowRowState() throws Exception {
        try (RootAllocator inputs = new RootAllocator(64L << 20)) {
            OperatorSubtaskState second;
            try (Harness source = harness(1, 0, null, true)) {
                process(source.harness, inputs, row(7, 10, 10_000, "winner", RowKind.INSERT));
                OperatorSubtaskState first = source.harness.snapshot(20, 20);
                IncrementalRemoteKeyedStateHandle firstHandle = incremental(first);
                source.harness.notifyOfCompletedCheckpoint(20);
                second = source.harness.snapshot(21, 21);
                IncrementalRemoteKeyedStateHandle secondHandle = incremental(second);
                assertThat(secondHandle.getSharedState()).hasSameSizeAs(firstHandle.getSharedState());
                assertThat(secondHandle.getCheckpointedSize()).isLessThan(firstHandle.getCheckpointedSize());
            }
            try (Harness restored = harness(1, 0, second, true)) {
                restored.harness.processWatermark(new Watermark(9_999));
                assertThat(takeOutputRows(restored.harness)).isEqualTo(1);
            }
        }
    }

    private static IncrementalRemoteKeyedStateHandle incremental(OperatorSubtaskState state) {
        assertThat(state.getRawKeyedState()).isEmpty();
        assertThat(state.getManagedKeyedState()).hasSize(1);
        return (IncrementalRemoteKeyedStateHandle)
                state.getManagedKeyedState().iterator().next();
    }

    private static OperatorSubtaskState snapshot(
            KeyedOneInputStreamOperatorTestHarness<RowData, ArrowRowDataBatch, ArrowRowDataBatch> harness,
            SnapshotKind kind)
            throws Exception {
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
        RowDataKeySelector selector = selector();
        KeyedOneInputStreamOperatorTestHarness<RowData, ArrowRowDataBatch, ArrowRowDataBatch> harness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        operator(selector),
                        new ArrowBatchKeySelector(selector),
                        selector.getProducedType(),
                        MAX_PARALLELISM,
                        parallelism,
                        subtask);
        harness.setStateBackend(new StreamFusionStateBackend(
                rocks ? new EmbeddedRocksDBStateBackend(true) : new HashMapStateBackend()));
        harness.setup(ArrowRowDataBatchSerializer.INSTANCE);
        if (state != null) {
            harness.initializeState(state);
        }
        harness.open();
        return new Harness(harness);
    }

    private static StreamFusionArrowWindowRankOperator operator(RowDataKeySelector selector) {
        SortSpec sort = SortSpec.builder().addField(1, true, true).build();
        byte[] plan = StreamFusionWindowRankPlan.create(INPUT_TYPE, new int[] {0}, sort, 2, 1, 1, true, "UTC");
        return new StreamFusionArrowWindowRankOperator(INPUT_TYPE, OUTPUT_TYPE, new int[] {0}, plan, selector);
    }

    private static RowDataKeySelector selector() {
        return KeySelectorUtil.getRowDataSelector(
                StreamFusionArrowWindowRankOperatorTest.class.getClassLoader(),
                new int[] {0},
                InternalTypeInfo.of(INPUT_TYPE));
    }

    private static Map<Integer, GenericRowData> rowsForOwners(RowDataKeySelector selector, int parallelism)
            throws Exception {
        Map<Integer, GenericRowData> rows = new HashMap<>();
        for (long key = 0; rows.size() < parallelism; key++) {
            GenericRowData row = row(key, 10, 10_000, "owner-" + key, RowKind.INSERT);
            int owner = KeyGroupRangeAssignment.assignKeyToParallelOperator(
                    selector.getKey(row), MAX_PARALLELISM, parallelism);
            rows.putIfAbsent(owner, row);
        }
        return rows;
    }

    private static GenericRowData row(long key, long score, long windowEnd, String payload, RowKind kind) {
        GenericRowData row =
                GenericRowData.of(key, score, TimestampData.fromEpochMillis(windowEnd), StringData.fromString(payload));
        row.setRowKind(kind);
        return row;
    }

    private static void process(
            KeyedOneInputStreamOperatorTestHarness<RowData, ArrowRowDataBatch, ArrowRowDataBatch> harness,
            RootAllocator allocator,
            GenericRowData row)
            throws Exception {
        try (ArrowRowDataBatch batch = ArrowRowDataBatch.transpose(List.of(row), INPUT_TYPE, allocator)
                .withEnvelope(new RowKind[] {row.getRowKind()}, new boolean[] {false}, new long[] {0})) {
            harness.processElement(new StreamRecord<>(batch));
        }
    }

    private static int takeOutputRows(
            KeyedOneInputStreamOperatorTestHarness<RowData, ArrowRowDataBatch, ArrowRowDataBatch> harness) {
        int rows = 0;
        Object value;
        while ((value = harness.getOutput().poll()) != null) {
            if (value instanceof StreamRecord) {
                @SuppressWarnings("unchecked")
                ArrowRowDataBatch batch = ((StreamRecord<ArrowRowDataBatch>) value).getValue();
                rows += batch.size();
            }
        }
        return rows;
    }

    private enum SnapshotKind {
        ALIGNED,
        UNALIGNED,
        CANONICAL
    }

    private static final class Harness implements AutoCloseable {
        private final KeyedOneInputStreamOperatorTestHarness<RowData, ArrowRowDataBatch, ArrowRowDataBatch> harness;

        private Harness(KeyedOneInputStreamOperatorTestHarness<RowData, ArrowRowDataBatch, ArrowRowDataBatch> harness) {
            this.harness = harness;
        }

        @Override
        public void close() throws Exception {
            harness.close();
        }
    }
}
