/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.match;

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
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchSerializer;
import tech.streamfusion.flink.deduplicate.ArrowBatchKeySelector;
import tech.streamfusion.flink.state.StreamFusionStateBackend;
import tech.streamfusion.proto.plan.v1.Comparison;
import tech.streamfusion.proto.plan.v1.ComparisonOperator;
import tech.streamfusion.proto.plan.v1.Expression;
import tech.streamfusion.proto.plan.v1.InputReference;
import tech.streamfusion.proto.plan.v1.StringLiteral;

class StreamFusionArrowMatchRecognizeOperatorTest {
    private static final int MAX_PARALLELISM = 128;
    private static final RowType INPUT_TYPE = RowType.of(
            new LogicalType[] {
                new VarCharType(false, VarCharType.MAX_LENGTH),
                new BigIntType(false),
                new VarCharType(false, VarCharType.MAX_LENGTH)
            },
            new String[] {"category", "id", "label"});
    private static final RowType OUTPUT_TYPE = RowType.of(
            new LogicalType[] {
                new VarCharType(false, VarCharType.MAX_LENGTH),
                new BigIntType(true),
                new BigIntType(true),
                new BigIntType(true)
            },
            new String[] {"category", "aid", "bid", "cid"});

    @Test
    void exposesLogicalFlinkIoAndNativeStateMetrics() throws Exception {
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
        RowDataKeySelector selector = selector();
        try (RootAllocator inputs = new RootAllocator(64L << 20);
                MockEnvironment environment = new MockEnvironmentBuilder()
                        .setTaskName("Match recognize metric parity")
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
                                operator(selector),
                                new ArrowBatchKeySelector(selector),
                                selector.getProducedType(),
                                environment)) {
            harness.setStateBackend(new StreamFusionStateBackend(new HashMapStateBackend()));
            harness.setup(ArrowRowDataBatchSerializer.INSTANCE);
            harness.open();

            ((Counter) metrics.get("numRecordsIn")).inc();
            ((Counter) metrics.get("numRecordsOut")).inc();
            process(harness, inputs, List.of(row("x", 1, "a"), row("x", 2, "b"), row("x", 3, "c")));
            assertThat(takeOutputCount(harness)).isEqualTo(1);
            assertThat(((Counter) metrics.get("numRecordsIn")).getCount()).isEqualTo(3L);
            assertThat(((Counter) metrics.get("numRecordsOut")).getCount()).isEqualTo(1L);
            assertThat(((Counter) metrics.get("numLateRecordsDropped")).getCount())
                    .isZero();
            assertThat(((Counter) metrics.get("processedBatches")).getCount()).isEqualTo(1L);
            assertThat(((Counter) metrics.get("processedRows")).getCount()).isEqualTo(3L);
            assertThat(((Counter) metrics.get("emittedRows")).getCount()).isEqualTo(1L);
            assertThat(((Counter) metrics.get("stateReadBatches")).getCount()).isEqualTo(1L);
            assertThat(((Counter) metrics.get("stateWriteBatches")).getCount()).isEqualTo(1L);
            assertThat(((Counter) metrics.get("matchRecognizeCompletedMatches")).getCount())
                    .isEqualTo(1L);
            assertThat(((Gauge<?>) metrics.get("rocksDbBackend")).getValue()).isEqualTo(0);
        }
    }

    @Test
    void restoresPartialMatchesAcrossBothBackendsAndCheckpointFormats() throws Exception {
        try (RootAllocator inputs = new RootAllocator(64L << 20)) {
            for (boolean sourceRocks : new boolean[] {false, true}) {
                for (boolean targetRocks : new boolean[] {false, true}) {
                    for (SnapshotKind kind : SnapshotKind.values()) {
                        if (kind != SnapshotKind.CANONICAL && sourceRocks != targetRocks) {
                            continue;
                        }
                        OperatorSubtaskState snapshot;
                        try (Harness source = harness(null, sourceRocks)) {
                            process(source.operator, inputs, List.of(row("x", 1, "a"), row("x", 2, "b")));
                            assertThat(takeOutputCount(source.operator)).isZero();
                            snapshot = snapshot(source.operator, kind);
                        }
                        try (Harness target = harness(snapshot, targetRocks)) {
                            process(target.operator, inputs, List.of(row("x", 3, "c")));
                            assertThat(takeOutputCount(target.operator)).isEqualTo(1);
                        }
                    }
                }
            }
        }
    }

    @Test
    void batchesAllTouchedPartitionsIntoOneStateReadAndWrite() throws Exception {
        try (RootAllocator inputs = new RootAllocator(64L << 20);
                Harness harness = harness(null, false)) {
            process(
                    harness.operator,
                    inputs,
                    List.of(row("x", 1, "a"), row("y", 10, "a"), row("x", 2, "b"), row("x", 3, "c")));
            assertThat(takeOutputCount(harness.operator)).isEqualTo(1);
        }
    }

    @Test
    void rocksCheckpointsReuseSstsAndRestorePartialMatches() throws Exception {
        try (RootAllocator inputs = new RootAllocator(64L << 20)) {
            OperatorSubtaskState second;
            try (Harness source = harness(null, true)) {
                process(source.operator, inputs, List.of(row("x", 1, "a"), row("x", 2, "b")));
                assertThat(takeOutputCount(source.operator)).isZero();
                OperatorSubtaskState first = source.operator.snapshot(20, 20);
                IncrementalRemoteKeyedStateHandle firstHandle = incremental(first);
                source.operator.notifyOfCompletedCheckpoint(20);
                second = source.operator.snapshot(21, 21);
                IncrementalRemoteKeyedStateHandle secondHandle = incremental(second);
                assertThat(secondHandle.getSharedState()).hasSameSizeAs(firstHandle.getSharedState());
                assertThat(secondHandle.getCheckpointedSize()).isLessThan(firstHandle.getCheckpointedSize());
            }
            try (Harness restored = harness(second, true)) {
                process(restored.operator, inputs, List.of(row("x", 3, "c")));
                assertThat(takeOutputCount(restored.operator)).isEqualTo(1);
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

    private static Harness harness(OperatorSubtaskState state, boolean rocks) throws Exception {
        RowDataKeySelector selector = selector();
        KeyedOneInputStreamOperatorTestHarness<RowData, ArrowRowDataBatch, ArrowRowDataBatch> harness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        operator(selector),
                        new ArrowBatchKeySelector(selector),
                        selector.getProducedType(),
                        MAX_PARALLELISM,
                        1,
                        0);
        harness.setStateBackend(new StreamFusionStateBackend(
                rocks ? new EmbeddedRocksDBStateBackend(true) : new HashMapStateBackend()));
        harness.setup(ArrowRowDataBatchSerializer.INSTANCE);
        if (state != null) {
            harness.initializeState(state);
        }
        harness.open();
        return new Harness(harness);
    }

    private static StreamFusionArrowMatchRecognizeOperator operator(RowDataKeySelector selector) {
        return new StreamFusionArrowMatchRecognizeOperator(
                INPUT_TYPE,
                OUTPUT_TYPE,
                new int[] {0},
                List.of("A", "B", "C"),
                List.of(condition("a"), condition("b"), condition("c")),
                new int[] {0, 1, 2},
                new int[] {1, 1, 1},
                true,
                selector);
    }

    private static Expression condition(String label) {
        return Expression.newBuilder()
                .setComparison(Comparison.newBuilder()
                        .setLeft(Expression.newBuilder()
                                .setInputReference(InputReference.newBuilder().setIndex(2)))
                        .setRight(Expression.newBuilder()
                                .setStringLiteral(StringLiteral.newBuilder().setValue(label)))
                        .setOperator(ComparisonOperator.COMPARISON_OPERATOR_EQUAL))
                .build();
    }

    private static RowDataKeySelector selector() {
        return KeySelectorUtil.getRowDataSelector(
                StreamFusionArrowMatchRecognizeOperatorTest.class.getClassLoader(),
                new int[] {0},
                InternalTypeInfo.of(INPUT_TYPE));
    }

    private static GenericRowData row(String category, long id, String label) {
        return GenericRowData.of(StringData.fromString(category), id, StringData.fromString(label));
    }

    private static void process(
            KeyedOneInputStreamOperatorTestHarness<RowData, ArrowRowDataBatch, ArrowRowDataBatch> harness,
            RootAllocator allocator,
            List<GenericRowData> rows)
            throws Exception {
        try (ArrowRowDataBatch batch = ArrowRowDataBatch.transpose(new ArrayList<>(rows), INPUT_TYPE, allocator)
                .withEnvelope(
                        rows.stream().map(ignored -> RowKind.INSERT).toArray(RowKind[]::new),
                        new boolean[rows.size()],
                        new long[rows.size()])) {
            harness.processElement(new StreamRecord<>(batch));
        }
    }

    private static int takeOutputCount(
            KeyedOneInputStreamOperatorTestHarness<RowData, ArrowRowDataBatch, ArrowRowDataBatch> harness) {
        int count = 0;
        Object value;
        while ((value = harness.getOutput().poll()) != null) {
            if (value instanceof StreamRecord) {
                @SuppressWarnings("unchecked")
                ArrowRowDataBatch batch = ((StreamRecord<ArrowRowDataBatch>) value).getValue();
                try (batch) {
                    count += batch.size();
                }
            }
        }
        return count;
    }

    private enum SnapshotKind {
        ALIGNED,
        UNALIGNED,
        CANONICAL
    }

    private static final class Harness implements AutoCloseable {
        private final KeyedOneInputStreamOperatorTestHarness<RowData, ArrowRowDataBatch, ArrowRowDataBatch> operator;

        private Harness(
                KeyedOneInputStreamOperatorTestHarness<RowData, ArrowRowDataBatch, ArrowRowDataBatch> operator) {
            this.operator = operator;
        }

        @Override
        public void close() throws Exception {
            operator.close();
        }
    }
}
