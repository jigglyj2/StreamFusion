/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.join;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.api.common.typeinfo.Types;
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
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.arrow.ArrowExchangeCDataBridge;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchSerializer;
import tech.streamfusion.flink.exchange.ArrowExchangeBatch;
import tech.streamfusion.flink.exchange.NativeExchangeFrame;
import tech.streamfusion.flink.exchange.NativeExchangeFrameKeySelector;
import tech.streamfusion.flink.exchange.NativeExchangePlanSerializer;
import tech.streamfusion.flink.state.StreamFusionStateBackend;

class StreamFusionArrowRegularJoinOperatorTest {
    private static final int MAX_PARALLELISM = 128;
    private static final RowType INPUT_TYPE = RowType.of(
            new LogicalType[] {new BigIntType(false), new VarCharType(true, VarCharType.MAX_LENGTH)},
            new String[] {"id", "payload"});
    private static final RowType OUTPUT_TYPE = RowType.of(
            new LogicalType[] {
                new BigIntType(false),
                new VarCharType(true, VarCharType.MAX_LENGTH),
                new BigIntType(false),
                new VarCharType(true, VarCharType.MAX_LENGTH)
            },
            new String[] {"left_id", "left_payload", "right_id", "right_payload"});
    private static final byte[] EXCHANGE_PLAN =
            NativeExchangePlanSerializer.hash(INPUT_TYPE, new int[] {0}, MAX_PARALLELISM);

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
        try (RootAllocator inputs = new RootAllocator(64L << 20);
                MockEnvironment environment = new MockEnvironmentBuilder()
                        .setTaskName("Regular join metric parity")
                        .setManagedMemorySize(64L << 20)
                        .setInputSplitProvider(new MockInputSplitProvider())
                        .setBufferSize(32 * 1024)
                        .setMaxParallelism(MAX_PARALLELISM)
                        .setParallelism(1)
                        .setSubtaskIndex(0)
                        .setMetricGroup(taskMetrics)
                        .build()) {
            NativeExchangeFrameKeySelector frameSelector = new NativeExchangeFrameKeySelector(MAX_PARALLELISM);
            try (MetricTwoInputHarness harness = new MetricTwoInputHarness(operator(), frameSelector, environment)) {
                harness.setStateBackend(new StreamFusionStateBackend(new HashMapStateBackend()));
                harness.setup(ArrowRowDataBatchSerializer.INSTANCE);
                harness.open();

                ((Counter) metrics.get("numRecordsIn")).inc();
                process(harness, inputs, 0, row(7, "left-1", RowKind.INSERT));
                ((Counter) metrics.get("numRecordsIn")).inc();
                process(harness, inputs, 0, row(7, "left-2", RowKind.INSERT));
                ((Counter) metrics.get("numRecordsIn")).inc();
                ((Counter) metrics.get("numRecordsOut")).inc();
                process(harness, inputs, 1, row(7, "right", RowKind.INSERT));

                assertThat(takeOutputBatches(harness)).isOne();
                assertThat(((Counter) metrics.get("numRecordsIn")).getCount()).isEqualTo(3L);
                assertThat(((Counter) metrics.get("numRecordsOut")).getCount()).isEqualTo(2L);
                assertThat(((Counter) metrics.get("processedBatches")).getCount())
                        .isEqualTo(3L);
                assertThat(((Counter) metrics.get("processedRows")).getCount()).isEqualTo(3L);
                assertThat(((Counter) metrics.get("emittedRows")).getCount()).isEqualTo(2L);
                assertThat(((Counter) metrics.get("emittedInserts")).getCount()).isEqualTo(2L);
                assertThat(((Counter) metrics.get("stateReadBatches")).getCount())
                        .isEqualTo(3L);
                assertThat(((Counter) metrics.get("stateWriteBatches")).getCount())
                        .isEqualTo(3L);
                assertThat(((Gauge<?>) metrics.get("pendingEventTimeTimers")).getValue())
                        .isEqualTo(0L);
                assertThat(((Gauge<?>) metrics.get("pendingProcessingTimeTimers")).getValue())
                        .isEqualTo(0L);
                assertThat(((Gauge<?>) metrics.get("rocksDbBackend")).getValue())
                        .isEqualTo(0);
            }
        }
    }

    @Test
    void boundedJoinExposesFlinkHashJoinMetricsAndLogicalTerminalIo() throws Exception {
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
        try (RootAllocator inputs = new RootAllocator(64L << 20);
                MockEnvironment environment = new MockEnvironmentBuilder()
                        .setTaskName("Bounded regular join metric parity")
                        .setManagedMemorySize(64L << 20)
                        .setInputSplitProvider(new MockInputSplitProvider())
                        .setBufferSize(32 * 1024)
                        .setMaxParallelism(MAX_PARALLELISM)
                        .setParallelism(1)
                        .setSubtaskIndex(0)
                        .setMetricGroup(taskMetrics)
                        .build();
                MetricTwoInputHarness harness = new MetricTwoInputHarness(
                        boundedOperator(), new NativeExchangeFrameKeySelector(MAX_PARALLELISM), environment)) {
            harness.setStateBackend(new StreamFusionStateBackend(new HashMapStateBackend()));
            harness.setup(ArrowRowDataBatchSerializer.INSTANCE);
            harness.open();

            assertThat(metrics.get("numRecordsIn")).isInstanceOf(Counter.class);
            assertThat(metrics.get("numRecordsOut")).isInstanceOf(Counter.class);
            assertThat(metrics.get("memoryUsedSizeInBytes")).isInstanceOf(Gauge.class);
            assertThat(metrics.get("numSpillFiles")).isInstanceOf(Gauge.class);
            assertThat(metrics.get("spillInBytes")).isInstanceOf(Gauge.class);
            ((Counter) metrics.get("numRecordsIn")).inc();
            process(harness, inputs, 0, row(7, "left", RowKind.INSERT));
            ((Counter) metrics.get("numRecordsIn")).inc();
            process(harness, inputs, 1, row(7, "right", RowKind.INSERT));
            assertThat(takeOutputBatches(harness)).isZero();

            harness.endInput1();
            ((Counter) metrics.get("numRecordsOut")).inc();
            harness.endInput2();
            assertThat(takeOutputBatches(harness)).isOne();
            assertThat(((Counter) metrics.get("numRecordsIn")).getCount()).isEqualTo(2L);
            assertThat(((Counter) metrics.get("numRecordsOut")).getCount()).isEqualTo(1L);
            assertThat(((Counter) metrics.get("processedBatches")).getCount()).isEqualTo(3L);
            assertThat(((Counter) metrics.get("processedRows")).getCount()).isEqualTo(2L);
            assertThat(((Counter) metrics.get("emittedRows")).getCount()).isEqualTo(1L);
            assertThat(((Counter) metrics.get("emittedInserts")).getCount()).isEqualTo(1L);
            assertThat(((Counter) metrics.get("stateReadBatches")).getCount()).isEqualTo(2L);
            assertThat(((Counter) metrics.get("stateWriteBatches")).getCount()).isEqualTo(2L);
            assertThat(((Gauge<?>) metrics.get("numSpillFiles")).getValue()).isEqualTo(0L);
            assertThat(((Gauge<?>) metrics.get("spillInBytes")).getValue()).isEqualTo(0L);
            assertThat(((Number) ((Gauge<?>) metrics.get("memoryUsedSizeInBytes")).getValue()).longValue())
                    .isGreaterThanOrEqualTo(0L);
        }
    }

    @Test
    void restoresJoinStateAcrossBothBackendsAndEveryCheckpointFormat() throws Exception {
        try (RootAllocator inputs = new RootAllocator(64L << 20)) {
            for (boolean sourceRocks : new boolean[] {false, true}) {
                for (boolean targetRocks : new boolean[] {false, true}) {
                    for (SnapshotKind kind : SnapshotKind.values()) {
                        if (kind != SnapshotKind.CANONICAL && sourceRocks != targetRocks) {
                            continue;
                        }
                        OperatorSubtaskState snapshot;
                        try (Harness source = harness(null, sourceRocks)) {
                            process(source.operator, inputs, 0, row(7, "left", RowKind.INSERT));
                            assertThat(takeOutputBatches(source.operator)).isZero();
                            snapshot = snapshot(source.operator, kind, 12);
                        }
                        try (Harness target = harness(snapshot, targetRocks)) {
                            process(target.operator, inputs, 1, row(7, "right", RowKind.INSERT));
                            assertThat(takeOutputBatches(target.operator)).isOne();
                        }
                    }
                }
            }
        }
    }

    @Test
    void streamingAndBoundedRocksCheckpointsReuseSstsAndRestoreBothSides() throws Exception {
        try (RootAllocator inputs = new RootAllocator(64L << 20)) {
            for (boolean bounded : new boolean[] {false, true}) {
                OperatorSubtaskState second;
                try (Harness source = harness(null, true, bounded)) {
                    process(source.operator, inputs, 0, row(7, "left", RowKind.INSERT));
                    OperatorSubtaskState first = source.operator.snapshot(20, 20);
                    IncrementalRemoteKeyedStateHandle firstHandle = incremental(first);
                    source.operator.notifyOfCompletedCheckpoint(20);
                    second = source.operator.snapshot(21, 21);
                    IncrementalRemoteKeyedStateHandle secondHandle = incremental(second);
                    assertThat(secondHandle.getSharedState()).hasSameSizeAs(firstHandle.getSharedState());
                    assertThat(secondHandle.getCheckpointedSize()).isLessThan(firstHandle.getCheckpointedSize());
                }
                try (Harness restored = harness(second, true, bounded)) {
                    process(restored.operator, inputs, 1, row(7, "right", RowKind.INSERT));
                    if (bounded) {
                        assertThat(takeOutputBatches(restored.operator)).isZero();
                        restored.operator.endInput1();
                        restored.operator.endInput2();
                    }
                    assertThat(takeOutputBatches(restored.operator)).isOne();
                }
            }
        }
    }

    @Test
    void boundedJoinRestoresTerminalResultsAcrossBothBackendsAndEveryCheckpointFormat() throws Exception {
        try (RootAllocator inputs = new RootAllocator(64L << 20)) {
            for (boolean sourceRocks : new boolean[] {false, true}) {
                for (boolean targetRocks : new boolean[] {false, true}) {
                    for (SnapshotKind kind : SnapshotKind.values()) {
                        if (kind != SnapshotKind.CANONICAL && sourceRocks != targetRocks) {
                            continue;
                        }
                        OperatorSubtaskState snapshot;
                        try (Harness source = harness(null, sourceRocks, true)) {
                            process(source.operator, inputs, 0, row(7, "left", RowKind.INSERT));
                            assertThat(takeOutputBatches(source.operator)).isZero();
                            snapshot = snapshot(source.operator, kind, 30);
                        }
                        try (Harness target = harness(snapshot, targetRocks, true)) {
                            process(target.operator, inputs, 1, row(7, "right", RowKind.INSERT));
                            assertThat(takeOutputBatches(target.operator)).isZero();
                            target.operator.endInput1();
                            assertThat(takeOutputBatches(target.operator)).isZero();
                            target.operator.endInput2();
                            assertThat(takeOutputBatches(target.operator)).isOne();
                        }
                    }
                }
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
            KeyedTwoInputStreamOperatorTestHarness<Integer, NativeExchangeFrame, NativeExchangeFrame, ArrowRowDataBatch>
                    harness,
            SnapshotKind kind,
            long checkpointId)
            throws Exception {
        if (kind == SnapshotKind.CANONICAL) {
            return harness.snapshotWithLocalState(
                            checkpointId, checkpointId, SavepointType.savepoint(SavepointFormatType.CANONICAL))
                    .getJobManagerOwnedState();
        }
        CheckpointStorageLocationReference location = CheckpointStorageLocationReference.getDefault();
        CheckpointOptions options = kind == SnapshotKind.UNALIGNED
                ? CheckpointOptions.unaligned(CheckpointType.CHECKPOINT, location)
                : CheckpointOptions.alignedNoTimeout(CheckpointType.CHECKPOINT, location);
        return OperatorSnapshotFinalizer.create(harness.getOperator()
                        .snapshotState(checkpointId, checkpointId, options, new MemCheckpointStreamFactory(64 << 20)))
                .getJobManagerOwnedState();
    }

    private static Harness harness(OperatorSubtaskState state, boolean rocks) throws Exception {
        return harness(state, rocks, false);
    }

    private static Harness harness(OperatorSubtaskState state, boolean rocks, boolean bounded) throws Exception {
        NativeExchangeFrameKeySelector frameSelector = new NativeExchangeFrameKeySelector(MAX_PARALLELISM);
        KeyedTwoInputStreamOperatorTestHarness<Integer, NativeExchangeFrame, NativeExchangeFrame, ArrowRowDataBatch>
                harness = new KeyedTwoInputStreamOperatorTestHarness<>(
                        bounded ? boundedOperator() : operator(),
                        frameSelector,
                        frameSelector,
                        Types.INT,
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

    private static StreamFusionArrowRegularJoinOperator operator() {
        return operator(false);
    }

    private static StreamFusionArrowRegularJoinOperator boundedOperator() {
        return operator(true);
    }

    private static StreamFusionArrowRegularJoinOperator operator(boolean bounded) {
        RowDataKeySelector selector = KeySelectorUtil.getRowDataSelector(
                StreamFusionArrowRegularJoinOperatorTest.class.getClassLoader(),
                new int[] {0},
                InternalTypeInfo.of(INPUT_TYPE));
        return new StreamFusionArrowRegularJoinOperator(
                INPUT_TYPE,
                INPUT_TYPE,
                OUTPUT_TYPE,
                new int[] {0},
                new int[] {0},
                bounded
                        ? StreamFusionRegularJoinPlan.createBounded(
                                INPUT_TYPE,
                                INPUT_TYPE,
                                new int[] {0},
                                new int[] {0},
                                new boolean[] {true},
                                FlinkJoinType.INNER,
                                null)
                        : StreamFusionRegularJoinPlan.create(
                                INPUT_TYPE,
                                INPUT_TYPE,
                                new int[] {0},
                                new int[] {0},
                                new boolean[] {true},
                                FlinkJoinType.INNER,
                                null),
                selector,
                selector,
                EXCHANGE_PLAN,
                EXCHANGE_PLAN,
                bounded);
    }

    private static GenericRowData row(long key, String payload, RowKind kind) {
        GenericRowData row = GenericRowData.of(key, StringData.fromString(payload));
        row.setRowKind(kind);
        return row;
    }

    private static void process(
            KeyedTwoInputStreamOperatorTestHarness<Integer, NativeExchangeFrame, NativeExchangeFrame, ArrowRowDataBatch>
                    harness,
            RootAllocator allocator,
            int side,
            GenericRowData row)
            throws Exception {
        try (ArrowRowDataBatch input = ArrowRowDataBatch.transpose(List.of(row), INPUT_TYPE, allocator)
                        .withEnvelope(new RowKind[] {row.getRowKind()}, new boolean[] {false}, new long[] {0});
                ArrowExchangeBatch.EnvelopeBatch envelope = ArrowExchangeBatch.withEnvelope(input, INPUT_TYPE)) {
            List<NativeExchangeFrame> frames = ArrowExchangeCDataBridge.route(
                    EXCHANGE_PLAN,
                    envelope.batch(),
                    allocator,
                    tech.streamfusion.flink.TestingNativeMemoryManager.create());
            assertThat(frames).hasSize(1);
            StreamRecord<NativeExchangeFrame> record = new StreamRecord<>(frames.get(0));
            if (side == 0) {
                harness.processElement1(record);
            } else {
                harness.processElement2(record);
            }
        }
    }

    private static void process(MetricTwoInputHarness harness, RootAllocator allocator, int side, GenericRowData row)
            throws Exception {
        try (ArrowRowDataBatch input = ArrowRowDataBatch.transpose(List.of(row), INPUT_TYPE, allocator)
                        .withEnvelope(new RowKind[] {row.getRowKind()}, new boolean[] {false}, new long[] {0});
                ArrowExchangeBatch.EnvelopeBatch envelope = ArrowExchangeBatch.withEnvelope(input, INPUT_TYPE)) {
            List<NativeExchangeFrame> frames = ArrowExchangeCDataBridge.route(
                    EXCHANGE_PLAN,
                    envelope.batch(),
                    allocator,
                    tech.streamfusion.flink.TestingNativeMemoryManager.create());
            assertThat(frames).hasSize(1);
            StreamRecord<NativeExchangeFrame> record = new StreamRecord<>(frames.get(0));
            if (side == 0) {
                harness.processElement1(record);
            } else {
                harness.processElement2(record);
            }
        }
    }

    private static int takeOutputBatches(
            KeyedTwoInputStreamOperatorTestHarness<Integer, NativeExchangeFrame, NativeExchangeFrame, ArrowRowDataBatch>
                    harness) {
        int batches = 0;
        Object value;
        while ((value = harness.getOutput().poll()) != null) {
            if (value instanceof StreamRecord) {
                Object batch = ((StreamRecord<?>) value).getValue();
                if (batch instanceof ArrowRowDataBatch) {
                    ((ArrowRowDataBatch) batch).close();
                }
                batches++;
            }
        }
        return batches;
    }

    private static int takeOutputBatches(MetricTwoInputHarness harness) {
        int batches = 0;
        Object value;
        while ((value = harness.getOutput().poll()) != null) {
            if (value instanceof StreamRecord) {
                Object batch = ((StreamRecord<?>) value).getValue();
                if (batch instanceof ArrowRowDataBatch) {
                    ((ArrowRowDataBatch) batch).close();
                }
                batches++;
            }
        }
        return batches;
    }

    private enum SnapshotKind {
        ALIGNED,
        UNALIGNED,
        CANONICAL
    }

    private static final class MetricTwoInputHarness extends AbstractStreamOperatorTestHarness<ArrowRowDataBatch> {
        private final StreamFusionArrowRegularJoinOperator twoInputOperator;

        private MetricTwoInputHarness(
                StreamFusionArrowRegularJoinOperator operator,
                NativeExchangeFrameKeySelector selector,
                MockEnvironment environment)
                throws Exception {
            super(operator, environment);
            this.twoInputOperator = operator;
            config.setStatePartitioner(0, selector);
            config.setStatePartitioner(1, selector);
            config.setStateKeySerializer(Types.INT.createSerializer(executionConfig.getSerializerConfig()));
            config.serializeAllConfigs();
        }

        private void processElement1(StreamRecord<NativeExchangeFrame> element) throws Exception {
            twoInputOperator.setKeyContextElement1(element);
            twoInputOperator.processElement1(element);
        }

        private void processElement2(StreamRecord<NativeExchangeFrame> element) throws Exception {
            twoInputOperator.setKeyContextElement2(element);
            twoInputOperator.processElement2(element);
        }

        private void endInput1() throws Exception {
            twoInputOperator.endInput(1);
        }

        private void endInput2() throws Exception {
            twoInputOperator.endInput(2);
        }
    }

    private static final class Harness implements AutoCloseable {
        private final KeyedTwoInputStreamOperatorTestHarness<
                        Integer, NativeExchangeFrame, NativeExchangeFrame, ArrowRowDataBatch>
                operator;

        private Harness(
                KeyedTwoInputStreamOperatorTestHarness<
                                Integer, NativeExchangeFrame, NativeExchangeFrame, ArrowRowDataBatch>
                        operator) {
            this.operator = operator;
        }

        @Override
        public void close() throws Exception {
            operator.close();
        }
    }
}
