/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.join;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
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
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.binary.BinarySegmentUtils;
import org.apache.flink.table.planner.plan.nodes.exec.spec.IntervalJoinSpec;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.flink.arrow.ArrowExchangeCDataBridge;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchSerializer;
import tech.streamfusion.flink.exchange.ArrowExchangeBatch;
import tech.streamfusion.flink.exchange.NativeExchangeFrame;
import tech.streamfusion.flink.exchange.NativeExchangeFrameKeySelector;
import tech.streamfusion.flink.exchange.NativeExchangePlanSerializer;
import tech.streamfusion.flink.state.StreamFusionStateBackend;

class StreamFusionArrowIntervalJoinOperatorTest {
    private static final int MAX_PARALLELISM = 128;
    private static final RowType INPUT_TYPE = RowType.of(
            new LogicalType[] {
                new BigIntType(false), new TimestampType(false, 3), new VarCharType(true, VarCharType.MAX_LENGTH)
            },
            new String[] {"id", "event_time", "payload"});
    private static final RowType OUTPUT_TYPE = RowType.of(new LogicalType[] {
        new BigIntType(false),
        new TimestampType(false, 3),
        new VarCharType(true, VarCharType.MAX_LENGTH),
        new BigIntType(true),
        new TimestampType(true, 3),
        new VarCharType(true, VarCharType.MAX_LENGTH)
    });
    private static final byte[] EXCHANGE_PLAN =
            NativeExchangePlanSerializer.hash(INPUT_TYPE, new int[] {0}, MAX_PARALLELISM);
    private static final IntervalJoinSpec.WindowBounds BOUNDS =
            new IntervalJoinSpec.WindowBounds(true, -1_000, 2_000, 1, 1);

    @Test
    void exposesLogicalFlinkIoAndCompleteIntervalStateMetrics() throws Exception {
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
                        .setTaskName("Interval join metric parity")
                        .setManagedMemorySize(64L << 20)
                        .setInputSplitProvider(new MockInputSplitProvider())
                        .setBufferSize(32 * 1024)
                        .setMaxParallelism(MAX_PARALLELISM)
                        .setParallelism(1)
                        .setSubtaskIndex(0)
                        .setMetricGroup(taskMetrics)
                        .build()) {
            NativeExchangeFrameKeySelector frameSelector = new NativeExchangeFrameKeySelector(MAX_PARALLELISM);
            try (MetricTwoInputHarness harness =
                    new MetricTwoInputHarness(operator(FlinkJoinType.INNER), frameSelector, environment)) {
                harness.setStateBackend(new StreamFusionStateBackend(new HashMapStateBackend()));
                harness.setup(ArrowRowDataBatchSerializer.INSTANCE);
                harness.open();

                ((Counter) metrics.get("numRecordsIn")).inc();
                process(harness, inputs, 0, row(7, 1_000, "left", RowKind.INSERT));
                ((Counter) metrics.get("numRecordsIn")).inc();
                ((Counter) metrics.get("numRecordsOut")).inc();
                process(harness, inputs, 1, row(7, 1_500, "right", RowKind.INSERT));
                ((Counter) metrics.get("numRecordsIn")).inc();
                ((Counter) metrics.get("numRecordsOut")).inc();
                process(harness, inputs, 1, row(7, 1_500, "right", RowKind.DELETE));

                assertThat(drainKinds(harness)).containsExactly(RowKind.INSERT, RowKind.DELETE);
                assertThat(((Counter) metrics.get("numRecordsIn")).getCount()).isEqualTo(3L);
                assertThat(((Counter) metrics.get("numRecordsOut")).getCount()).isEqualTo(2L);
                assertThat(((Counter) metrics.get("processedBatches")).getCount())
                        .isEqualTo(3L);
                assertThat(((Counter) metrics.get("processedRows")).getCount()).isEqualTo(3L);
                assertThat(((Counter) metrics.get("emittedRows")).getCount()).isEqualTo(2L);
                assertThat(((Counter) metrics.get("emittedInserts")).getCount()).isEqualTo(1L);
                assertThat(((Counter) metrics.get("emittedDeletes")).getCount()).isEqualTo(1L);
                assertThat(((Counter) metrics.get("stateReadBatches")).getCount())
                        .isEqualTo(3L);
                assertThat(((Counter) metrics.get("stateWriteBatches")).getCount())
                        .isEqualTo(3L);
                assertThat(((Counter) metrics.get("timersRegistered")).getCount())
                        .isGreaterThan(0L);
                assertThat(((Gauge<?>) metrics.get("pendingEventTimeTimers")).getValue())
                        .isInstanceOf(Long.class);
                assertThat(((Gauge<?>) metrics.get("pendingProcessingTimeTimers")).getValue())
                        .isEqualTo(0L);
                assertThat(((Gauge<?>) metrics.get("rocksDbBackend")).getValue())
                        .isEqualTo(0);
            }
        }
    }

    @Test
    void emitsBoundedMatchesAndRetractionsBeforeTheDelayedWatermark() throws Exception {
        try (RootAllocator allocator = new RootAllocator(64L << 20);
                Harness harness = harness(null, false, FlinkJoinType.INNER)) {
            process(harness.operator, allocator, 0, row(7, 1_000, "left", RowKind.INSERT));
            process(harness.operator, allocator, 1, row(7, 1_500, "right", RowKind.INSERT));
            process(harness.operator, allocator, 1, row(7, 1_500, "right", RowKind.DELETE));
            List<RowKind> kinds = drainKinds(harness.operator);
            assertThat(kinds).containsExactly(RowKind.INSERT, RowKind.DELETE);

            harness.operator.processWatermark1(new Watermark(10_000));
            harness.operator.processWatermark2(new Watermark(10_000));
            assertThat(harness.operator.getOutput().stream()
                            .filter(Watermark.class::isInstance)
                            .map(Watermark.class::cast)
                            .map(Watermark::getTimestamp))
                    .containsExactly(8_000L);
        }
    }

    @Test
    void restoresPendingOuterTimerAcrossBackendsAndCheckpointFormats() throws Exception {
        try (RootAllocator allocator = new RootAllocator(64L << 20)) {
            for (boolean sourceRocks : new boolean[] {false, true}) {
                for (boolean targetRocks : new boolean[] {false, true}) {
                    for (SnapshotKind kind : SnapshotKind.values()) {
                        if (kind != SnapshotKind.CANONICAL && sourceRocks != targetRocks) {
                            continue;
                        }
                        OperatorSubtaskState snapshot;
                        try (Harness source = harness(null, sourceRocks, FlinkJoinType.LEFT)) {
                            process(source.operator, allocator, 0, row(9, 1_000, "unmatched", RowKind.INSERT));
                            snapshot = snapshot(source.operator, kind, 42);
                        }
                        try (Harness target = harness(snapshot, targetRocks, FlinkJoinType.LEFT)) {
                            target.operator.processWatermark1(new Watermark(2_001));
                            target.operator.processWatermark2(new Watermark(2_001));
                            assertThat(drainKinds(target.operator)).containsExactly(RowKind.INSERT);
                        }
                    }
                }
            }
        }
    }

    @Test
    void rocksCheckpointsReuseSstsAndRestoreRowsAndTimers() throws Exception {
        try (RootAllocator allocator = new RootAllocator(64L << 20)) {
            OperatorSubtaskState second;
            try (Harness source = harness(null, true, FlinkJoinType.LEFT)) {
                process(source.operator, allocator, 0, row(7, 1_000, "left", RowKind.INSERT));
                OperatorSubtaskState first = source.operator.snapshot(20, 20);
                IncrementalRemoteKeyedStateHandle firstHandle = incremental(first);
                source.operator.notifyOfCompletedCheckpoint(20);
                second = source.operator.snapshot(21, 21);
                IncrementalRemoteKeyedStateHandle secondHandle = incremental(second);
                assertThat(secondHandle.getSharedState()).hasSameSizeAs(firstHandle.getSharedState());
                assertThat(secondHandle.getCheckpointedSize()).isLessThan(firstHandle.getCheckpointedSize());
            }
            try (Harness restored = harness(second, true, FlinkJoinType.LEFT)) {
                restored.operator.processWatermark1(new Watermark(2_001));
                restored.operator.processWatermark2(new Watermark(2_001));
                assertThat(drainKinds(restored.operator)).containsExactly(RowKind.INSERT);
            }
        }
    }

    @ParameterizedTest(name = "key/state type: {0}")
    @MethodSource("tech.streamfusion.flink.deduplicate.StreamFusionDeduplicateKeyTypeParityTest#keyCases")
    void acceptsEverySupportedLogicalTypeAsOpaqueKeyAndStateValue(String description, LogicalType keyType, Object key)
            throws Exception {
        RowType inputType = RowType.of(
                new LogicalType[] {keyType, new TimestampType(false, 3), new BigIntType(false)},
                new String[] {"join_key", "event_time", "payload"});
        RowType outputType = RowType.of(new LogicalType[] {
            keyType,
            new TimestampType(false, 3),
            new BigIntType(false),
            keyType,
            new TimestampType(false, 3),
            new BigIntType(false)
        });
        RowDataKeySelector selector = KeySelectorUtil.getRowDataSelector(
                getClass().getClassLoader(), new int[] {0}, InternalTypeInfo.of(inputType));
        byte[] exchangePlan = NativeExchangePlanSerializer.hash(inputType, new int[] {0}, MAX_PARALLELISM);
        StreamFusionArrowIntervalJoinOperator operator = new StreamFusionArrowIntervalJoinOperator(
                inputType,
                inputType,
                outputType,
                new int[] {0},
                new int[] {0},
                StreamFusionIntervalJoinPlan.create(
                        inputType,
                        inputType,
                        new int[] {0},
                        new int[] {0},
                        new boolean[] {true},
                        FlinkJoinType.INNER,
                        BOUNDS,
                        0),
                selector,
                selector,
                exchangePlan,
                exchangePlan,
                true,
                2_000);
        NativeExchangeFrameKeySelector frameSelector = new NativeExchangeFrameKeySelector(MAX_PARALLELISM);
        try (RootAllocator allocator = new RootAllocator(64L << 20);
                KeyedTwoInputStreamOperatorTestHarness<
                                Integer, NativeExchangeFrame, NativeExchangeFrame, ArrowRowDataBatch>
                        harness = new KeyedTwoInputStreamOperatorTestHarness<>(
                                operator, frameSelector, frameSelector, Types.INT, MAX_PARALLELISM, 1, 0)) {
            harness.setStateBackend(new StreamFusionStateBackend(new HashMapStateBackend()));
            harness.setup(ArrowRowDataBatchSerializer.INSTANCE);
            harness.open();
            GenericRowData left = GenericRowData.of(key, TimestampData.fromEpochMillis(1_000), 1L);
            GenericRowData right = GenericRowData.of(key, TimestampData.fromEpochMillis(1_500), 2L);
            process(harness, allocator, 0, left, inputType, exchangePlan, selector, keyType);
            process(harness, allocator, 1, right, inputType, exchangePlan, selector, keyType);
            right.setRowKind(RowKind.DELETE);
            process(harness, allocator, 1, right, inputType, exchangePlan, selector, keyType);
            assertThat(drainKinds(harness)).as(description).containsExactly(RowKind.INSERT, RowKind.DELETE);
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

    private static Harness harness(OperatorSubtaskState state, boolean rocks, FlinkJoinType joinType) throws Exception {
        NativeExchangeFrameKeySelector selector = new NativeExchangeFrameKeySelector(MAX_PARALLELISM);
        KeyedTwoInputStreamOperatorTestHarness<Integer, NativeExchangeFrame, NativeExchangeFrame, ArrowRowDataBatch>
                harness = new KeyedTwoInputStreamOperatorTestHarness<>(
                        operator(joinType), selector, selector, Types.INT, MAX_PARALLELISM, 1, 0);
        harness.setStateBackend(new StreamFusionStateBackend(
                rocks ? new EmbeddedRocksDBStateBackend(true) : new HashMapStateBackend()));
        harness.setup(ArrowRowDataBatchSerializer.INSTANCE);
        if (state != null) {
            harness.initializeState(state);
        }
        harness.open();
        return new Harness(harness);
    }

    private static StreamFusionArrowIntervalJoinOperator operator(FlinkJoinType joinType) {
        RowDataKeySelector selector = KeySelectorUtil.getRowDataSelector(
                StreamFusionArrowIntervalJoinOperatorTest.class.getClassLoader(),
                new int[] {0},
                InternalTypeInfo.of(INPUT_TYPE));
        return new StreamFusionArrowIntervalJoinOperator(
                INPUT_TYPE,
                INPUT_TYPE,
                OUTPUT_TYPE,
                new int[] {0},
                new int[] {0},
                StreamFusionIntervalJoinPlan.create(
                        INPUT_TYPE,
                        INPUT_TYPE,
                        new int[] {0},
                        new int[] {0},
                        new boolean[] {true},
                        joinType,
                        BOUNDS,
                        0),
                selector,
                selector,
                EXCHANGE_PLAN,
                EXCHANGE_PLAN,
                true,
                2_000);
    }

    private static GenericRowData row(long key, long time, String payload, RowKind kind) {
        GenericRowData row =
                GenericRowData.of(key, TimestampData.fromEpochMillis(time), StringData.fromString(payload));
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
            List<NativeExchangeFrame> frames =
                    ArrowExchangeCDataBridge.route(EXCHANGE_PLAN, envelope.batch(), allocator);
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
            List<NativeExchangeFrame> frames =
                    ArrowExchangeCDataBridge.route(EXCHANGE_PLAN, envelope.batch(), allocator);
            assertThat(frames).hasSize(1);
            StreamRecord<NativeExchangeFrame> record = new StreamRecord<>(frames.get(0));
            if (side == 0) {
                harness.processElement1(record);
            } else {
                harness.processElement2(record);
            }
        }
    }

    private static void process(
            KeyedTwoInputStreamOperatorTestHarness<Integer, NativeExchangeFrame, NativeExchangeFrame, ArrowRowDataBatch>
                    harness,
            RootAllocator allocator,
            int side,
            GenericRowData row,
            RowType inputType,
            byte[] exchangePlan,
            RowDataKeySelector selector,
            LogicalType keyType)
            throws Exception {
        List<byte[]> routingKeys = null;
        if (requiresPreencodedExchangeKey(keyType)) {
            BinaryRowData binary = (BinaryRowData) selector.getKey(row);
            routingKeys = List.of(
                    BinarySegmentUtils.copyToBytes(binary.getSegments(), binary.getOffset(), binary.getSizeInBytes()));
        }
        try (ArrowRowDataBatch input = ArrowRowDataBatch.transpose(List.of(row), inputType, allocator)
                        .withEnvelope(new RowKind[] {row.getRowKind()}, new boolean[] {false}, new long[] {0});
                ArrowExchangeBatch.EnvelopeBatch envelope =
                        ArrowExchangeBatch.withEnvelope(input, inputType, routingKeys)) {
            List<NativeExchangeFrame> frames =
                    ArrowExchangeCDataBridge.route(exchangePlan, envelope.batch(), allocator);
            assertThat(frames).hasSize(1);
            StreamRecord<NativeExchangeFrame> record = new StreamRecord<>(frames.get(0));
            if (side == 0) {
                harness.processElement1(record);
            } else {
                harness.processElement2(record);
            }
        }
    }

    private static boolean requiresPreencodedExchangeKey(LogicalType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case CHAR:
            case VARCHAR:
            case BINARY:
            case VARBINARY:
            case DECIMAL:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return false;
            default:
                return true;
        }
    }

    private static List<RowKind> drainKinds(
            KeyedTwoInputStreamOperatorTestHarness<Integer, NativeExchangeFrame, NativeExchangeFrame, ArrowRowDataBatch>
                    harness) {
        List<RowKind> kinds = new ArrayList<>();
        Object value;
        while ((value = harness.getOutput().poll()) != null) {
            if (value instanceof StreamRecord) {
                Object batch = ((StreamRecord<?>) value).getValue();
                if (batch instanceof ArrowRowDataBatch) {
                    ArrowRowDataBatch arrow = (ArrowRowDataBatch) batch;
                    for (int row = 0; row < arrow.size(); row++) {
                        kinds.add(arrow.rowKind(row));
                    }
                    arrow.close();
                }
            }
        }
        return kinds;
    }

    private static List<RowKind> drainKinds(MetricTwoInputHarness harness) {
        List<RowKind> kinds = new ArrayList<>();
        Object value;
        while ((value = harness.getOutput().poll()) != null) {
            if (value instanceof StreamRecord) {
                Object batch = ((StreamRecord<?>) value).getValue();
                if (batch instanceof ArrowRowDataBatch) {
                    ArrowRowDataBatch arrow = (ArrowRowDataBatch) batch;
                    for (int row = 0; row < arrow.size(); row++) {
                        kinds.add(arrow.rowKind(row));
                    }
                    arrow.close();
                }
            }
        }
        return kinds;
    }

    private enum SnapshotKind {
        ALIGNED,
        UNALIGNED,
        CANONICAL
    }

    private static final class MetricTwoInputHarness extends AbstractStreamOperatorTestHarness<ArrowRowDataBatch> {
        private final StreamFusionArrowIntervalJoinOperator twoInputOperator;

        private MetricTwoInputHarness(
                StreamFusionArrowIntervalJoinOperator operator,
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
            Object value;
            while ((value = operator.getOutput().poll()) != null) {
                if (value instanceof StreamRecord
                        && ((StreamRecord<?>) value).getValue() instanceof ArrowRowDataBatch) {
                    ((ArrowRowDataBatch) ((StreamRecord<?>) value).getValue()).close();
                }
            }
            operator.close();
        }
    }
}
