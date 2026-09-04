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
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;
import org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.MultipleInputStreamOperator;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFinalizer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.KeyedMultiInputStreamOperatorTestHarness;
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

/** Runtime parity for the V2 N-input operator, including backend-neutral recovery. */
class StreamFusionArrowMultiJoinOperatorTest {
    private static final int MAX_PARALLELISM = 128;
    private static final int INPUT_COUNT = 3;
    private static final RowType INPUT_TYPE = RowType.of(
            new LogicalType[] {new BigIntType(false), new VarCharType(true, VarCharType.MAX_LENGTH)},
            new String[] {"id", "payload"});
    private static final RowType OUTPUT_TYPE = RowType.of(new LogicalType[] {
        new BigIntType(false),
        new VarCharType(true, VarCharType.MAX_LENGTH),
        new BigIntType(false),
        new VarCharType(true, VarCharType.MAX_LENGTH),
        new BigIntType(false),
        new VarCharType(true, VarCharType.MAX_LENGTH)
    });
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
                        .setTaskName("Multi-join metric parity")
                        .setManagedMemorySize(64L << 20)
                        .setInputSplitProvider(new MockInputSplitProvider())
                        .setBufferSize(32 * 1024)
                        .setMaxParallelism(MAX_PARALLELISM)
                        .setParallelism(1)
                        .setSubtaskIndex(0)
                        .setMetricGroup(taskMetrics)
                        .build();
                MetricHarness harness = new MetricHarness(factory(), environment)) {
            harness.setStateBackend(new StreamFusionStateBackend(new HashMapStateBackend()));
            harness.setup(ArrowRowDataBatchSerializer.INSTANCE);
            harness.open();

            for (int input = 0; input < INPUT_COUNT; input++) {
                ((Counter) metrics.get("numRecordsIn")).inc();
                process(harness, inputs, input, row(7, "input-" + input, RowKind.INSERT));
            }
            ((Counter) metrics.get("numRecordsOut")).inc();

            assertThat(takeOutputBatches(harness)).isOne();
            assertCounter(metrics, "numRecordsIn", 3);
            assertCounter(metrics, "numRecordsOut", 1);
            assertCounter(metrics, "processedBatches", 3);
            assertCounter(metrics, "processedRows", 3);
            assertCounter(metrics, "emittedRows", 1);
            assertCounter(metrics, "emittedInserts", 1);
            assertCounter(metrics, "emittedUpdateBefores", 0);
            assertCounter(metrics, "emittedUpdateAfters", 0);
            assertCounter(metrics, "emittedDeletes", 0);
            assertCounter(metrics, "stateReadBatches", 3);
            assertCounter(metrics, "stateWriteBatches", 3);
            assertCounter(metrics, "processingFailures", 0);
            assertCounter(metrics, "watermarksAdvanced", 0);
            for (String name : List.of(
                    "eventTimeTimersFired",
                    "processingTimeTimersFired",
                    "timersRegistered",
                    "timersDeleted",
                    "timersFired",
                    "checkpoints",
                    "alignedCheckpoints",
                    "unalignedCheckpoints",
                    "canonicalSavepoints",
                    "incrementalCheckpoints",
                    "checkpointBytes",
                    "incrementalUploadedBytes",
                    "incrementalReusedBytes",
                    "checkpointDurationNanos",
                    "checkpointFailures",
                    "restores",
                    "restoreBytes",
                    "restoreDurationNanos",
                    "restoreFailures")) {
                assertCounter(metrics, name, 0);
            }
            assertThat(((Gauge<?>) metrics.get("pendingEventTimeTimers")).getValue())
                    .isEqualTo(0L);
            assertThat(((Gauge<?>) metrics.get("pendingProcessingTimeTimers")).getValue())
                    .isEqualTo(0L);
            assertThat(((Gauge<?>) metrics.get("rocksDbBackend")).getValue()).isEqualTo(0);
        }
    }

    @Test
    void restoresAllInputsAcrossBothBackendsAndEveryCheckpointFormat() throws Exception {
        try (RootAllocator inputs = new RootAllocator(64L << 20)) {
            for (boolean sourceRocks : new boolean[] {false, true}) {
                for (boolean targetRocks : new boolean[] {false, true}) {
                    for (SnapshotKind kind : SnapshotKind.values()) {
                        if (kind != SnapshotKind.CANONICAL && sourceRocks != targetRocks) {
                            continue;
                        }
                        OperatorSubtaskState snapshot;
                        try (Harness source = harness(null, sourceRocks)) {
                            process(source, inputs, 0, row(7, "left", RowKind.INSERT));
                            process(source, inputs, 1, row(7, "middle", RowKind.INSERT));
                            assertThat(takeOutputBatches(source)).isZero();
                            snapshot = snapshot(source, kind, 12);
                        }
                        try (Harness target = harness(snapshot, targetRocks)) {
                            process(target, inputs, 2, row(7, "right", RowKind.INSERT));
                            assertThat(takeOutputBatches(target)).isOne();
                        }
                    }
                }
            }
        }
    }

    @Test
    void rocksCheckpointsReuseSstsAndRestoreAllInputs() throws Exception {
        try (RootAllocator inputs = new RootAllocator(64L << 20)) {
            OperatorSubtaskState second;
            try (Harness source = harness(null, true)) {
                process(source, inputs, 0, row(7, "left", RowKind.INSERT));
                process(source, inputs, 1, row(7, "middle", RowKind.INSERT));
                OperatorSubtaskState first = source.snapshot(20, 20);
                IncrementalRemoteKeyedStateHandle firstHandle = incremental(first);
                source.notifyOfCompletedCheckpoint(20);
                second = source.snapshot(21, 21);
                IncrementalRemoteKeyedStateHandle secondHandle = incremental(second);
                assertThat(secondHandle.getSharedState()).hasSameSizeAs(firstHandle.getSharedState());
                assertThat(secondHandle.getCheckpointedSize()).isLessThan(firstHandle.getCheckpointedSize());
            }
            try (Harness restored = harness(second, true)) {
                process(restored, inputs, 2, row(7, "right", RowKind.INSERT));
                assertThat(takeOutputBatches(restored)).isOne();
            }
        }
    }

    @Test
    void redistributesCanonicalKeyGroupsFromOneToTwoSubtasksOnBothBackends() throws Exception {
        try (RootAllocator inputs = new RootAllocator(64L << 20)) {
            RowDataKeySelector selector = commonKeySelector();
            Map<Integer, GenericRowData> rows = rowsForEverySubtask(selector);
            for (boolean rocks : new boolean[] {false, true}) {
                OperatorSubtaskState initial;
                try (Harness source = harness(1, 0, null, rocks)) {
                    for (GenericRowData row : rows.values()) {
                        process(source, inputs, 0, copy(row, "left"));
                        process(source, inputs, 1, copy(row, "middle"));
                    }
                    assertThat(takeOutputBatches(source)).isZero();
                    initial = source.snapshot(30, 30);
                }

                OperatorSubtaskState packaged = AbstractStreamOperatorTestHarness.repackageState(initial);
                for (int subtask = 0; subtask < 2; subtask++) {
                    OperatorSubtaskState assigned = AbstractStreamOperatorTestHarness.repartitionOperatorState(
                            packaged, MAX_PARALLELISM, 1, 2, subtask);
                    try (Harness scaled = harness(2, subtask, assigned, rocks)) {
                        process(scaled, inputs, 2, copy(rows.get(subtask), "right"));
                        assertThat(takeOutputBatches(scaled)).isOne();
                    }
                }
            }
        }
    }

    private static void assertCounter(InterceptingOperatorMetricGroup metrics, String name, long expected) {
        assertThat(metrics.get(name)).as(name).isInstanceOf(Counter.class);
        assertThat(((Counter) metrics.get(name)).getCount()).as(name).isEqualTo(expected);
    }

    private static IncrementalRemoteKeyedStateHandle incremental(OperatorSubtaskState state) {
        assertThat(state.getRawKeyedState()).isEmpty();
        assertThat(state.getManagedKeyedState()).hasSize(1);
        return (IncrementalRemoteKeyedStateHandle)
                state.getManagedKeyedState().iterator().next();
    }

    private static OperatorSubtaskState snapshot(Harness harness, SnapshotKind kind, long checkpointId)
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
        return OperatorSnapshotFinalizer.create(harness.nativeOperator()
                        .snapshotState(checkpointId, checkpointId, options, new MemCheckpointStreamFactory(64 << 20)))
                .getJobManagerOwnedState();
    }

    private static Harness harness(OperatorSubtaskState state, boolean rocks) throws Exception {
        return harness(1, 0, state, rocks);
    }

    private static Harness harness(int parallelism, int subtask, OperatorSubtaskState state, boolean rocks)
            throws Exception {
        Harness harness = new Harness(factory(), parallelism, subtask);
        harness.setStateBackend(new StreamFusionStateBackend(
                rocks ? new EmbeddedRocksDBStateBackend(true) : new HashMapStateBackend()));
        harness.setup(ArrowRowDataBatchSerializer.INSTANCE);
        if (state != null) {
            harness.initializeState(state);
        }
        harness.open();
        return harness;
    }

    private static StreamFusionMultiJoinOperatorFactory factory() {
        RowDataKeySelector selector = commonKeySelector();
        List<RowType> types = List.of(INPUT_TYPE, INPUT_TYPE, INPUT_TYPE);
        List<int[]> keys = List.of(new int[] {0}, new int[] {0}, new int[] {0});
        byte[] plan = StreamFusionMultiJoinPlan.create(
                types,
                keys,
                List.of(FlinkJoinType.INNER, FlinkJoinType.INNER, FlinkJoinType.INNER),
                Map.of(),
                new long[] {0, 0, 0});
        return new StreamFusionMultiJoinOperatorFactory(
                types,
                OUTPUT_TYPE,
                keys,
                plan,
                List.of(selector, selector, selector),
                List.of(Map.of(), Map.of(), Map.of()),
                List.of(EXCHANGE_PLAN, EXCHANGE_PLAN, EXCHANGE_PLAN));
    }

    private static RowDataKeySelector commonKeySelector() {
        return KeySelectorUtil.getRowDataSelector(
                StreamFusionArrowMultiJoinOperatorTest.class.getClassLoader(),
                new int[] {0},
                InternalTypeInfo.of(INPUT_TYPE));
    }

    private static Map<Integer, GenericRowData> rowsForEverySubtask(RowDataKeySelector selector) throws Exception {
        Map<Integer, GenericRowData> rows = new java.util.LinkedHashMap<>();
        for (long key = 0; rows.size() < 2; key++) {
            GenericRowData row = row(key, "seed", RowKind.INSERT);
            int subtask = KeyGroupRangeAssignment.assignKeyToParallelOperator(selector.getKey(row), MAX_PARALLELISM, 2);
            rows.putIfAbsent(subtask, row);
        }
        return rows;
    }

    private static GenericRowData copy(GenericRowData source, String payload) {
        return row(source.getLong(0), payload, RowKind.INSERT);
    }

    private static GenericRowData row(long key, String payload, RowKind kind) {
        GenericRowData row = GenericRowData.of(key, StringData.fromString(payload));
        row.setRowKind(kind);
        return row;
    }

    private static void process(Harness harness, RootAllocator allocator, int input, GenericRowData row)
            throws Exception {
        process((MultiInputProcessor) harness::processElement, allocator, input, row);
    }

    private static void process(MetricHarness harness, RootAllocator allocator, int input, GenericRowData row)
            throws Exception {
        process((MultiInputProcessor) harness::processElement, allocator, input, row);
    }

    private static void process(MultiInputProcessor processor, RootAllocator allocator, int input, GenericRowData row)
            throws Exception {
        try (ArrowRowDataBatch batch = ArrowRowDataBatch.transpose(List.of(row), INPUT_TYPE, allocator)
                        .withEnvelope(new RowKind[] {row.getRowKind()}, new boolean[] {false}, new long[] {0});
                ArrowExchangeBatch.EnvelopeBatch envelope = ArrowExchangeBatch.withEnvelope(batch, INPUT_TYPE)) {
            List<NativeExchangeFrame> frames =
                    ArrowExchangeCDataBridge.route(EXCHANGE_PLAN, envelope.batch(), allocator);
            assertThat(frames).hasSize(1);
            processor.process(input, new StreamRecord<>(frames.get(0)));
        }
    }

    private static int takeOutputBatches(AbstractStreamOperatorTestHarness<ArrowRowDataBatch> harness) {
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

    @FunctionalInterface
    private interface MultiInputProcessor {
        void process(int input, StreamRecord<NativeExchangeFrame> record) throws Exception;
    }

    private static final class Harness extends KeyedMultiInputStreamOperatorTestHarness<Integer, ArrowRowDataBatch> {
        private Harness(StreamFusionMultiJoinOperatorFactory factory, int parallelism, int subtask) throws Exception {
            super(factory, MAX_PARALLELISM, parallelism, subtask);
            config.setStateKeySerializer(Types.INT.createSerializer(executionConfig.getSerializerConfig()));
            NativeExchangeFrameKeySelector selector = new NativeExchangeFrameKeySelector(MAX_PARALLELISM);
            for (int input = 0; input < INPUT_COUNT; input++) {
                setKeySelector(input, selector);
            }
            config.serializeAllConfigs();
        }

        private StreamFusionArrowMultiJoinOperator nativeOperator() {
            return (StreamFusionArrowMultiJoinOperator) operator;
        }
    }

    private static final class MetricHarness extends AbstractStreamOperatorTestHarness<ArrowRowDataBatch> {
        private MetricHarness(StreamFusionMultiJoinOperatorFactory factory, MockEnvironment environment)
                throws Exception {
            super(factory, environment);
            config.setStateKeySerializer(Types.INT.createSerializer(executionConfig.getSerializerConfig()));
            NativeExchangeFrameKeySelector selector = new NativeExchangeFrameKeySelector(MAX_PARALLELISM);
            for (int input = 0; input < INPUT_COUNT; input++) {
                config.setStatePartitioner(input, selector);
            }
            config.serializeAllConfigs();
        }

        @SuppressWarnings("unchecked")
        private void processElement(int input, StreamRecord<NativeExchangeFrame> element) throws Exception {
            List<Input> inputs = ((MultipleInputStreamOperator<ArrowRowDataBatch>) operator).getInputs();
            Input<Object> operatorInput = inputs.get(input);
            StreamRecord<Object> raw = new StreamRecord<>(element.getValue());
            operatorInput.setKeyContextElement(raw);
            operatorInput.processElement(raw);
        }
    }
}
