/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.topn;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
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
import org.apache.flink.runtime.checkpoint.metadata.MetadataV3Serializer;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.metrics.groups.InternalOperatorMetricGroup;
import org.apache.flink.runtime.metrics.util.InterceptingOperatorMetricGroup;
import org.apache.flink.runtime.metrics.util.InterceptingTaskMetricGroup;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle.HandleAndLocalPath;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;
import org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFinalizer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchSerializer;
import tech.streamfusion.flink.deduplicate.ArrowBatchKeySelector;
import tech.streamfusion.flink.state.StreamFusionStateBackend;
import tech.streamfusion.nativebridge.NativeTopNBridge;
import tech.streamfusion.proto.plan.v1.EmptyType;
import tech.streamfusion.proto.plan.v1.Input;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;
import tech.streamfusion.proto.plan.v1.Schema;
import tech.streamfusion.proto.plan.v1.TopN;
import tech.streamfusion.proto.plan.v1.TopNStrategy;

class StreamFusionArrowTopNOperatorTest {
    private static final int MAX_PARALLELISM = 128;
    private static final RowType INPUT_TYPE = RowType.of(
            new LogicalType[] {new BigIntType(false), new BigIntType(false)}, new String[] {"partition_key", "score"});
    private static final RowType OUTPUT_TYPE = RowType.of(
            new LogicalType[] {new BigIntType(false), new BigIntType(false), new BigIntType(false)},
            new String[] {"partition_key", "score", "row_num"});

    @Test
    void exposesTheCompleteFlinkRankMetricSurfaceAndValuesForEachStrategy() throws Exception {
        for (StreamFusionTopNStrategy strategy : StreamFusionTopNStrategy.values()) {
            InterceptingOperatorMetricGroup metrics = new InterceptingOperatorMetricGroup();
            InterceptingTaskMetricGroup taskMetrics = new InterceptingTaskMetricGroup() {
                @Override
                public InternalOperatorMetricGroup getOrAddOperator(
                        OperatorID id, String name, Map<String, String> additionalVariables) {
                    return metrics;
                }
            };
            RowDataKeySelector partition = partitionSelector();
            StreamFusionArrowTopNOperator operator = new StreamFusionArrowTopNOperator(
                    INPUT_TYPE, OUTPUT_TYPE, new int[] {0}, plan(strategy), partition, strategy, 12_345L);
            try (MockEnvironment environment = new MockEnvironmentBuilder()
                            .setTaskName("Top-N metric parity")
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
                                    new ArrowBatchKeySelector(partition),
                                    partition.getProducedType(),
                                    environment)) {
                harness.setStateBackend(new StreamFusionStateBackend(new HashMapStateBackend()));
                harness.setup(ArrowRowDataBatchSerializer.INSTANCE);
                harness.open();

                assertThat(metrics.get("topn.invalidTopSize")).isInstanceOf(Counter.class);
                if (strategy == StreamFusionTopNStrategy.RETRACT) {
                    assertThat(metrics.get("topn.cache.hitRate")).isNull();
                    assertThat(metrics.get("topn.cache.size")).isNull();
                } else {
                    assertThat(((Gauge<?>) metrics.get("topn.cache.hitRate")).getValue())
                            .isEqualTo(1.0D);
                    assertThat(((Gauge<?>) metrics.get("topn.cache.size")).getValue())
                            .isEqualTo(strategy == StreamFusionTopNStrategy.UPDATE_FAST ? 12_345L : 0L);
                }
            }
        }
    }

    @Test
    void restoresAlignedAndUnalignedCheckpointsOnBothBackends() throws Exception {
        try (RootAllocator inputs = new RootAllocator(64L << 20)) {
            for (boolean rocksDb : new boolean[] {false, true}) {
                for (boolean unaligned : new boolean[] {false, true}) {
                    OperatorSubtaskState snapshot;
                    try (KeyedOneInputStreamOperatorTestHarness<RowData, ArrowRowDataBatch, ArrowRowDataBatch> before =
                            harness(1, 0, null, rocksDb)) {
                        process(before, inputs, row(7, 10));
                        process(before, inputs, row(7, 20));
                        takeKinds(before);
                        snapshot = snapshot(before, unaligned ? 2 : 1, unaligned);
                    }
                    try (KeyedOneInputStreamOperatorTestHarness<RowData, ArrowRowDataBatch, ArrowRowDataBatch> after =
                            harness(1, 0, snapshot, rocksDb)) {
                        process(after, inputs, row(7, 30));
                        assertThat(takeKinds(after)).containsExactly(RowKind.UPDATE_AFTER, RowKind.UPDATE_AFTER);
                    }
                }
            }
        }
    }

    @Test
    void saturatedGlobalLimitSkipsNativeTransferAndRevalidatesAfterRestore() throws Exception {
        try (RootAllocator inputs = new RootAllocator(64L << 20)) {
            for (boolean rocksDb : new boolean[] {false, true}) {
                for (boolean unaligned : new boolean[] {false, true}) {
                    NativeTopNBridge.resetMetrics();
                    OperatorSubtaskState snapshot;
                    try (KeyedOneInputStreamOperatorTestHarness<RowData, ArrowRowDataBatch, ArrowRowDataBatch> before =
                            limitHarness(null, rocksDb)) {
                        processBatch(before, inputs, List.of(row(1, 10), row(2, 20), row(3, 30), row(4, 40)));
                        assertThat(takeKinds(before)).containsExactly(RowKind.INSERT, RowKind.INSERT);
                        assertThat(NativeTopNBridge.executedBatchCount()).isEqualTo(1);

                        process(before, inputs, row(5, 50));
                        assertThat(takeKinds(before)).isEmpty();
                        assertThat(NativeTopNBridge.executedBatchCount()).isEqualTo(1);
                        snapshot = snapshot(before, unaligned ? 21 : 20, unaligned);
                    }
                    try (KeyedOneInputStreamOperatorTestHarness<RowData, ArrowRowDataBatch, ArrowRowDataBatch> after =
                            limitHarness(snapshot, rocksDb)) {
                        process(after, inputs, row(6, 60));
                        assertThat(takeKinds(after)).isEmpty();
                        assertThat(NativeTopNBridge.executedBatchCount()).isEqualTo(2);
                        process(after, inputs, row(7, 70));
                        assertThat(takeKinds(after)).isEmpty();
                        assertThat(NativeTopNBridge.executedBatchCount()).isEqualTo(2);
                    }
                }
            }
        }
    }

    @Test
    void globalLimitCanonicalSavepointsRestoreAcrossEveryBackendPair() throws Exception {
        try (RootAllocator inputs = new RootAllocator(64L << 20)) {
            for (boolean sourceRocks : new boolean[] {false, true}) {
                for (boolean targetRocks : new boolean[] {false, true}) {
                    OperatorSubtaskState savepoint;
                    try (KeyedOneInputStreamOperatorTestHarness<RowData, ArrowRowDataBatch, ArrowRowDataBatch> source =
                            limitHarness(null, sourceRocks)) {
                        processBatch(source, inputs, List.of(row(1, 10), row(2, 20), row(3, 30), row(4, 40)));
                        takeKinds(source);
                        savepoint = source.snapshotWithLocalState(
                                        22, 22, SavepointType.savepoint(SavepointFormatType.CANONICAL))
                                .getJobManagerOwnedState();
                        assertThat(savepoint.getRawKeyedState()).hasSize(1);
                    }
                    try (KeyedOneInputStreamOperatorTestHarness<RowData, ArrowRowDataBatch, ArrowRowDataBatch> target =
                            limitHarness(savepoint, targetRocks)) {
                        process(target, inputs, row(5, 50));
                        assertThat(takeKinds(target)).isEmpty();
                    }
                }
            }
        }
    }

    @Test
    void canonicalSavepointsRestoreAcrossEveryBackendPair() throws Exception {
        try (RootAllocator inputs = new RootAllocator(64L << 20)) {
            for (boolean sourceRocks : new boolean[] {false, true}) {
                for (boolean targetRocks : new boolean[] {false, true}) {
                    OperatorSubtaskState savepoint;
                    try (KeyedOneInputStreamOperatorTestHarness<RowData, ArrowRowDataBatch, ArrowRowDataBatch> source =
                            harness(1, 0, null, sourceRocks)) {
                        process(source, inputs, row(7, 10));
                        process(source, inputs, row(7, 20));
                        takeKinds(source);
                        savepoint = source.snapshotWithLocalState(
                                        3, 3, SavepointType.savepoint(SavepointFormatType.CANONICAL))
                                .getJobManagerOwnedState();
                        assertThat(savepoint.getRawKeyedState()).hasSize(1);
                    }
                    try (KeyedOneInputStreamOperatorTestHarness<RowData, ArrowRowDataBatch, ArrowRowDataBatch> target =
                            harness(1, 0, savepoint, targetRocks)) {
                        process(target, inputs, row(7, 30));
                        assertThat(takeKinds(target)).containsExactly(RowKind.UPDATE_AFTER, RowKind.UPDATE_AFTER);
                    }
                }
            }
        }
    }

    @Test
    void redistributesKeyGroupsOneToTwoToOneOnBothBackends() throws Exception {
        try (RootAllocator inputs = new RootAllocator(64L << 20)) {
            RowDataKeySelector selector = partitionSelector();
            Map<Integer, GenericRowData> rows = rowsForEverySubtask(selector, 2);
            for (boolean rocksDb : new boolean[] {false, true}) {
                OperatorSubtaskState initialSnapshot;
                try (KeyedOneInputStreamOperatorTestHarness<RowData, ArrowRowDataBatch, ArrowRowDataBatch> initial =
                        harness(1, 0, null, rocksDb)) {
                    for (GenericRowData row : rows.values()) {
                        process(initial, inputs, row);
                    }
                    takeKinds(initial);
                    initialSnapshot = initial.snapshot(4, 4);
                }
                OperatorSubtaskState packaged = AbstractStreamOperatorTestHarness.repackageState(initialSnapshot);
                List<OperatorSubtaskState> scaledSnapshots = new ArrayList<>();
                for (int subtask = 0; subtask < 2; subtask++) {
                    OperatorSubtaskState assigned = AbstractStreamOperatorTestHarness.repartitionOperatorState(
                            packaged, MAX_PARALLELISM, 1, 2, subtask);
                    GenericRowData original = rows.get(subtask);
                    try (KeyedOneInputStreamOperatorTestHarness<RowData, ArrowRowDataBatch, ArrowRowDataBatch> scaled =
                            harness(2, subtask, assigned, rocksDb)) {
                        process(scaled, inputs, row(original.getLong(0), 100));
                        assertThat(takeKinds(scaled)).containsExactly(RowKind.UPDATE_AFTER, RowKind.INSERT);
                        scaledSnapshots.add(scaled.snapshot(5, 5));
                    }
                }
                OperatorSubtaskState packagedScaled = AbstractStreamOperatorTestHarness.repackageState(
                        scaledSnapshots.toArray(new OperatorSubtaskState[0]));
                OperatorSubtaskState assignedBack = AbstractStreamOperatorTestHarness.repartitionOperatorState(
                        packagedScaled, MAX_PARALLELISM, 2, 1, 0);
                try (KeyedOneInputStreamOperatorTestHarness<RowData, ArrowRowDataBatch, ArrowRowDataBatch> target =
                        harness(1, 0, assignedBack, rocksDb)) {
                    for (GenericRowData original : rows.values()) {
                        process(target, inputs, row(original.getLong(0), 200));
                    }
                    assertThat(takeKinds(target))
                            .containsExactly(
                                    RowKind.UPDATE_AFTER,
                                    RowKind.UPDATE_AFTER,
                                    RowKind.UPDATE_AFTER,
                                    RowKind.UPDATE_AFTER);
                }
            }
        }
    }

    @Test
    void emitsIncrementalRocksHandlesReusesSstsAndRestoresMetadataRoundTrip() throws Exception {
        try (RootAllocator inputs = new RootAllocator(64L << 20)) {
            OperatorSubtaskState secondSnapshot;
            try (KeyedOneInputStreamOperatorTestHarness<RowData, ArrowRowDataBatch, ArrowRowDataBatch> rocks =
                    harness(1, 0, null, true)) {
                process(rocks, inputs, row(7, 10));
                takeKinds(rocks);
                OperatorSubtaskState firstSnapshot = rocks.snapshot(6, 6);
                IncrementalRemoteKeyedStateHandle first = incrementalHandle(firstSnapshot);
                assertThat(firstSnapshot.getRawKeyedState()).isEmpty();
                assertThat(first.getSharedState()).isNotEmpty();

                rocks.notifyOfCompletedCheckpoint(6);
                secondSnapshot = rocks.snapshot(7, 7);
                IncrementalRemoteKeyedStateHandle second = incrementalHandle(secondSnapshot);
                assertThat(sharedHandles(second)).isEqualTo(sharedHandles(first));
                assertThat(second.getCheckpointedSize()).isLessThan(first.getCheckpointedSize());
                IncrementalRemoteKeyedStateHandle roundTripped = metadataRoundTrip(second);
                secondSnapshot = secondSnapshot.toBuilder()
                        .setManagedKeyedState(roundTripped)
                        .build();
            }
            try (KeyedOneInputStreamOperatorTestHarness<RowData, ArrowRowDataBatch, ArrowRowDataBatch> restored =
                    harness(1, 0, secondSnapshot, true)) {
                process(restored, inputs, row(7, 20));
                assertThat(takeKinds(restored)).containsExactly(RowKind.UPDATE_AFTER, RowKind.INSERT);
            }
        }
    }

    private static KeyedOneInputStreamOperatorTestHarness<RowData, ArrowRowDataBatch, ArrowRowDataBatch> harness(
            int parallelism, int subtask, OperatorSubtaskState state, boolean rocksDb) throws Exception {
        RowDataKeySelector partition = partitionSelector();
        StreamFusionArrowTopNOperator operator = new StreamFusionArrowTopNOperator(
                INPUT_TYPE,
                OUTPUT_TYPE,
                new int[] {0},
                plan(StreamFusionTopNStrategy.APPEND_FAST),
                partition,
                StreamFusionTopNStrategy.APPEND_FAST,
                10_000L);
        KeyedOneInputStreamOperatorTestHarness<RowData, ArrowRowDataBatch, ArrowRowDataBatch> harness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        operator,
                        new ArrowBatchKeySelector(partition),
                        partition.getProducedType(),
                        MAX_PARALLELISM,
                        parallelism,
                        subtask);
        harness.setStateBackend(new StreamFusionStateBackend(
                rocksDb ? new EmbeddedRocksDBStateBackend(true) : new HashMapStateBackend()));
        harness.setup(ArrowRowDataBatchSerializer.INSTANCE);
        if (state != null) {
            harness.initializeState(state);
        }
        harness.open();
        return harness;
    }

    private static KeyedOneInputStreamOperatorTestHarness<RowData, ArrowRowDataBatch, ArrowRowDataBatch> limitHarness(
            OperatorSubtaskState state, boolean rocksDb) throws Exception {
        RowDataKeySelector partition = partitionSelector();
        StreamFusionArrowTopNOperator operator = new StreamFusionArrowTopNOperator(
                INPUT_TYPE, INPUT_TYPE, new int[0], limitPlan(), partition, StreamFusionTopNStrategy.APPEND_FAST, 0L);
        KeyedOneInputStreamOperatorTestHarness<RowData, ArrowRowDataBatch, ArrowRowDataBatch> harness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        operator,
                        new ArrowBatchKeySelector(partition),
                        partition.getProducedType(),
                        MAX_PARALLELISM,
                        1,
                        0);
        harness.setStateBackend(new StreamFusionStateBackend(
                rocksDb ? new EmbeddedRocksDBStateBackend(true) : new HashMapStateBackend()));
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
            GenericRowData row)
            throws Exception {
        try (ArrowRowDataBatch batch = ArrowRowDataBatch.transpose(List.of(row), INPUT_TYPE, allocator)
                .withEnvelope(new RowKind[] {row.getRowKind()}, new boolean[] {false}, new long[] {0})) {
            harness.processElement(new StreamRecord<>(batch));
        }
    }

    private static void processBatch(
            KeyedOneInputStreamOperatorTestHarness<RowData, ArrowRowDataBatch, ArrowRowDataBatch> harness,
            RootAllocator allocator,
            List<GenericRowData> rows)
            throws Exception {
        try (ArrowRowDataBatch batch = ArrowRowDataBatch.transpose(rows, INPUT_TYPE, allocator)) {
            harness.processElement(new StreamRecord<>(batch));
        }
    }

    private static List<RowKind> takeKinds(
            KeyedOneInputStreamOperatorTestHarness<RowData, ArrowRowDataBatch, ArrowRowDataBatch> harness) {
        List<RowKind> kinds = new ArrayList<>();
        Object output;
        while ((output = harness.getOutput().poll()) != null) {
            @SuppressWarnings("unchecked")
            StreamRecord<ArrowRowDataBatch> record = (StreamRecord<ArrowRowDataBatch>) output;
            ArrowRowDataBatch batch = record.getValue();
            for (int row = 0; row < batch.size(); row++) {
                kinds.add(batch.rowKind(row));
            }
        }
        return kinds;
    }

    private static OperatorSubtaskState snapshot(
            KeyedOneInputStreamOperatorTestHarness<RowData, ArrowRowDataBatch, ArrowRowDataBatch> harness,
            long checkpointId,
            boolean unaligned)
            throws Exception {
        CheckpointStorageLocationReference location = CheckpointStorageLocationReference.getDefault();
        CheckpointOptions options = unaligned
                ? CheckpointOptions.unaligned(CheckpointType.CHECKPOINT, location)
                : CheckpointOptions.alignedNoTimeout(CheckpointType.CHECKPOINT, location);
        return OperatorSnapshotFinalizer.create(harness.getOperator()
                        .snapshotState(checkpointId, checkpointId, options, new MemCheckpointStreamFactory(64 << 20)))
                .getJobManagerOwnedState();
    }

    private static RowDataKeySelector partitionSelector() {
        return selector(0);
    }

    private static RowDataKeySelector selector(int field) {
        return KeySelectorUtil.getRowDataSelector(
                StreamFusionArrowTopNOperatorTest.class.getClassLoader(),
                new int[] {field},
                InternalTypeInfo.of(INPUT_TYPE));
    }

    private static GenericRowData row(long partition, long score) {
        return GenericRowData.of(partition, score);
    }

    private static Map<Integer, GenericRowData> rowsForEverySubtask(RowDataKeySelector selector, int parallelism)
            throws Exception {
        Map<Integer, GenericRowData> rows = new HashMap<>();
        for (long key = 0; rows.size() < parallelism; key++) {
            GenericRowData row = row(key, 10);
            int owner = KeyGroupRangeAssignment.assignKeyToParallelOperator(
                    selector.getKey(row), MAX_PARALLELISM, parallelism);
            rows.putIfAbsent(owner, row);
        }
        return rows;
    }

    private static IncrementalRemoteKeyedStateHandle incrementalHandle(OperatorSubtaskState snapshot) {
        assertThat(snapshot.getManagedKeyedState()).hasSize(1);
        return (IncrementalRemoteKeyedStateHandle)
                snapshot.getManagedKeyedState().iterator().next();
    }

    private static Map<String, Object> sharedHandles(IncrementalRemoteKeyedStateHandle handle) {
        Map<String, Object> handles = new HashMap<>();
        for (HandleAndLocalPath file : handle.getSharedState()) {
            handles.put(file.getLocalPath(), file.getHandle().getStreamStateHandleID());
        }
        return handles;
    }

    private static IncrementalRemoteKeyedStateHandle metadataRoundTrip(KeyedStateHandle handle) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            MetadataV3Serializer.INSTANCE.serializeKeyedStateHandleUtil(handle, output);
        }
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            return (IncrementalRemoteKeyedStateHandle)
                    MetadataV3Serializer.INSTANCE.deserializeKeyedStateHandleUtil(input);
        }
    }

    private static byte[] plan(StreamFusionTopNStrategy strategy) {
        tech.streamfusion.proto.plan.v1.LogicalType bigint = tech.streamfusion.proto.plan.v1.LogicalType.newBuilder()
                .setBigint(EmptyType.getDefaultInstance())
                .build();
        Schema schema = Schema.newBuilder()
                .addFields(tech.streamfusion.proto.plan.v1.Field.newBuilder()
                        .setName("partition_key")
                        .setType(bigint))
                .addFields(tech.streamfusion.proto.plan.v1.Field.newBuilder()
                        .setName("score")
                        .setType(bigint))
                .build();
        Schema outputSchema = schema.toBuilder()
                .addFields(tech.streamfusion.proto.plan.v1.Field.newBuilder()
                        .setName("row_num")
                        .setType(bigint))
                .build();
        TopN topN = TopN.newBuilder()
                .setInput(Operator.newBuilder().setInput(Input.newBuilder()))
                .addPartitionKeyIndices(0)
                .addSortKeyIndices(1)
                .addSortAscending(false)
                .addSortNullsLast(true)
                .setRankStart(1)
                .setRankEnd(2)
                .setOutputRankNumber(true)
                .setStrategy(TopNStrategy.valueOf("TOP_N_STRATEGY_" + strategy.name()))
                .setInputSchema(schema)
                .setOutputSchema(outputSchema)
                .build();
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setTopN(topN))
                .build()
                .toByteArray();
    }

    private static byte[] limitPlan() {
        tech.streamfusion.proto.plan.v1.LogicalType bigint = tech.streamfusion.proto.plan.v1.LogicalType.newBuilder()
                .setBigint(EmptyType.getDefaultInstance())
                .build();
        Schema schema = Schema.newBuilder()
                .addFields(tech.streamfusion.proto.plan.v1.Field.newBuilder()
                        .setName("partition_key")
                        .setType(bigint))
                .addFields(tech.streamfusion.proto.plan.v1.Field.newBuilder()
                        .setName("score")
                        .setType(bigint))
                .build();
        TopN limit = TopN.newBuilder()
                .setInput(Operator.newBuilder().setInput(Input.newBuilder()))
                .setRankStart(2)
                .setRankEnd(3)
                .setStrategy(TopNStrategy.TOP_N_STRATEGY_APPEND_FAST)
                .setInputSchema(schema)
                .setOutputSchema(schema)
                .build();
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setTopN(limit))
                .build()
                .toByteArray();
    }
}
