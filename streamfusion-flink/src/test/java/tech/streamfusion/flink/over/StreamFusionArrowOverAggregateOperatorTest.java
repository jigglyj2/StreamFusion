/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.over;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.HashMap;
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
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle.HandleAndLocalPath;
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
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchSerializer;
import tech.streamfusion.flink.deduplicate.ArrowBatchKeySelector;
import tech.streamfusion.flink.proto.FlinkLogicalTypeProto;
import tech.streamfusion.flink.state.StreamFusionStateBackend;
import tech.streamfusion.proto.plan.v1.AggregateCall;
import tech.streamfusion.proto.plan.v1.AggregateFunction;
import tech.streamfusion.proto.plan.v1.Field;
import tech.streamfusion.proto.plan.v1.Input;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;
import tech.streamfusion.proto.plan.v1.OverAggregate;
import tech.streamfusion.proto.plan.v1.OverTimeAttribute;
import tech.streamfusion.proto.plan.v1.Schema;

class StreamFusionArrowOverAggregateOperatorTest {
    private static final int MAX_PARALLELISM = 128;
    private static final RowType INPUT_TYPE = RowType.of(
            new LogicalType[] {new BigIntType(false), new BigIntType(false), new BigIntType(true)},
            new String[] {"account", "sequence", "amount"});
    private static final RowType OUTPUT_TYPE = RowType.of(
            new LogicalType[] {new BigIntType(false), new BigIntType(false), new BigIntType(true), new BigIntType(true)
            },
            new String[] {"account", "sequence", "amount", "running_sum"});
    private static final RowType EVENT_INPUT_TYPE = RowType.of(
            new LogicalType[] {new BigIntType(false), new TimestampType(false, 3), new BigIntType(true)},
            new String[] {"account", "ts", "amount"});
    private static final RowType EVENT_OUTPUT_TYPE = RowType.of(
            new LogicalType[] {
                new BigIntType(false), new TimestampType(false, 3), new BigIntType(true), new BigIntType(true)
            },
            new String[] {"account", "ts", "amount", "running_sum"});

    @Test
    void exposesCompleteTimerFreeOverMetricsWithLogicalRecordSemantics() throws Exception {
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
        StreamFusionArrowOverAggregateOperator operator = new StreamFusionArrowOverAggregateOperator(
                INPUT_TYPE, OUTPUT_TYPE, new int[] {0}, plan(), true, true, false, selector);
        try (RootAllocator inputs = new RootAllocator(64L << 20);
                MockEnvironment environment = new MockEnvironmentBuilder()
                        .setTaskName("OVER aggregate metric parity")
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

            assertThat(metrics.get("numOfIdsNotFound")).isInstanceOf(Counter.class);
            assertThat(metrics.get("numOfSortKeysNotFound")).isInstanceOf(Counter.class);
            assertThat(metrics.get("rocksDbBackend")).isInstanceOf(Gauge.class);

            // Flink's surrounding task/output counters see one physical Arrow transport record.
            // The native operator replaces those increments with the logical row cardinalities.
            ((Counter) metrics.get("numRecordsIn")).inc();
            ((Counter) metrics.get("numRecordsOut")).inc();
            process(harness, inputs, row(7, 20, 20));

            assertThat(takeKinds(harness)).containsExactly(RowKind.INSERT);
            assertThat(((Counter) metrics.get("numRecordsIn")).getCount()).isEqualTo(1L);
            assertThat(((Counter) metrics.get("numRecordsOut")).getCount()).isEqualTo(1L);
            assertThat(((Counter) metrics.get("processedBatches")).getCount()).isEqualTo(1L);
            assertThat(((Counter) metrics.get("processedRows")).getCount()).isEqualTo(1L);
            assertThat(((Counter) metrics.get("emittedRows")).getCount()).isEqualTo(1L);
            assertThat(((Counter) metrics.get("emittedInserts")).getCount()).isEqualTo(1L);
            assertThat(((Counter) metrics.get("stateReadBatches")).getCount()).isEqualTo(1L);
            assertThat(((Counter) metrics.get("stateWriteBatches")).getCount()).isEqualTo(1L);
            assertThat(((Counter) metrics.get("numOfIdsNotFound")).getCount()).isZero();
            assertThat(((Counter) metrics.get("numOfSortKeysNotFound")).getCount())
                    .isZero();
            assertThat(((Gauge<?>) metrics.get("rocksDbBackend")).getValue()).isEqualTo(0);
        }
    }

    @Test
    void exposesFlinkEventTimeMetricsAndNativeTimerDiagnostics() throws Exception {
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
        RowDataKeySelector selector = eventSelector();
        StreamFusionArrowOverAggregateOperator operator = new StreamFusionArrowOverAggregateOperator(
                EVENT_INPUT_TYPE, EVENT_OUTPUT_TYPE, new int[] {0}, eventPlan(), true, false, true, selector);
        try (RootAllocator inputs = new RootAllocator(64L << 20);
                MockEnvironment environment = new MockEnvironmentBuilder()
                        .setTaskName("event-time OVER aggregate metric parity")
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
            assertThat(metrics.get("numOfIdsNotFound")).isNull();
            assertThat(metrics.get("numOfSortKeysNotFound")).isNull();
            assertThat(metrics.get("pendingEventTimeTimers")).isInstanceOf(Gauge.class);

            processEvent(harness, inputs, eventRow(7, 1_000, 20));
            assertThat(((Gauge<?>) metrics.get("pendingEventTimeTimers")).getValue())
                    .isEqualTo(1L);
            harness.processWatermark(new Watermark(1_000));
            assertThat(takeKinds(harness)).containsExactly(RowKind.INSERT);
            processEvent(harness, inputs, eventRow(7, 999, 10));

            assertThat(((Counter) metrics.get("numLateRecordsDropped")).getCount())
                    .isEqualTo(1L);
            assertThat(((Counter) metrics.get("watermarksAdvanced")).getCount()).isEqualTo(1L);
            assertThat(((Counter) metrics.get("eventTimeTimersFired")).getCount())
                    .isEqualTo(1L);
            assertThat(((Counter) metrics.get("timersRegistered")).getCount()).isEqualTo(1L);
            assertThat(((Counter) metrics.get("timersFired")).getCount()).isEqualTo(1L);
            assertThat(((Gauge<?>) metrics.get("pendingEventTimeTimers")).getValue())
                    .isEqualTo(0L);
        }
    }

    @Test
    void restoresAlignedAndUnalignedCheckpointsAcrossEveryBackendPair() throws Exception {
        try (RootAllocator inputs = new RootAllocator(64L << 20)) {
            for (boolean sourceRocks : new boolean[] {false, true}) {
                for (boolean targetRocks : new boolean[] {false, true}) {
                    for (boolean unaligned : new boolean[] {false, true}) {
                        OperatorSubtaskState snapshot;
                        try (Harness source = harness(1, 0, null, sourceRocks)) {
                            process(source.operator, inputs, row(7, 20, 20));
                            assertThat(takeKinds(source.operator)).containsExactly(RowKind.INSERT);
                            snapshot = snapshot(source.operator, unaligned ? 2 : 1, unaligned);
                        }
                        try (Harness target = harness(1, 0, snapshot, targetRocks)) {
                            process(target.operator, inputs, row(7, 10, 10));
                            assertThat(takeKinds(target.operator))
                                    .containsExactly(RowKind.INSERT, RowKind.UPDATE_BEFORE, RowKind.UPDATE_AFTER);
                        }
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
                    try (Harness source = harness(1, 0, null, sourceRocks)) {
                        process(source.operator, inputs, row(7, 20, 20));
                        takeKinds(source.operator);
                        savepoint = source.operator
                                .snapshotWithLocalState(3, 3, SavepointType.savepoint(SavepointFormatType.CANONICAL))
                                .getJobManagerOwnedState();
                        assertThat(savepoint.getRawKeyedState()).hasSize(1);
                    }
                    try (Harness target = harness(1, 0, savepoint, targetRocks)) {
                        process(target.operator, inputs, row(7, 10, 10));
                        assertThat(takeKinds(target.operator))
                                .containsExactly(RowKind.INSERT, RowKind.UPDATE_BEFORE, RowKind.UPDATE_AFTER);
                    }
                }
            }
        }
    }

    @Test
    void redistributesKeyGroupsFromOneToTwoAndBackOnBothBackends() throws Exception {
        try (RootAllocator inputs = new RootAllocator(64L << 20)) {
            RowDataKeySelector selector = selector();
            Map<Integer, Long> accountByOwner = accountsForEverySubtask(selector, 2);
            for (boolean rocksDb : new boolean[] {false, true}) {
                OperatorSubtaskState initialSnapshot;
                try (Harness initial = harness(1, 0, null, rocksDb)) {
                    for (long account : accountByOwner.values()) {
                        process(initial.operator, inputs, row(account, 20, 20));
                    }
                    takeKinds(initial.operator);
                    initialSnapshot = initial.operator.snapshot(4, 4);
                }

                OperatorSubtaskState packaged = AbstractStreamOperatorTestHarness.repackageState(initialSnapshot);
                List<OperatorSubtaskState> scaledSnapshots = new ArrayList<>();
                for (int subtask = 0; subtask < 2; subtask++) {
                    OperatorSubtaskState assigned = AbstractStreamOperatorTestHarness.repartitionOperatorState(
                            packaged, MAX_PARALLELISM, 1, 2, subtask);
                    try (Harness scaled = harness(2, subtask, assigned, rocksDb)) {
                        process(scaled.operator, inputs, row(accountByOwner.get(subtask), 10, 10));
                        assertThat(takeKinds(scaled.operator))
                                .containsExactly(RowKind.INSERT, RowKind.UPDATE_BEFORE, RowKind.UPDATE_AFTER);
                        scaledSnapshots.add(scaled.operator.snapshot(5, 5));
                    }
                }

                OperatorSubtaskState packagedScaled = AbstractStreamOperatorTestHarness.repackageState(
                        scaledSnapshots.toArray(new OperatorSubtaskState[0]));
                OperatorSubtaskState assignedBack = AbstractStreamOperatorTestHarness.repartitionOperatorState(
                        packagedScaled, MAX_PARALLELISM, 2, 1, 0);
                try (Harness scaledBack = harness(1, 0, assignedBack, rocksDb)) {
                    for (long account : accountByOwner.values()) {
                        process(scaledBack.operator, inputs, row(account, 30, 30));
                    }
                    assertThat(takeKinds(scaledBack.operator)).containsExactly(RowKind.INSERT, RowKind.INSERT);
                }
            }
        }
    }

    @Test
    void nativeRocksCheckpointsReuseSharedSsts() throws Exception {
        try (RootAllocator inputs = new RootAllocator(64L << 20);
                Harness rocks = harness(1, 0, null, true)) {
            process(rocks.operator, inputs, row(7, 20, 20));
            takeKinds(rocks.operator);
            IncrementalRemoteKeyedStateHandle first = incrementalHandle(rocks.operator.snapshot(6, 6));
            assertThat(first.getSharedState()).isNotEmpty();

            rocks.operator.notifyOfCompletedCheckpoint(6);
            IncrementalRemoteKeyedStateHandle second = incrementalHandle(rocks.operator.snapshot(7, 7));
            assertThat(sharedHandles(second)).isEqualTo(sharedHandles(first));
            assertThat(second.getCheckpointedSize()).isLessThan(first.getCheckpointedSize());
        }
    }

    @Test
    void pendingEventTimeTimersRestoreAcrossCheckpointKindsAndBackendPairs() throws Exception {
        try (RootAllocator inputs = new RootAllocator(64L << 20)) {
            for (boolean sourceRocks : new boolean[] {false, true}) {
                for (boolean targetRocks : new boolean[] {false, true}) {
                    for (int recoveryKind = 0; recoveryKind < 3; recoveryKind++) {
                        OperatorSubtaskState snapshot;
                        try (Harness source = eventHarness(null, sourceRocks)) {
                            processEvent(source.operator, inputs, eventRow(7, 1_000, 20));
                            assertThat(takeKinds(source.operator)).isEmpty();
                            snapshot = recoveryKind == 2
                                    ? source.operator
                                            .snapshotWithLocalState(
                                                    20, 20, SavepointType.savepoint(SavepointFormatType.CANONICAL))
                                            .getJobManagerOwnedState()
                                    : snapshot(source.operator, 20, recoveryKind == 1);
                        }
                        try (Harness target = eventHarness(snapshot, targetRocks)) {
                            target.operator.processWatermark(new Watermark(1_000));
                            assertThat(takeKinds(target.operator)).containsExactly(RowKind.INSERT);
                        }
                    }
                }
            }
        }
    }

    @Test
    void redistributesPendingEventTimeTimersFromOneToTwoAndBackOnBothBackends() throws Exception {
        try (RootAllocator inputs = new RootAllocator(64L << 20)) {
            RowDataKeySelector selector = eventSelector();
            Map<Integer, Long> accountByOwner = eventAccountsForEverySubtask(selector, 2);
            for (boolean rocksDb : new boolean[] {false, true}) {
                OperatorSubtaskState initialSnapshot;
                try (Harness initial = eventHarness(1, 0, null, rocksDb)) {
                    for (long account : accountByOwner.values()) {
                        processEvent(initial.operator, inputs, eventRow(account, 1_000, 20));
                    }
                    assertThat(takeKinds(initial.operator)).isEmpty();
                    initialSnapshot = initial.operator.snapshot(30, 30);
                }

                OperatorSubtaskState packaged = AbstractStreamOperatorTestHarness.repackageState(initialSnapshot);
                List<OperatorSubtaskState> scaledSnapshots = new ArrayList<>();
                for (int subtask = 0; subtask < 2; subtask++) {
                    OperatorSubtaskState assigned = AbstractStreamOperatorTestHarness.repartitionOperatorState(
                            packaged, MAX_PARALLELISM, 1, 2, subtask);
                    try (Harness scaled = eventHarness(2, subtask, assigned, rocksDb)) {
                        scaled.operator.processWatermark(new Watermark(1_000));
                        assertThat(takeKinds(scaled.operator)).containsExactly(RowKind.INSERT);
                        processEvent(scaled.operator, inputs, eventRow(accountByOwner.get(subtask), 2_000, 10));
                        assertThat(takeKinds(scaled.operator)).isEmpty();
                        scaledSnapshots.add(scaled.operator.snapshot(31, 31));
                    }
                }

                OperatorSubtaskState packagedScaled = AbstractStreamOperatorTestHarness.repackageState(
                        scaledSnapshots.toArray(new OperatorSubtaskState[0]));
                OperatorSubtaskState assignedBack = AbstractStreamOperatorTestHarness.repartitionOperatorState(
                        packagedScaled, MAX_PARALLELISM, 2, 1, 0);
                try (Harness scaledBack = eventHarness(1, 0, assignedBack, rocksDb)) {
                    scaledBack.operator.processWatermark(new Watermark(2_000));
                    assertThat(takeKinds(scaledBack.operator)).containsExactly(RowKind.INSERT, RowKind.INSERT);
                }
            }
        }
    }

    private static IncrementalRemoteKeyedStateHandle incrementalHandle(OperatorSubtaskState snapshot) {
        assertThat(snapshot.getRawKeyedState()).isEmpty();
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

    private static OperatorSubtaskState snapshot(
            KeyedOneInputStreamOperatorTestHarness<RowData, ArrowRowDataBatch, ArrowRowDataBatch> operator,
            long checkpointId,
            boolean unaligned)
            throws Exception {
        CheckpointStorageLocationReference location = CheckpointStorageLocationReference.getDefault();
        CheckpointOptions options = unaligned
                ? CheckpointOptions.unaligned(CheckpointType.CHECKPOINT, location)
                : CheckpointOptions.alignedNoTimeout(CheckpointType.CHECKPOINT, location);
        return OperatorSnapshotFinalizer.create(operator.getOperator()
                        .snapshotState(checkpointId, checkpointId, options, new MemCheckpointStreamFactory(64 << 20)))
                .getJobManagerOwnedState();
    }

    private static Harness harness(int parallelism, int subtask, OperatorSubtaskState state, boolean rocksDb)
            throws Exception {
        RowDataKeySelector selector = selector();
        StreamFusionArrowOverAggregateOperator operator = new StreamFusionArrowOverAggregateOperator(
                INPUT_TYPE, OUTPUT_TYPE, new int[] {0}, plan(), true, true, false, selector);
        KeyedOneInputStreamOperatorTestHarness<RowData, ArrowRowDataBatch, ArrowRowDataBatch> harness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        operator,
                        new ArrowBatchKeySelector(selector),
                        selector.getProducedType(),
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
        return new Harness(harness);
    }

    private static Harness eventHarness(OperatorSubtaskState state, boolean rocksDb) throws Exception {
        return eventHarness(1, 0, state, rocksDb);
    }

    private static Harness eventHarness(int parallelism, int subtask, OperatorSubtaskState state, boolean rocksDb)
            throws Exception {
        RowDataKeySelector selector = eventSelector();
        StreamFusionArrowOverAggregateOperator operator = new StreamFusionArrowOverAggregateOperator(
                EVENT_INPUT_TYPE, EVENT_OUTPUT_TYPE, new int[] {0}, eventPlan(), true, false, true, selector);
        KeyedOneInputStreamOperatorTestHarness<RowData, ArrowRowDataBatch, ArrowRowDataBatch> harness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        operator,
                        new ArrowBatchKeySelector(selector),
                        selector.getProducedType(),
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
        return new Harness(harness);
    }

    private static void process(
            KeyedOneInputStreamOperatorTestHarness<RowData, ArrowRowDataBatch, ArrowRowDataBatch> operator,
            RootAllocator allocator,
            GenericRowData row)
            throws Exception {
        try (ArrowRowDataBatch batch = ArrowRowDataBatch.transpose(List.of(row), INPUT_TYPE, allocator)
                .withEnvelope(new RowKind[] {row.getRowKind()}, new boolean[] {false}, new long[] {0})) {
            operator.processElement(new StreamRecord<>(batch));
        }
    }

    private static void processEvent(
            KeyedOneInputStreamOperatorTestHarness<RowData, ArrowRowDataBatch, ArrowRowDataBatch> operator,
            RootAllocator allocator,
            GenericRowData row)
            throws Exception {
        try (ArrowRowDataBatch batch = ArrowRowDataBatch.transpose(List.of(row), EVENT_INPUT_TYPE, allocator)
                .withEnvelope(new RowKind[] {row.getRowKind()}, new boolean[] {false}, new long[] {0})) {
            operator.processElement(new StreamRecord<>(batch));
        }
    }

    private static List<RowKind> takeKinds(
            KeyedOneInputStreamOperatorTestHarness<RowData, ArrowRowDataBatch, ArrowRowDataBatch> operator) {
        List<RowKind> kinds = new ArrayList<>();
        Object output;
        while ((output = operator.getOutput().poll()) != null) {
            if (output instanceof Watermark) {
                continue;
            }
            @SuppressWarnings("unchecked")
            StreamRecord<ArrowRowDataBatch> record = (StreamRecord<ArrowRowDataBatch>) output;
            ArrowRowDataBatch batch = record.getValue();
            try {
                for (int row = 0; row < batch.size(); row++) {
                    kinds.add(batch.rowKind(row));
                }
            } finally {
                batch.close();
            }
        }
        return kinds;
    }

    private static RowDataKeySelector selector() {
        return KeySelectorUtil.getRowDataSelector(
                StreamFusionArrowOverAggregateOperatorTest.class.getClassLoader(),
                new int[] {0},
                InternalTypeInfo.of(INPUT_TYPE));
    }

    private static RowDataKeySelector eventSelector() {
        return KeySelectorUtil.getRowDataSelector(
                StreamFusionArrowOverAggregateOperatorTest.class.getClassLoader(),
                new int[] {0},
                InternalTypeInfo.of(EVENT_INPUT_TYPE));
    }

    private static Map<Integer, Long> accountsForEverySubtask(RowDataKeySelector selector, int parallelism)
            throws Exception {
        Map<Integer, Long> accounts = new HashMap<>();
        for (long account = 0; accounts.size() < parallelism; account++) {
            int owner = KeyGroupRangeAssignment.assignKeyToParallelOperator(
                    selector.getKey(row(account, 0, 0)), MAX_PARALLELISM, parallelism);
            accounts.putIfAbsent(owner, account);
        }
        return accounts;
    }

    private static Map<Integer, Long> eventAccountsForEverySubtask(RowDataKeySelector selector, int parallelism)
            throws Exception {
        Map<Integer, Long> accounts = new HashMap<>();
        for (long account = 0; accounts.size() < parallelism; account++) {
            int owner = KeyGroupRangeAssignment.assignKeyToParallelOperator(
                    selector.getKey(eventRow(account, 0, 0)), MAX_PARALLELISM, parallelism);
            accounts.putIfAbsent(owner, account);
        }
        return accounts;
    }

    private static GenericRowData row(long account, long sequence, long amount) {
        return GenericRowData.of(account, sequence, amount);
    }

    private static GenericRowData eventRow(long account, long timestamp, long amount) {
        return GenericRowData.of(account, TimestampData.fromEpochMillis(timestamp), amount);
    }

    private static byte[] plan() {
        OverAggregate aggregate = OverAggregate.newBuilder()
                .setInput(Operator.newBuilder().setInput(Input.newBuilder()))
                .addPartitionKeyIndices(0)
                .setOrderKeyIndex(1)
                .setRowsFrame(true)
                .setTimeAttribute(OverTimeAttribute.OVER_TIME_ATTRIBUTE_NON_TIME)
                .addAggregateCalls(AggregateCall.newBuilder()
                        .setFunction(AggregateFunction.AGGREGATE_FUNCTION_SUM)
                        .setInputIndex(2)
                        .setInputType(FlinkLogicalTypeProto.serialize(INPUT_TYPE.getTypeAt(2)))
                        .setOutputType(FlinkLogicalTypeProto.serialize(OUTPUT_TYPE.getTypeAt(3)))
                        .setRetractable(true))
                .setInputSchema(schema(INPUT_TYPE))
                .setOutputSchema(schema(OUTPUT_TYPE))
                .setInputChangelog(true)
                .setSortAscending(true)
                .build();
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setOverAggregate(aggregate))
                .build()
                .toByteArray();
    }

    private static byte[] eventPlan() {
        OverAggregate aggregate = OverAggregate.newBuilder()
                .setInput(Operator.newBuilder().setInput(Input.newBuilder()))
                .addPartitionKeyIndices(0)
                .setOrderKeyIndex(1)
                .setRowsFrame(true)
                .setTimeAttribute(OverTimeAttribute.OVER_TIME_ATTRIBUTE_EVENT_TIME)
                .addAggregateCalls(AggregateCall.newBuilder()
                        .setFunction(AggregateFunction.AGGREGATE_FUNCTION_SUM)
                        .setInputIndex(2)
                        .setInputType(FlinkLogicalTypeProto.serialize(EVENT_INPUT_TYPE.getTypeAt(2)))
                        .setOutputType(FlinkLogicalTypeProto.serialize(EVENT_OUTPUT_TYPE.getTypeAt(3)))
                        .setRetractable(true))
                .setInputSchema(schema(EVENT_INPUT_TYPE))
                .setOutputSchema(schema(EVENT_OUTPUT_TYPE))
                .setInputChangelog(true)
                .setSortAscending(true)
                .build();
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setOverAggregate(aggregate))
                .build()
                .toByteArray();
    }

    private static Schema schema(RowType type) {
        Schema.Builder schema = Schema.newBuilder();
        for (RowType.RowField field : type.getFields()) {
            schema.addFields(Field.newBuilder()
                    .setName(field.getName())
                    .setType(FlinkLogicalTypeProto.serialize(field.getType())));
        }
        return schema.build();
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
