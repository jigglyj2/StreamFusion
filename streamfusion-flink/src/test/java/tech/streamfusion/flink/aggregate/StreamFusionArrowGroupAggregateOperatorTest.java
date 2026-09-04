/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.aggregate;

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
import org.apache.flink.metrics.MetricGroup;
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
import tech.streamfusion.proto.plan.v1.AggregateCall;
import tech.streamfusion.proto.plan.v1.AggregateFunction;
import tech.streamfusion.proto.plan.v1.EmptyType;
import tech.streamfusion.proto.plan.v1.GroupAggregate;
import tech.streamfusion.proto.plan.v1.Input;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;

class StreamFusionArrowGroupAggregateOperatorTest {
    private static final int MAX_PARALLELISM = 128;
    private static final RowType INPUT_TYPE = RowType.of(
            new LogicalType[] {new BigIntType(false), new BigIntType(true)}, new String[] {"bidder", "price"});
    private static final RowType OUTPUT_TYPE = RowType.of(
            new LogicalType[] {new BigIntType(false), new BigIntType(false), new BigIntType(true)},
            new String[] {"bidder", "bids", "average_price"});
    private static final RowType DISTINCT_AGGREGATE_OUTPUT_TYPE = RowType.of(
            new LogicalType[] {new BigIntType(false), new BigIntType(false), new BigIntType(true), new BigIntType(true)
            },
            new String[] {"bidder", "distinct_prices", "distinct_sum", "distinct_average"});
    private static final RowType DISTINCT_OUTPUT_TYPE =
            RowType.of(new LogicalType[] {new BigIntType(false)}, new String[] {"bidder"});
    private static final RowType GLOBAL_OUTPUT_TYPE =
            RowType.of(new LogicalType[] {new BigIntType(false)}, new String[] {"bids"});

    @Test
    void exposesLogicalFlinkIoAndCompleteTimerFreeStateMetricsForGlobalAggregation() throws Exception {
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
        RowDataKeySelector selector = emptySelector();
        StreamFusionArrowGroupAggregateOperator operator = new StreamFusionArrowGroupAggregateOperator(
                INPUT_TYPE, GLOBAL_OUTPUT_TYPE, new int[0], globalPlan(), false, selector);
        try (RootAllocator inputs = new RootAllocator(64L << 20);
                MockEnvironment environment = new MockEnvironmentBuilder()
                        .setTaskName("Global aggregate metric parity")
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

            assertThat(metrics.get("rocksDbBackend")).isInstanceOf(Gauge.class);
            ((Counter) metrics.get("numRecordsIn")).inc();
            ((Counter) metrics.get("numRecordsOut")).inc();
            process(harness, inputs, row(7, 10));
            assertThat(takeKinds(harness)).containsExactly(RowKind.INSERT);

            assertThat(((Counter) metrics.get("numRecordsIn")).getCount()).isEqualTo(1L);
            assertThat(((Counter) metrics.get("numRecordsOut")).getCount()).isEqualTo(1L);
            assertThat(((Counter) metrics.get("processedBatches")).getCount()).isEqualTo(1L);
            assertThat(((Counter) metrics.get("processedRows")).getCount()).isEqualTo(1L);
            assertThat(((Counter) metrics.get("emittedRows")).getCount()).isEqualTo(1L);
            assertThat(((Counter) metrics.get("emittedInserts")).getCount()).isEqualTo(1L);
            assertThat(((Counter) metrics.get("stateReadBatches")).getCount()).isEqualTo(1L);
            assertThat(((Counter) metrics.get("stateWriteBatches")).getCount()).isEqualTo(1L);
            assertThat(((Counter) metrics.get("processingFailures")).getCount()).isZero();
            assertThat(((Gauge<?>) metrics.get("rocksDbBackend")).getValue()).isEqualTo(0);
        }
    }

    @Test
    void globalAggregateRestoresAcrossBackendPairsAndCheckpointModes() throws Exception {
        try (RootAllocator inputs = new RootAllocator(64L << 20)) {
            for (boolean sourceRocks : new boolean[] {false, true}) {
                for (boolean targetRocks : new boolean[] {false, true}) {
                    for (boolean unaligned : new boolean[] {false, true}) {
                        OperatorSubtaskState snapshot;
                        try (KeyedOneInputStreamOperatorTestHarness<RowData, ArrowRowDataBatch, ArrowRowDataBatch>
                                source = globalHarness(null, sourceRocks)) {
                            process(source, inputs, row(7, 10));
                            assertThat(takeKinds(source)).containsExactly(RowKind.INSERT);
                            snapshot = snapshot(source, unaligned ? 202 : 201, unaligned);
                        }
                        try (KeyedOneInputStreamOperatorTestHarness<RowData, ArrowRowDataBatch, ArrowRowDataBatch>
                                target = globalHarness(snapshot, targetRocks)) {
                            process(target, inputs, row(8, 20));
                            assertThat(takeKinds(target)).containsExactly(RowKind.UPDATE_AFTER);
                        }
                    }
                }
            }
        }
    }

    @Test
    void globalAggregateCanonicalSavepointRestoresAcrossEveryBackendPair() throws Exception {
        try (RootAllocator inputs = new RootAllocator(64L << 20)) {
            for (boolean sourceRocks : new boolean[] {false, true}) {
                for (boolean targetRocks : new boolean[] {false, true}) {
                    OperatorSubtaskState savepoint;
                    try (KeyedOneInputStreamOperatorTestHarness<RowData, ArrowRowDataBatch, ArrowRowDataBatch> source =
                            globalHarness(null, sourceRocks)) {
                        process(source, inputs, row(7, 10));
                        takeKinds(source);
                        savepoint = source.snapshotWithLocalState(
                                        203, 203, SavepointType.savepoint(SavepointFormatType.CANONICAL))
                                .getJobManagerOwnedState();
                        assertThat(savepoint.getRawKeyedState()).hasSize(1);
                    }
                    try (KeyedOneInputStreamOperatorTestHarness<RowData, ArrowRowDataBatch, ArrowRowDataBatch> target =
                            globalHarness(savepoint, targetRocks)) {
                        process(target, inputs, row(8, 20));
                        assertThat(takeKinds(target)).containsExactly(RowKind.UPDATE_AFTER);
                    }
                }
            }
        }
    }

    @Test
    void selectDistinctRetractionsRestoreAcrossBackendPairsAndCheckpointModes() throws Exception {
        try (RootAllocator inputs = new RootAllocator(64L << 20)) {
            for (boolean sourceRocks : new boolean[] {false, true}) {
                for (boolean targetRocks : new boolean[] {false, true}) {
                    for (boolean unaligned : new boolean[] {false, true}) {
                        OperatorSubtaskState snapshot;
                        try (KeyedOneInputStreamOperatorTestHarness<RowData, ArrowRowDataBatch, ArrowRowDataBatch>
                                source = distinctHarness(1, 0, null, sourceRocks)) {
                            process(source, inputs, row(RowKind.INSERT, 7, 10));
                            process(source, inputs, row(RowKind.UPDATE_AFTER, 7, 20));
                            assertThat(takeKinds(source)).containsExactly(RowKind.INSERT);
                            snapshot = snapshot(source, unaligned ? 102 : 101, unaligned);
                            process(source, inputs, row(RowKind.UPDATE_BEFORE, 7, 20));
                            assertThat(takeKinds(source)).isEmpty();
                            process(source, inputs, row(RowKind.DELETE, 7, 10));
                            assertThat(takeKinds(source)).containsExactly(RowKind.DELETE);
                        }
                        try (KeyedOneInputStreamOperatorTestHarness<RowData, ArrowRowDataBatch, ArrowRowDataBatch>
                                target = distinctHarness(1, 0, snapshot, targetRocks)) {
                            process(target, inputs, row(RowKind.UPDATE_BEFORE, 7, 20));
                            assertThat(takeKinds(target)).isEmpty();
                            process(target, inputs, row(RowKind.DELETE, 7, 10));
                            assertThat(takeKinds(target)).containsExactly(RowKind.DELETE);
                            process(target, inputs, row(RowKind.DELETE, 7, 10));
                            assertThat(takeKinds(target)).isEmpty();
                        }
                    }
                }
            }
        }
    }

    @Test
    void selectDistinctCanonicalSavepointsRestoreAcrossEveryBackendPair() throws Exception {
        try (RootAllocator inputs = new RootAllocator(64L << 20)) {
            for (boolean sourceRocks : new boolean[] {false, true}) {
                for (boolean targetRocks : new boolean[] {false, true}) {
                    OperatorSubtaskState savepoint;
                    try (KeyedOneInputStreamOperatorTestHarness<RowData, ArrowRowDataBatch, ArrowRowDataBatch> source =
                            distinctHarness(1, 0, null, sourceRocks)) {
                        process(source, inputs, row(RowKind.INSERT, 7, 10));
                        process(source, inputs, row(RowKind.UPDATE_AFTER, 7, 20));
                        assertThat(takeKinds(source)).containsExactly(RowKind.INSERT);
                        savepoint = source.snapshotWithLocalState(
                                        103, 103, SavepointType.savepoint(SavepointFormatType.CANONICAL))
                                .getJobManagerOwnedState();
                        assertThat(savepoint.getRawKeyedState()).hasSize(1);
                    }
                    try (KeyedOneInputStreamOperatorTestHarness<RowData, ArrowRowDataBatch, ArrowRowDataBatch> target =
                            distinctHarness(1, 0, savepoint, targetRocks)) {
                        process(target, inputs, row(RowKind.UPDATE_BEFORE, 7, 20));
                        assertThat(takeKinds(target)).isEmpty();
                        process(target, inputs, row(RowKind.DELETE, 7, 10));
                        assertThat(takeKinds(target)).containsExactly(RowKind.DELETE);
                    }
                }
            }
        }
    }

    @Test
    void distinctAggregateRestoresAcrossBackendPairsAndCheckpointModes() throws Exception {
        try (RootAllocator inputs = new RootAllocator(64L << 20)) {
            for (boolean sourceRocks : new boolean[] {false, true}) {
                for (boolean targetRocks : new boolean[] {false, true}) {
                    for (boolean unaligned : new boolean[] {false, true}) {
                        OperatorSubtaskState snapshot;
                        try (KeyedOneInputStreamOperatorTestHarness<RowData, ArrowRowDataBatch, ArrowRowDataBatch>
                                source = distinctAggregateHarness(null, sourceRocks)) {
                            process(source, inputs, row(RowKind.INSERT, 7, 10));
                            process(source, inputs, row(RowKind.INSERT, 7, 10));
                            assertThat(takeKinds(source)).containsExactly(RowKind.INSERT);
                            snapshot = snapshot(source, unaligned ? 212 : 211, unaligned);
                        }
                        try (KeyedOneInputStreamOperatorTestHarness<RowData, ArrowRowDataBatch, ArrowRowDataBatch>
                                target = distinctAggregateHarness(snapshot, targetRocks)) {
                            process(target, inputs, row(RowKind.DELETE, 7, 10));
                            assertThat(takeKinds(target)).isEmpty();
                            process(target, inputs, row(RowKind.DELETE, 7, 10));
                            assertThat(takeKinds(target)).containsExactly(RowKind.DELETE);
                        }
                    }
                }
            }
        }
    }

    @Test
    void distinctAggregateCanonicalSavepointRestoresAcrossEveryBackendPair() throws Exception {
        try (RootAllocator inputs = new RootAllocator(64L << 20)) {
            for (boolean sourceRocks : new boolean[] {false, true}) {
                for (boolean targetRocks : new boolean[] {false, true}) {
                    OperatorSubtaskState savepoint;
                    try (KeyedOneInputStreamOperatorTestHarness<RowData, ArrowRowDataBatch, ArrowRowDataBatch> source =
                            distinctAggregateHarness(null, sourceRocks)) {
                        process(source, inputs, row(RowKind.INSERT, 7, 10));
                        process(source, inputs, row(RowKind.INSERT, 7, 10));
                        assertThat(takeKinds(source)).containsExactly(RowKind.INSERT);
                        savepoint = source.snapshotWithLocalState(
                                        213, 213, SavepointType.savepoint(SavepointFormatType.CANONICAL))
                                .getJobManagerOwnedState();
                        assertThat(savepoint.getRawKeyedState()).hasSize(1);
                    }
                    try (KeyedOneInputStreamOperatorTestHarness<RowData, ArrowRowDataBatch, ArrowRowDataBatch> target =
                            distinctAggregateHarness(savepoint, targetRocks)) {
                        process(target, inputs, row(RowKind.DELETE, 7, 10));
                        assertThat(takeKinds(target)).isEmpty();
                        process(target, inputs, row(RowKind.DELETE, 7, 10));
                        assertThat(takeKinds(target)).containsExactly(RowKind.DELETE);
                    }
                }
            }
        }
    }

    @Test
    void selectDistinctRedistributesKeyGroupsFromOneToTwoAndBackOnBothBackends() throws Exception {
        try (RootAllocator inputs = new RootAllocator(64L << 20)) {
            RowDataKeySelector selector = rowSelector();
            Map<Integer, GenericRowData> rows = rowsForEverySubtask(selector, 2);
            for (boolean rocksDb : new boolean[] {false, true}) {
                OperatorSubtaskState initialSnapshot;
                try (KeyedOneInputStreamOperatorTestHarness<RowData, ArrowRowDataBatch, ArrowRowDataBatch> initial =
                        distinctHarness(1, 0, null, rocksDb)) {
                    for (GenericRowData row : rows.values()) {
                        row.setRowKind(RowKind.INSERT);
                        process(initial, inputs, row);
                    }
                    assertThat(takeKinds(initial)).containsExactly(RowKind.INSERT, RowKind.INSERT);
                    initialSnapshot = initial.snapshot(104, 104);
                }

                OperatorSubtaskState packaged = AbstractStreamOperatorTestHarness.repackageState(initialSnapshot);
                List<OperatorSubtaskState> scaledSnapshots = new ArrayList<>();
                for (int subtask = 0; subtask < 2; subtask++) {
                    OperatorSubtaskState assigned = AbstractStreamOperatorTestHarness.repartitionOperatorState(
                            packaged, MAX_PARALLELISM, 1, 2, subtask);
                    GenericRowData duplicate = rows.get(subtask);
                    duplicate.setRowKind(RowKind.UPDATE_AFTER);
                    try (KeyedOneInputStreamOperatorTestHarness<RowData, ArrowRowDataBatch, ArrowRowDataBatch> scaled =
                            distinctHarness(2, subtask, assigned, rocksDb)) {
                        process(scaled, inputs, duplicate);
                        assertThat(takeKinds(scaled)).isEmpty();
                        scaledSnapshots.add(scaled.snapshot(105, 105));
                    }
                }

                OperatorSubtaskState packagedScaled = AbstractStreamOperatorTestHarness.repackageState(
                        scaledSnapshots.toArray(new OperatorSubtaskState[0]));
                OperatorSubtaskState assignedBack = AbstractStreamOperatorTestHarness.repartitionOperatorState(
                        packagedScaled, MAX_PARALLELISM, 2, 1, 0);
                try (KeyedOneInputStreamOperatorTestHarness<RowData, ArrowRowDataBatch, ArrowRowDataBatch> scaledBack =
                        distinctHarness(1, 0, assignedBack, rocksDb)) {
                    for (GenericRowData row : rows.values()) {
                        row.setRowKind(RowKind.DELETE);
                        process(scaledBack, inputs, row);
                        assertThat(takeKinds(scaledBack)).isEmpty();
                        process(scaledBack, inputs, row);
                        assertThat(takeKinds(scaledBack)).containsExactly(RowKind.DELETE);
                    }
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
                        assertThat(takeKinds(before)).containsExactly(RowKind.INSERT);
                        snapshot = snapshot(before, unaligned ? 2 : 1, unaligned);
                    }
                    try (KeyedOneInputStreamOperatorTestHarness<RowData, ArrowRowDataBatch, ArrowRowDataBatch> after =
                            harness(1, 0, snapshot, rocksDb)) {
                        process(after, inputs, row(7, 20));
                        assertThat(takeKinds(after)).containsExactly(RowKind.UPDATE_AFTER);
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
                        takeKinds(source);
                        savepoint = source.snapshotWithLocalState(
                                        3, 3, SavepointType.savepoint(SavepointFormatType.CANONICAL))
                                .getJobManagerOwnedState();
                        assertThat(savepoint.getRawKeyedState()).hasSize(1);
                    }
                    try (KeyedOneInputStreamOperatorTestHarness<RowData, ArrowRowDataBatch, ArrowRowDataBatch> target =
                            harness(1, 0, savepoint, targetRocks)) {
                        process(target, inputs, row(7, 20));
                        assertThat(takeKinds(target)).containsExactly(RowKind.UPDATE_AFTER);
                    }
                }
            }
        }
    }

    @Test
    void redistributesKeyGroupsFromOneToTwoAndBackToOneOnBothBackends() throws Exception {
        try (RootAllocator inputs = new RootAllocator(64L << 20)) {
            RowDataKeySelector selector = rowSelector();
            Map<Integer, GenericRowData> rows = rowsForEverySubtask(selector, 2);
            for (boolean rocksDb : new boolean[] {false, true}) {
                OperatorSubtaskState oneSubtask;
                try (KeyedOneInputStreamOperatorTestHarness<RowData, ArrowRowDataBatch, ArrowRowDataBatch> initial =
                        harness(1, 0, null, rocksDb)) {
                    for (GenericRowData row : rows.values()) {
                        process(initial, inputs, row);
                    }
                    takeKinds(initial);
                    oneSubtask = initial.snapshot(4, 4);
                }

                OperatorSubtaskState packaged = AbstractStreamOperatorTestHarness.repackageState(oneSubtask);
                List<OperatorSubtaskState> twoSubtaskSnapshots = new ArrayList<>();
                for (int subtask = 0; subtask < 2; subtask++) {
                    OperatorSubtaskState assigned = AbstractStreamOperatorTestHarness.repartitionOperatorState(
                            packaged, MAX_PARALLELISM, 1, 2, subtask);
                    try (KeyedOneInputStreamOperatorTestHarness<RowData, ArrowRowDataBatch, ArrowRowDataBatch> scaled =
                            harness(2, subtask, assigned, rocksDb)) {
                        process(scaled, inputs, rows.get(subtask));
                        assertThat(takeKinds(scaled)).containsExactly(RowKind.UPDATE_AFTER);
                        twoSubtaskSnapshots.add(scaled.snapshot(5, 5));
                    }
                }

                OperatorSubtaskState packagedTwo = AbstractStreamOperatorTestHarness.repackageState(
                        twoSubtaskSnapshots.toArray(new OperatorSubtaskState[0]));
                OperatorSubtaskState assignedBack = AbstractStreamOperatorTestHarness.repartitionOperatorState(
                        packagedTwo, MAX_PARALLELISM, 2, 1, 0);
                try (KeyedOneInputStreamOperatorTestHarness<RowData, ArrowRowDataBatch, ArrowRowDataBatch> scaledBack =
                        harness(1, 0, assignedBack, rocksDb)) {
                    for (GenericRowData row : rows.values()) {
                        process(scaledBack, inputs, row);
                    }
                    assertThat(takeKinds(scaledBack)).containsExactly(RowKind.UPDATE_AFTER, RowKind.UPDATE_AFTER);
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
                assertThat(sharedHandles(roundTripped)).isEqualTo(sharedHandles(second));
                secondSnapshot = secondSnapshot.toBuilder()
                        .setManagedKeyedState(roundTripped)
                        .build();
            }

            try (KeyedOneInputStreamOperatorTestHarness<RowData, ArrowRowDataBatch, ArrowRowDataBatch> restored =
                    harness(1, 0, secondSnapshot, true)) {
                process(restored, inputs, row(7, 20));
                assertThat(takeKinds(restored)).containsExactly(RowKind.UPDATE_AFTER);
            }
        }
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

    private static KeyedOneInputStreamOperatorTestHarness<RowData, ArrowRowDataBatch, ArrowRowDataBatch> harness(
            int parallelism, int subtask, OperatorSubtaskState state, boolean rocksDb) throws Exception {
        RowDataKeySelector selector = rowSelector();
        StreamFusionArrowGroupAggregateOperator operator = new StreamFusionArrowGroupAggregateOperator(
                INPUT_TYPE, OUTPUT_TYPE, new int[] {0}, plan(), false, selector);
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
        return harness;
    }

    private static KeyedOneInputStreamOperatorTestHarness<RowData, ArrowRowDataBatch, ArrowRowDataBatch>
            distinctHarness(int parallelism, int subtask, OperatorSubtaskState state, boolean rocksDb)
                    throws Exception {
        RowDataKeySelector selector = rowSelector();
        StreamFusionArrowGroupAggregateOperator operator = new StreamFusionArrowGroupAggregateOperator(
                INPUT_TYPE, DISTINCT_OUTPUT_TYPE, new int[] {0}, distinctPlan(), true, selector);
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
        return harness;
    }

    private static KeyedOneInputStreamOperatorTestHarness<RowData, ArrowRowDataBatch, ArrowRowDataBatch>
            distinctAggregateHarness(OperatorSubtaskState state, boolean rocksDb) throws Exception {
        RowDataKeySelector selector = rowSelector();
        StreamFusionArrowGroupAggregateOperator operator = new StreamFusionArrowGroupAggregateOperator(
                INPUT_TYPE, DISTINCT_AGGREGATE_OUTPUT_TYPE, new int[] {0}, distinctAggregatePlan(), true, selector);
        KeyedOneInputStreamOperatorTestHarness<RowData, ArrowRowDataBatch, ArrowRowDataBatch> harness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        operator,
                        new ArrowBatchKeySelector(selector),
                        selector.getProducedType(),
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

    private static KeyedOneInputStreamOperatorTestHarness<RowData, ArrowRowDataBatch, ArrowRowDataBatch> globalHarness(
            OperatorSubtaskState state, boolean rocksDb) throws Exception {
        RowDataKeySelector selector = emptySelector();
        StreamFusionArrowGroupAggregateOperator operator = new StreamFusionArrowGroupAggregateOperator(
                INPUT_TYPE, GLOBAL_OUTPUT_TYPE, new int[0], globalPlan(), false, selector);
        KeyedOneInputStreamOperatorTestHarness<RowData, ArrowRowDataBatch, ArrowRowDataBatch> harness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        operator,
                        new ArrowBatchKeySelector(selector),
                        selector.getProducedType(),
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

    private static RowDataKeySelector rowSelector() {
        return KeySelectorUtil.getRowDataSelector(
                StreamFusionArrowGroupAggregateOperatorTest.class.getClassLoader(),
                new int[] {0},
                InternalTypeInfo.of(INPUT_TYPE));
    }

    private static RowDataKeySelector emptySelector() {
        return KeySelectorUtil.getRowDataSelector(
                StreamFusionArrowGroupAggregateOperatorTest.class.getClassLoader(),
                new int[0],
                InternalTypeInfo.of(INPUT_TYPE));
    }

    private static Map<Integer, GenericRowData> rowsForEverySubtask(RowDataKeySelector selector, int parallelism)
            throws Exception {
        Map<Integer, GenericRowData> rows = new HashMap<>();
        for (long bidder = 0; rows.size() < parallelism; bidder++) {
            GenericRowData row = row(bidder, bidder * 10);
            int owner = KeyGroupRangeAssignment.assignKeyToParallelOperator(
                    selector.getKey(row), MAX_PARALLELISM, parallelism);
            rows.putIfAbsent(owner, row);
        }
        return rows;
    }

    private static GenericRowData row(long bidder, long price) {
        return GenericRowData.of(bidder, price);
    }

    private static GenericRowData row(RowKind kind, long bidder, long price) {
        GenericRowData row = GenericRowData.of(bidder, price);
        row.setRowKind(kind);
        return row;
    }

    private static byte[] distinctPlan() {
        GroupAggregate aggregate = GroupAggregate.newBuilder()
                .setInput(Operator.newBuilder().setInput(Input.newBuilder()))
                .addGroupingIndices(0)
                .setInputChangelog(true)
                .build();
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setGroupAggregate(aggregate))
                .build()
                .toByteArray();
    }

    private static byte[] plan() {
        tech.streamfusion.proto.plan.v1.LogicalType bigint = tech.streamfusion.proto.plan.v1.LogicalType.newBuilder()
                .setBigint(EmptyType.getDefaultInstance())
                .build();
        GroupAggregate aggregate = GroupAggregate.newBuilder()
                .setInput(Operator.newBuilder().setInput(Input.newBuilder()))
                .addGroupingIndices(0)
                .addAggregateCalls(AggregateCall.newBuilder()
                        .setFunction(AggregateFunction.AGGREGATE_FUNCTION_COUNT_STAR)
                        .setOutputType(bigint))
                .addAggregateCalls(AggregateCall.newBuilder()
                        .setFunction(AggregateFunction.AGGREGATE_FUNCTION_AVG)
                        .setInputIndex(1)
                        .setInputType(bigint)
                        .setOutputType(bigint)
                        .setAccumulatorType(bigint)
                        .setRetractable(true))
                .build();
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setGroupAggregate(aggregate))
                .build()
                .toByteArray();
    }

    private static byte[] distinctAggregatePlan() {
        tech.streamfusion.proto.plan.v1.LogicalType bigint = tech.streamfusion.proto.plan.v1.LogicalType.newBuilder()
                .setBigint(EmptyType.getDefaultInstance())
                .build();
        GroupAggregate aggregate = GroupAggregate.newBuilder()
                .setInput(Operator.newBuilder().setInput(Input.newBuilder()))
                .addGroupingIndices(0)
                .setInputChangelog(true)
                .addAggregateCalls(AggregateCall.newBuilder()
                        .setFunction(AggregateFunction.AGGREGATE_FUNCTION_COUNT)
                        .setInputIndex(1)
                        .setInputType(bigint)
                        .setOutputType(bigint)
                        .setRetractable(true)
                        .setDistinct(true))
                .addAggregateCalls(AggregateCall.newBuilder()
                        .setFunction(AggregateFunction.AGGREGATE_FUNCTION_SUM)
                        .setInputIndex(1)
                        .setInputType(bigint)
                        .setOutputType(bigint)
                        .setRetractable(true)
                        .setDistinct(true))
                .addAggregateCalls(AggregateCall.newBuilder()
                        .setFunction(AggregateFunction.AGGREGATE_FUNCTION_AVG)
                        .setInputIndex(1)
                        .setInputType(bigint)
                        .setOutputType(bigint)
                        .setAccumulatorType(bigint)
                        .setRetractable(true)
                        .setDistinct(true))
                .build();
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setGroupAggregate(aggregate))
                .build()
                .toByteArray();
    }

    private static byte[] globalPlan() {
        tech.streamfusion.proto.plan.v1.LogicalType bigint = tech.streamfusion.proto.plan.v1.LogicalType.newBuilder()
                .setBigint(EmptyType.getDefaultInstance())
                .build();
        GroupAggregate aggregate = GroupAggregate.newBuilder()
                .setInput(Operator.newBuilder().setInput(Input.newBuilder()))
                .addAggregateCalls(AggregateCall.newBuilder()
                        .setFunction(AggregateFunction.AGGREGATE_FUNCTION_COUNT_STAR)
                        .setOutputType(bigint))
                .build();
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setGroupAggregate(aggregate))
                .build()
                .toByteArray();
    }
}
