/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.aggregate;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
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
            new LogicalType[] {new BigIntType(false), new BigIntType(false)}, new String[] {"bidder", "bids"});

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
    void canonicalSavepointMovesFromRocksDbToMemory() throws Exception {
        try (RootAllocator inputs = new RootAllocator(64L << 20)) {
            OperatorSubtaskState savepoint;
            try (KeyedOneInputStreamOperatorTestHarness<RowData, ArrowRowDataBatch, ArrowRowDataBatch> rocks =
                    harness(1, 0, null, true)) {
                process(rocks, inputs, row(7, 10));
                takeKinds(rocks);
                savepoint = rocks.snapshotWithLocalState(3, 3, SavepointType.savepoint(SavepointFormatType.CANONICAL))
                        .getJobManagerOwnedState();
                assertThat(savepoint.getRawKeyedState()).hasSize(1);
            }
            try (KeyedOneInputStreamOperatorTestHarness<RowData, ArrowRowDataBatch, ArrowRowDataBatch> memory =
                    harness(1, 0, savepoint, false)) {
                process(memory, inputs, row(7, 20));
                assertThat(takeKinds(memory)).containsExactly(RowKind.UPDATE_AFTER);
            }
        }
    }

    @Test
    void redistributesKeyGroupsFromOneSubtaskToTwoOnBothBackends() throws Exception {
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
                for (int subtask = 0; subtask < 2; subtask++) {
                    OperatorSubtaskState assigned = AbstractStreamOperatorTestHarness.repartitionOperatorState(
                            packaged, MAX_PARALLELISM, 1, 2, subtask);
                    try (KeyedOneInputStreamOperatorTestHarness<RowData, ArrowRowDataBatch, ArrowRowDataBatch> scaled =
                            harness(2, subtask, assigned, rocksDb)) {
                        process(scaled, inputs, rows.get(subtask));
                        assertThat(takeKinds(scaled)).containsExactly(RowKind.UPDATE_AFTER);
                    }
                }
            }
        }
    }

    private static void process(
            KeyedOneInputStreamOperatorTestHarness<RowData, ArrowRowDataBatch, ArrowRowDataBatch> harness,
            RootAllocator allocator,
            GenericRowData row)
            throws Exception {
        try (ArrowRowDataBatch batch = ArrowRowDataBatch.transpose(List.of(row), INPUT_TYPE, allocator)) {
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

    private static RowDataKeySelector rowSelector() {
        return KeySelectorUtil.getRowDataSelector(
                StreamFusionArrowGroupAggregateOperatorTest.class.getClassLoader(),
                new int[] {0},
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
                .build();
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setGroupAggregate(aggregate))
                .build()
                .toByteArray();
    }
}
