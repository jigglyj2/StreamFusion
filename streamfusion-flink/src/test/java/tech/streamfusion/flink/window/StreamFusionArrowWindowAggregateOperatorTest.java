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
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.SavepointType;
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
import tech.streamfusion.flink.state.StreamFusionStateBackend;
import tech.streamfusion.proto.plan.v1.AggregateCall;
import tech.streamfusion.proto.plan.v1.AggregateFunction;
import tech.streamfusion.proto.plan.v1.EmptyType;
import tech.streamfusion.proto.plan.v1.Field;
import tech.streamfusion.proto.plan.v1.Input;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;
import tech.streamfusion.proto.plan.v1.PrecisionType;
import tech.streamfusion.proto.plan.v1.Schema;
import tech.streamfusion.proto.plan.v1.WindowAggregate;
import tech.streamfusion.proto.plan.v1.WindowKind;
import tech.streamfusion.proto.plan.v1.WindowProperty;

class StreamFusionArrowWindowAggregateOperatorTest {
    private static final int MAX_PARALLELISM = 128;
    private static final RowType INPUT_TYPE = RowType.of(
            new LogicalType[] {new BigIntType(false), new TimestampType(false, 3)}, new String[] {"key", "ts"});
    private static final RowType OUTPUT_TYPE = RowType.of(
            new LogicalType[] {
                new BigIntType(false), new BigIntType(false), new TimestampType(false, 3), new TimestampType(false, 3)
            },
            new String[] {"key", "count", "window_start", "window_end"});

    @Test
    void pendingTimersAndStateRestoreAcrossEveryBackendAndCheckpointFormat() throws Exception {
        try (RootAllocator inputs = new RootAllocator(64L << 20)) {
            for (boolean sourceRocks : new boolean[] {false, true}) {
                for (boolean targetRocks : new boolean[] {false, true}) {
                    for (SnapshotKind kind : SnapshotKind.values()) {
                        if (kind != SnapshotKind.CANONICAL && sourceRocks != targetRocks) {
                            continue;
                        }
                        OperatorSubtaskState snapshot;
                        try (Harness source = harness(null, sourceRocks)) {
                            process(source.harness, inputs, row(7, 1_000));
                            assertThat(takeOutputCount(source.harness)).isZero();
                            snapshot = snapshot(source.harness, kind);
                        }
                        try (Harness target = harness(snapshot, targetRocks)) {
                            target.harness.processWatermark(new Watermark(9_999));
                            assertThat(takeOutputCount(target.harness)).isEqualTo(1);
                        }
                    }
                }
            }
        }
    }

    @Test
    void rocksWindowCheckpointsReuseSstsAndRestorePendingTimers() throws Exception {
        try (RootAllocator inputs = new RootAllocator(64L << 20)) {
            OperatorSubtaskState second;
            try (Harness source = harness(null, true)) {
                process(source.harness, inputs, row(7, 1_000));
                OperatorSubtaskState first = source.harness.snapshot(20, 20);
                IncrementalRemoteKeyedStateHandle firstHandle = incremental(first);
                source.harness.notifyOfCompletedCheckpoint(20);
                second = source.harness.snapshot(21, 21);
                IncrementalRemoteKeyedStateHandle secondHandle = incremental(second);
                assertThat(secondHandle.getSharedState()).hasSameSizeAs(firstHandle.getSharedState());
                assertThat(secondHandle.getCheckpointedSize()).isLessThan(firstHandle.getCheckpointedSize());
            }
            try (Harness restored = harness(second, true)) {
                restored.harness.processWatermark(new Watermark(9_999));
                assertThat(takeOutputCount(restored.harness)).isEqualTo(1);
            }
        }
    }

    @Test
    void processingTimeWindowsRegisterAndFireFlinkTimers() throws Exception {
        try (RootAllocator inputs = new RootAllocator(64L << 20);
                Harness harness = harness(1, 0, null, false, true)) {
            harness.harness.setProcessingTime(1_000);
            process(harness.harness, inputs, row(7, 0));
            assertThat(takeOutputCount(harness.harness)).isZero();
            harness.harness.setProcessingTime(9_999);
            assertThat(takeOutputCount(harness.harness)).isEqualTo(1);
        }
    }

    @Test
    void redistributesPendingWindowStateAndTimersOneToTwoAndBack() throws Exception {
        try (RootAllocator inputs = new RootAllocator(64L << 20)) {
            RowDataKeySelector selector = selector();
            Map<Integer, GenericRowData> rows = rowsForOwners(selector, 2, 1_000);
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
                        GenericRowData ownerRow = rows.get(subtask);
                        process(scaled.harness, inputs, row(ownerRow.getLong(0), 11_000));
                        scaled.harness.processWatermark(new Watermark(9_999));
                        firstWindowRows += takeOutputCount(scaled.harness);
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
                    assertThat(takeOutputCount(back.harness)).isEqualTo(2);
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

    private static void process(
            KeyedOneInputStreamOperatorTestHarness<RowData, ArrowRowDataBatch, ArrowRowDataBatch> harness,
            RootAllocator allocator,
            GenericRowData row)
            throws Exception {
        try (ArrowRowDataBatch batch = ArrowRowDataBatch.transpose(List.of(row), INPUT_TYPE, allocator)
                .withEnvelope(new RowKind[] {RowKind.INSERT}, new boolean[] {true}, new long[] {
                    row.getTimestamp(1, 3).getMillisecond()
                })) {
            harness.processElement(new StreamRecord<>(batch));
        }
    }

    private static int takeOutputCount(
            KeyedOneInputStreamOperatorTestHarness<RowData, ArrowRowDataBatch, ArrowRowDataBatch> harness) {
        int rows = 0;
        Object output;
        while ((output = harness.getOutput().poll()) != null) {
            if (!(output instanceof StreamRecord)) {
                continue;
            }
            @SuppressWarnings("unchecked")
            ArrowRowDataBatch batch = ((StreamRecord<ArrowRowDataBatch>) output).getValue();
            rows += batch.size();
        }
        return rows;
    }

    private static Harness harness(OperatorSubtaskState state, boolean rocks) throws Exception {
        return harness(1, 0, state, rocks);
    }

    private static Harness harness(int parallelism, int subtask, OperatorSubtaskState state, boolean rocks)
            throws Exception {
        return harness(parallelism, subtask, state, rocks, false);
    }

    private static Harness harness(
            int parallelism, int subtask, OperatorSubtaskState state, boolean rocks, boolean processingTime)
            throws Exception {
        RowDataKeySelector selector = selector();
        StreamFusionArrowWindowAggregateOperator operator = new StreamFusionArrowWindowAggregateOperator(
                INPUT_TYPE, OUTPUT_TYPE, new int[] {0}, plan(processingTime), false, processingTime, selector);
        KeyedOneInputStreamOperatorTestHarness<RowData, ArrowRowDataBatch, ArrowRowDataBatch> harness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        operator,
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

    private static RowDataKeySelector selector() {
        return KeySelectorUtil.getRowDataSelector(
                StreamFusionArrowWindowAggregateOperatorTest.class.getClassLoader(),
                new int[] {0},
                InternalTypeInfo.of(INPUT_TYPE));
    }

    private static Map<Integer, GenericRowData> rowsForOwners(
            RowDataKeySelector selector, int parallelism, long timestamp) throws Exception {
        Map<Integer, GenericRowData> rows = new HashMap<>();
        for (long key = 0; rows.size() < parallelism; key++) {
            GenericRowData row = row(key, timestamp);
            int owner = KeyGroupRangeAssignment.assignKeyToParallelOperator(
                    selector.getKey(row), MAX_PARALLELISM, parallelism);
            rows.putIfAbsent(owner, row);
        }
        return rows;
    }

    private static GenericRowData row(long key, long timestamp) {
        return GenericRowData.of(key, TimestampData.fromEpochMillis(timestamp));
    }

    private static byte[] plan() {
        return plan(false);
    }

    private static byte[] plan(boolean processingTime) {
        tech.streamfusion.proto.plan.v1.LogicalType bigint = tech.streamfusion.proto.plan.v1.LogicalType.newBuilder()
                .setBigint(EmptyType.getDefaultInstance())
                .build();
        tech.streamfusion.proto.plan.v1.LogicalType timestamp = tech.streamfusion.proto.plan.v1.LogicalType.newBuilder()
                .setTimestamp(PrecisionType.newBuilder().setPrecision(3))
                .build();
        Schema inputSchema = Schema.newBuilder()
                .addFields(Field.newBuilder().setName("key").setType(bigint))
                .addFields(Field.newBuilder().setName("ts").setType(timestamp))
                .build();
        Schema outputSchema = Schema.newBuilder()
                .addFields(Field.newBuilder().setName("key").setType(bigint))
                .addFields(Field.newBuilder().setName("count").setType(bigint))
                .addFields(Field.newBuilder().setName("window_start").setType(timestamp))
                .addFields(Field.newBuilder().setName("window_end").setType(timestamp))
                .build();
        WindowAggregate aggregate = WindowAggregate.newBuilder()
                .setInput(Operator.newBuilder().setInput(Input.newBuilder()))
                .addGroupingIndices(0)
                .addAggregateCalls(AggregateCall.newBuilder()
                        .setFunction(AggregateFunction.AGGREGATE_FUNCTION_COUNT_STAR)
                        .setOutputType(bigint))
                .setTimeAttributeIndex(1)
                .setKind(WindowKind.WINDOW_KIND_TUMBLE)
                .setSizeMillis(10_000)
                .setProcessingTime(processingTime)
                .setShiftTimeZone("UTC")
                .addWindowProperties(WindowProperty.WINDOW_PROPERTY_START)
                .addWindowProperties(WindowProperty.WINDOW_PROPERTY_END)
                .setInputSchema(inputSchema)
                .setOutputSchema(outputSchema)
                .build();
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setWindowAggregate(aggregate))
                .build()
                .toByteArray();
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
