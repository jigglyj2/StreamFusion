/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.deduplicate;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.runtime.checkpoint.metadata.MetadataV3Serializer;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle.HandleAndLocalPath;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;
import org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFinalizer;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.table.planner.codegen.EqualiserCodeGenerator;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.deduplicate.ProcTimeDeduplicateKeepLastRowFunction;
import org.apache.flink.table.runtime.operators.deduplicate.RowTimeDeduplicateFunction;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.state.StreamFusionStateBackend;

/** Q18-shaped RowData parity tests against Flink's row-time deduplicate implementation. */
class StreamFusionDeduplicateOperatorTest {
    private static final int MAX_PARALLELISM = 128;
    private static final int[] UNIQUE_KEYS = {1, 0};
    private static final int ORDER_INDEX = 5;
    private static final RowType INPUT_TYPE = RowType.of(
            new LogicalType[] {
                new BigIntType(false),
                new BigIntType(false),
                new BigIntType(false),
                new VarCharType(false, VarCharType.MAX_LENGTH),
                new VarCharType(false, VarCharType.MAX_LENGTH),
                new TimestampType(false, TimestampKind.ROWTIME, 3),
                new VarCharType(false, VarCharType.MAX_LENGTH)
            },
            new String[] {"auction", "bidder", "price", "channel", "url", "dateTime", "extra"});
    private static final InternalTypeInfo<RowData> INPUT_INFO = InternalTypeInfo.of(INPUT_TYPE);

    @Test
    void matchesFlinkQ18ChangelogAcrossRawKeyedCheckpointRestore() throws Exception {
        Harnesses before = Harnesses.open();
        OperatorSubtaskState nativeSnapshot;
        OperatorSubtaskState flinkSnapshot;
        try {
            processBoth(before, bid(10, 7, 100, 100, "first"));
            processBoth(before, bid(10, 7, 90, 90, "older"));
            processBoth(before, bid(10, 7, 101, 100, "equal-time-wins"));
            processBoth(before, bid(10, 7, 200, 200, "newer"));
            processBoth(before, bid(11, 7, 150, 150, "other-auction"));

            nativeSnapshot = before.nativeHarness.snapshot(1L, 1L);
            flinkSnapshot = before.flinkHarness.snapshot(1L, 1L);
            assertThat(takeOutput(before.nativeHarness)).isEqualTo(takeOutput(before.flinkHarness));
        } finally {
            before.close();
        }

        Harnesses after = Harnesses.restore(nativeSnapshot, flinkSnapshot);
        try {
            processBoth(after, bid(10, 7, 175, 175, "older-after-restore"));
            processBoth(after, bid(10, 7, 300, 300, "latest-after-restore"));
            processBoth(after, bid(10, 8, 250, 250, "other-bidder"));
            after.nativeHarness.processWatermark(new Watermark(400));
            after.flinkHarness.processWatermark(new Watermark(400));

            assertThat(takeOutput(after.nativeHarness)).isEqualTo(takeOutput(after.flinkHarness));
        } finally {
            after.close();
        }
    }

    @Test
    void redistributesRawKeyGroupsFromOneToTwoAndBackToOne() throws Exception {
        assertRedistributesKeyGroups(false);
    }

    @Test
    void redistributesNativeRocksKeyGroupsFromOneToTwoAndBackToOne() throws Exception {
        assertRedistributesKeyGroups(true);
    }

    @Test
    void emitsFlinkIncrementalRocksHandlesAndReusesUnchangedSstFiles() throws Exception {
        OperatorSubtaskState secondSnapshot;
        try (KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> rocks =
                nativeHarness(1, 0, null, true)) {
            rocks.processElement(new StreamRecord<>(bid(10, 7, 100, 100, "first")));
            OperatorSubtaskState firstSnapshot = rocks.snapshot(5L, 5L);
            IncrementalRemoteKeyedStateHandle first = incrementalHandle(firstSnapshot);
            assertThat(firstSnapshot.getRawKeyedState()).isEmpty();
            assertThat(first.getSharedState()).isNotEmpty();

            rocks.notifyOfCompletedCheckpoint(5L);
            secondSnapshot = rocks.snapshot(6L, 6L);
            IncrementalRemoteKeyedStateHandle second = incrementalHandle(secondSnapshot);
            assertThat(sharedHandles(second)).isEqualTo(sharedHandles(first));
            assertThat(second.getCheckpointedSize()).isLessThan(first.getCheckpointedSize());
            IncrementalRemoteKeyedStateHandle roundTripped = metadataRoundTrip(second);
            assertThat(sharedHandles(roundTripped)).isEqualTo(sharedHandles(second));
            secondSnapshot = secondSnapshot.toBuilder()
                    .setManagedKeyedState(roundTripped)
                    .build();
        }

        try (KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> restored =
                nativeHarness(1, 0, secondSnapshot, true)) {
            restored.processElement(new StreamRecord<>(bid(10, 7, 90, 90, "older")));
            restored.endInput();
            assertThat(takeRowKinds(restored)).isEmpty();
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

    private static void assertRedistributesKeyGroups(boolean rocksDb) throws Exception {
        Map<Integer, GenericRowData> rowsByFutureSubtask = rowsForEverySubtask(2);
        OperatorSubtaskState oneSubtaskSnapshot;
        try (KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> initial =
                nativeHarness(1, 0, null, rocksDb)) {
            for (GenericRowData row : rowsByFutureSubtask.values()) {
                initial.processElement(new StreamRecord<>(copy(row)));
            }
            oneSubtaskSnapshot = initial.snapshot(10L, 10L);
            assertThat(takeRowKinds(initial)).containsExactly("+I", "+I");
        }

        OperatorSubtaskState packagedOne = AbstractStreamOperatorTestHarness.repackageState(oneSubtaskSnapshot);
        List<OperatorSubtaskState> twoSubtaskSnapshots = new ArrayList<>();
        for (int subtask = 0; subtask < 2; subtask++) {
            OperatorSubtaskState assigned = AbstractStreamOperatorTestHarness.repartitionOperatorState(
                    packagedOne, MAX_PARALLELISM, 1, 2, subtask);
            try (KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> scaled =
                    nativeHarness(2, subtask, assigned, rocksDb)) {
                GenericRowData original = rowsByFutureSubtask.get(subtask);
                scaled.processElement(new StreamRecord<>(withTimeAndPrice(original, 90, 90)));
                scaled.snapshot(11L, 11L);
                assertThat(takeRowKinds(scaled)).isEmpty();

                scaled.processElement(new StreamRecord<>(withTimeAndPrice(original, 200, 200)));
                twoSubtaskSnapshots.add(scaled.snapshot(12L, 12L));
                assertThat(takeRowKinds(scaled)).containsExactly("+U");
            }
        }

        OperatorSubtaskState packagedTwo = AbstractStreamOperatorTestHarness.repackageState(
                twoSubtaskSnapshots.toArray(new OperatorSubtaskState[0]));
        OperatorSubtaskState assignedBack =
                AbstractStreamOperatorTestHarness.repartitionOperatorState(packagedTwo, MAX_PARALLELISM, 2, 1, 0);
        try (KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> scaledBack =
                nativeHarness(1, 0, assignedBack, rocksDb)) {
            for (GenericRowData original : rowsByFutureSubtask.values()) {
                scaledBack.processElement(new StreamRecord<>(withTimeAndPrice(original, 150, 150)));
            }
            scaledBack.snapshot(13L, 13L);
            assertThat(takeRowKinds(scaledBack)).isEmpty();

            for (GenericRowData original : rowsByFutureSubtask.values()) {
                scaledBack.processElement(new StreamRecord<>(withTimeAndPrice(original, 300, 300)));
            }
            scaledBack.snapshot(14L, 14L);
            assertThat(takeRowKinds(scaledBack)).containsExactly("+U", "+U");
        }
    }

    @Test
    void restoresCanonicalRawStateFromMemoryToNativeRocksDbAndBack() throws Exception {
        OperatorSubtaskState memorySnapshot;
        try (KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> memory =
                nativeHarness(1, 0, null, false)) {
            memory.processElement(new StreamRecord<>(bid(10, 7, 100, 100, "memory")));
            memorySnapshot = memory.snapshot(20L, 20L);
            assertThat(takeRowKinds(memory)).containsExactly("+I");
        }

        OperatorSubtaskState rocksSnapshot;
        try (KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> rocks =
                nativeHarness(1, 0, memorySnapshot, true)) {
            rocks.processElement(new StreamRecord<>(bid(10, 7, 90, 90, "older-in-rocks")));
            assertThat(takeRowKinds(rocks)).isEmpty();
            rocks.processElement(new StreamRecord<>(bid(10, 7, 200, 200, "newer-in-rocks")));
            rocksSnapshot = rocks.snapshot(21L, 21L);
            assertThat(takeRowKinds(rocks)).containsExactly("+U");
        }

        try (KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> restoredMemory =
                nativeHarness(1, 0, rocksSnapshot, false)) {
            restoredMemory.processElement(new StreamRecord<>(bid(10, 7, 150, 150, "older-back-in-memory")));
            assertThat(takeRowKinds(restoredMemory)).isEmpty();
            restoredMemory.processElement(new StreamRecord<>(bid(10, 7, 300, 300, "newer-back-in-memory")));
            restoredMemory.snapshot(22L, 22L);
            assertThat(takeRowKinds(restoredMemory)).containsExactly("+U");
        }
    }

    @Test
    void canonicalSavepointMovesFromNativeRocksToNativeMemory() throws Exception {
        OperatorSubtaskState savepoint;
        try (KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> rocks =
                nativeHarness(1, 0, null, true)) {
            rocks.processElement(new StreamRecord<>(bid(10, 7, 100, 100, "rocks")));
            savepoint = rocks.snapshotWithLocalState(32L, 32L, SavepointType.savepoint(SavepointFormatType.CANONICAL))
                    .getJobManagerOwnedState();
            assertThat(savepoint.getRawKeyedState()).hasSize(1);
        }
        try (KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> memory =
                nativeHarness(1, 0, savepoint, false)) {
            memory.processElement(new StreamRecord<>(bid(10, 7, 90, 90, "older")));
            memory.endInput();
            assertThat(takeRowKinds(memory)).isEmpty();
        }
    }

    @Test
    void restoresBufferedStateFromAlignedAndUnalignedCheckpoints() throws Exception {
        for (boolean rocksDb : new boolean[] {false, true}) {
            for (boolean unaligned : new boolean[] {false, true}) {
                OperatorSubtaskState snapshot;
                try (KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> before =
                        nativeHarness(1, 0, null, rocksDb)) {
                    before.processElement(new StreamRecord<>(bid(10, 7, 100, 100, "before")));
                    snapshot = snapshotWithAlignment(before, unaligned ? 41L : 40L, unaligned);
                    assertThat(takeRowKinds(before)).containsExactly("+I");
                }
                try (KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> after =
                        nativeHarness(1, 0, snapshot, rocksDb)) {
                    after.processElement(new StreamRecord<>(bid(10, 7, 90, 90, "older")));
                    after.endInput();
                    assertThat(takeRowKinds(after)).isEmpty();
                }
            }
        }
    }

    @Test
    void matchesFlinkLastRowDeduplicateForInputRetractionsAndUpdateBeforeOutput() throws Exception {
        for (boolean rocksDb : new boolean[] {false, true}) {
            Q18KeySelector nativeSelector = new Q18KeySelector();
            Q18KeySelector flinkSelector = new Q18KeySelector();
            KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> nativeHarness = unopenedNativeHarness(
                    new StreamFusionDeduplicateOperator(INPUT_TYPE, UNIQUE_KEYS, ORDER_INDEX, true, true, true),
                    nativeSelector,
                    rocksDb);
            ProcTimeDeduplicateKeepLastRowFunction flinkFunction = new ProcTimeDeduplicateKeepLastRowFunction(
                    INPUT_INFO,
                    0L,
                    true,
                    true,
                    false,
                    new EqualiserCodeGenerator(
                                    INPUT_TYPE.getChildren().toArray(new LogicalType[0]),
                                    Thread.currentThread().getContextClassLoader())
                            .generateRecordEqualiser("StreamFusionRetractionParity"),
                    null);
            KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> flinkHarness =
                    Harnesses.harness(new KeyedProcessOperator<>(flinkFunction), flinkSelector, 1, 0);
            try {
                nativeHarness.open();
                flinkHarness.open();
                processChangelogBoth(nativeHarness, flinkHarness, bid(RowKind.INSERT, 10, 7, 100, 100));
                processChangelogBoth(nativeHarness, flinkHarness, bid(RowKind.UPDATE_AFTER, 10, 7, 200, 200));
                processChangelogBoth(nativeHarness, flinkHarness, bid(RowKind.UPDATE_BEFORE, 10, 7, 0, 0));
                processChangelogBoth(nativeHarness, flinkHarness, bid(RowKind.DELETE, 10, 7, 0, 0));
                processChangelogBoth(nativeHarness, flinkHarness, bid(RowKind.INSERT, 10, 7, 300, 300));
                nativeHarness.endInput();

                assertThat(takeOutput(nativeHarness)).isEqualTo(takeOutput(flinkHarness));
            } finally {
                try {
                    nativeHarness.close();
                } finally {
                    flinkHarness.close();
                }
            }
        }
    }

    @Test
    void matchesFlinkRowtimeUpdateBeforeOutput() throws Exception {
        Q18KeySelector nativeSelector = new Q18KeySelector();
        Q18KeySelector flinkSelector = new Q18KeySelector();
        try (KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> nativeHarness = Harnesses.harness(
                        new StreamFusionDeduplicateOperator(INPUT_TYPE, UNIQUE_KEYS, ORDER_INDEX, true, false, true),
                        nativeSelector,
                        1,
                        0);
                KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> flinkHarness = Harnesses.harness(
                        new KeyedProcessOperator<>(
                                new RowTimeDeduplicateFunction(INPUT_INFO, 0L, ORDER_INDEX, true, true, true)),
                        flinkSelector,
                        1,
                        0)) {
            nativeHarness.open();
            flinkHarness.open();
            processRowtimeBoth(nativeHarness, flinkHarness, bid(10, 7, 100, 100, "first"));
            processRowtimeBoth(nativeHarness, flinkHarness, bid(10, 7, 200, 200, "second"));
            nativeHarness.endInput();
            assertThat(takeOutput(nativeHarness)).isEqualTo(takeOutput(flinkHarness));
        }
    }

    private static void processRowtimeBoth(
            KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> nativeHarness,
            KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> flinkHarness,
            GenericRowData row)
            throws Exception {
        nativeHarness.processElement(new StreamRecord<>(copy(row)));
        flinkHarness.processElement(
                new StreamRecord<>(INPUT_INFO.toRowSerializer().toBinaryRow(row).copy()));
    }

    private static void processChangelogBoth(
            KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> nativeHarness,
            KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> flinkHarness,
            GenericRowData row)
            throws Exception {
        nativeHarness.processElement(new StreamRecord<>(copy(row)));
        flinkHarness.processElement(new StreamRecord<>(copy(row)));
    }

    private static void processBoth(Harnesses harnesses, GenericRowData row) throws Exception {
        long timestamp = row.getTimestamp(ORDER_INDEX, 3).getMillisecond();
        harnesses.nativeHarness.processElement(new StreamRecord<>(copy(row), timestamp));
        // Flink's row-time deduplicate reads the compact timestamp slot with getLong(). Production
        // table pipelines provide BinaryRowData, whose compact TIMESTAMP(3) representation supports
        // both getLong() and getTimestamp(); GenericRowData only supports the latter.
        RowData flinkRow = INPUT_INFO.toRowSerializer().toBinaryRow(row).copy();
        harnesses.flinkHarness.processElement(new StreamRecord<>(flinkRow, timestamp));
    }

    private static GenericRowData bid(long auction, long bidder, long price, long eventTime, String extra) {
        GenericRowData row = new GenericRowData(RowKind.INSERT, 7);
        row.setField(0, auction);
        row.setField(1, bidder);
        row.setField(2, price);
        row.setField(3, StringData.fromString("channel"));
        row.setField(4, StringData.fromString("url"));
        row.setField(5, TimestampData.fromEpochMillis(eventTime));
        row.setField(6, StringData.fromString(extra));
        return row;
    }

    private static GenericRowData bid(RowKind kind, long auction, long bidder, long price, long eventTime) {
        GenericRowData row = bid(auction, bidder, price, eventTime, "changelog");
        row.setRowKind(kind);
        return row;
    }

    private static RowData copy(RowData row) {
        return INPUT_INFO.toRowSerializer().copy(row);
    }

    private static GenericRowData withTimeAndPrice(GenericRowData original, long price, long time) {
        return bid(original.getLong(0), original.getLong(1), price, time, "rescaled");
    }

    private static Map<Integer, GenericRowData> rowsForEverySubtask(int parallelism) throws Exception {
        Q18KeySelector selector = new Q18KeySelector();
        Map<Integer, GenericRowData> rows = new HashMap<>();
        for (long bidder = 0; rows.size() < parallelism; bidder++) {
            GenericRowData row = bid(10, bidder, 100, 100, "initial");
            int owner = KeyGroupRangeAssignment.assignKeyToParallelOperator(
                    selector.getKey(row), MAX_PARALLELISM, parallelism);
            rows.putIfAbsent(owner, row);
        }
        return rows;
    }

    private static List<String> takeRowKinds(
            KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness) {
        List<String> kinds = new ArrayList<>();
        Object value;
        while ((value = harness.getOutput().poll()) != null) {
            if (value instanceof StreamRecord) {
                @SuppressWarnings("unchecked")
                StreamRecord<RowData> record = (StreamRecord<RowData>) value;
                kinds.add(record.getValue().getRowKind().shortString());
            }
        }
        return kinds;
    }

    private static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> nativeHarness(
            int parallelism, int subtask, OperatorSubtaskState state) throws Exception {
        return nativeHarness(parallelism, subtask, state, false);
    }

    private static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> nativeHarness(
            int parallelism, int subtask, OperatorSubtaskState state, boolean rocksDb) throws Exception {
        Q18KeySelector selector = new Q18KeySelector();
        KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        new StreamFusionDeduplicateOperator(INPUT_TYPE, UNIQUE_KEYS, ORDER_INDEX, true),
                        selector,
                        selector.getProducedType(),
                        MAX_PARALLELISM,
                        parallelism,
                        subtask);
        if (rocksDb) {
            harness.setStateBackend(new StreamFusionStateBackend(new EmbeddedRocksDBStateBackend(true)));
        } else {
            harness.setStateBackend(new StreamFusionStateBackend(new HashMapStateBackend()));
        }
        harness.setup(INPUT_INFO.toSerializer());
        if (state != null) {
            harness.initializeState(state);
        }
        harness.open();
        return harness;
    }

    private static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> unopenedNativeHarness(
            OneInputStreamOperator<RowData, RowData> operator, Q18KeySelector selector, boolean rocksDb)
            throws Exception {
        KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        operator, selector, selector.getProducedType(), MAX_PARALLELISM, 1, 0);
        if (rocksDb) {
            harness.setStateBackend(new StreamFusionStateBackend(new EmbeddedRocksDBStateBackend(true)));
        } else {
            harness.setStateBackend(new StreamFusionStateBackend(new HashMapStateBackend()));
        }
        harness.setup(INPUT_INFO.toSerializer());
        return harness;
    }

    private static List<String> takeOutput(KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness) {
        List<String> output = new ArrayList<>();
        Queue<Object> queue = harness.getOutput();
        Object value;
        while ((value = queue.poll()) != null) {
            if (value instanceof Watermark) {
                output.add("watermark:" + ((Watermark) value).getTimestamp());
                continue;
            }
            @SuppressWarnings("unchecked")
            StreamRecord<RowData> record = (StreamRecord<RowData>) value;
            try {
                DataOutputSerializer bytes = new DataOutputSerializer(128);
                INPUT_INFO.toRowSerializer().serialize(record.getValue(), bytes);
                output.add("record:"
                        + (record.hasTimestamp() ? Long.toString(record.getTimestamp()) : "no-timestamp")
                        + ":"
                        + Base64.getEncoder().encodeToString(bytes.getCopyOfBuffer()));
            } catch (IOException error) {
                throw new IllegalStateException("Could not serialize parity output", error);
            }
        }
        return output;
    }

    private static OperatorSubtaskState snapshotWithAlignment(
            KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness,
            long checkpointId,
            boolean unaligned)
            throws Exception {
        CheckpointStorageLocationReference location = CheckpointStorageLocationReference.getDefault();
        CheckpointOptions options = unaligned
                ? CheckpointOptions.unaligned(CheckpointType.CHECKPOINT, location)
                : CheckpointOptions.alignedNoTimeout(CheckpointType.CHECKPOINT, location);
        return OperatorSnapshotFinalizer.create(harness.getOperator()
                        .snapshotState(checkpointId, checkpointId, options, new MemCheckpointStreamFactory(32 << 20)))
                .getJobManagerOwnedState();
    }

    private static final class Harnesses implements AutoCloseable {
        private final KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> nativeHarness;
        private final KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> flinkHarness;

        private Harnesses(
                KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> nativeHarness,
                KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> flinkHarness) {
            this.nativeHarness = nativeHarness;
            this.flinkHarness = flinkHarness;
        }

        private static Harnesses open() throws Exception {
            Harnesses harnesses = create();
            harnesses.nativeHarness.open();
            harnesses.flinkHarness.open();
            return harnesses;
        }

        private static Harnesses restore(OperatorSubtaskState nativeSnapshot, OperatorSubtaskState flinkSnapshot)
                throws Exception {
            Harnesses harnesses = create();
            harnesses.nativeHarness.initializeState(nativeSnapshot);
            harnesses.flinkHarness.initializeState(flinkSnapshot);
            harnesses.nativeHarness.open();
            harnesses.flinkHarness.open();
            return harnesses;
        }

        private static Harnesses create() throws Exception {
            Q18KeySelector nativeSelector = new Q18KeySelector();
            Q18KeySelector flinkSelector = new Q18KeySelector();
            KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> nativeHarness = harness(
                    new StreamFusionDeduplicateOperator(INPUT_TYPE, UNIQUE_KEYS, ORDER_INDEX, true),
                    nativeSelector,
                    1,
                    0);
            RowTimeDeduplicateFunction flinkFunction =
                    new RowTimeDeduplicateFunction(INPUT_INFO, 0L, ORDER_INDEX, false, true, true);
            KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> flinkHarness =
                    harness(new KeyedProcessOperator<>(flinkFunction), flinkSelector, 1, 0);
            return new Harnesses(nativeHarness, flinkHarness);
        }

        private static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness(
                OneInputStreamOperator<RowData, RowData> operator,
                Q18KeySelector selector,
                int parallelism,
                int subtask)
                throws Exception {
            KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness =
                    new KeyedOneInputStreamOperatorTestHarness<>(
                            operator, selector, selector.getProducedType(), MAX_PARALLELISM, parallelism, subtask);
            harness.setup(INPUT_INFO.toSerializer());
            return harness;
        }

        @Override
        public void close() throws Exception {
            try {
                nativeHarness.close();
            } finally {
                flinkHarness.close();
            }
        }
    }

    /** Matches the BinaryRowData key that the planner installs on the transformation. */
    private static final class Q18KeySelector implements RowDataKeySelector {
        private static final long serialVersionUID = 1L;
        private static final InternalTypeInfo<RowData> KEY_TYPE =
                InternalTypeInfo.ofFields(new BigIntType(false), new BigIntType(false));

        @Override
        public RowData getKey(RowData value) {
            BinaryRowData key = new BinaryRowData(2);
            BinaryRowWriter writer = new BinaryRowWriter(key);
            writer.writeLong(0, value.getLong(1));
            writer.writeLong(1, value.getLong(0));
            writer.complete();
            return key;
        }

        @Override
        public InternalTypeInfo<RowData> getProducedType() {
            return KEY_TYPE;
        }

        @Override
        public RowDataKeySelector copy() {
            return new Q18KeySelector();
        }
    }
}
