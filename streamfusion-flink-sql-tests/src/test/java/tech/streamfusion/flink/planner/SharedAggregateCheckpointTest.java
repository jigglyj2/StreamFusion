/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFinalizer;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;

/** Flink owns snapshot/restore; the ordinary shared region owns the complete Calc/Aggregate/Calc tree. */
class SharedAggregateCheckpointTest {
    @Test
    void sharedRegionRestoresCanonicalAndAlignedAndUnalignedStateWithSqlChangelogParity() throws Exception {
        for (boolean rocks : List.of(false, true))
            for (int mode = 0; mode < 3; mode++) {
                var live = new ArrayList<GenericRowData>();
                try (var oracle = SharedAggregateFlinkOracle.create();
                        var allocator = new RootAllocator(64L << 20)) {
                    OperatorSubtaskState snapshot;
                    try (var source = new SharedAggregateRuntimeHarness(rocks, null)) {
                        for (int seed = 0; seed < 3; seed++) compare(source, oracle, allocator, live, seed);
                        source.processWatermark(0, new Watermark(1000));
                        assertThat(source.controls).containsExactly(new Watermark(1000));
                        snapshot = snapshot(source, mode, 1);
                        if (rocks && mode != 0) {
                            assertThat(snapshot.getRawKeyedState()).isEmpty();
                            var first = (IncrementalRemoteKeyedStateHandle)
                                    snapshot.getManagedKeyedState().iterator().next();
                            source.notifyOfCompletedCheckpoint(1);
                            var second = snapshot(source, mode, 2);
                            var next = (IncrementalRemoteKeyedStateHandle)
                                    second.getManagedKeyedState().iterator().next();
                            assertThat(next.getCheckpointedSize()).isLessThan(first.getCheckpointedSize());
                        } else assertThat(snapshot.getRawKeyedState()).hasSize(1);
                    }
                    try (var target = new SharedAggregateRuntimeHarness(mode == 0 ? !rocks : rocks, snapshot)) {
                        for (int seed = 3; seed < 6; seed++) compare(target, oracle, allocator, live, seed);
                    }
                    assertThat(allocator.getAllocatedMemory()).isZero();
                }
            }
    }

    static OperatorSubtaskState snapshot(SharedAggregateRuntimeHarness harness, int mode, long id) throws Exception {
        if (mode == 0)
            return harness.snapshotWithLocalState(id, id, SavepointType.savepoint(SavepointFormatType.CANONICAL))
                    .getJobManagerOwnedState();
        var location = CheckpointStorageLocationReference.getDefault();
        var options = mode == 1
                ? CheckpointOptions.alignedNoTimeout(CheckpointType.CHECKPOINT, location)
                : CheckpointOptions.unaligned(CheckpointType.CHECKPOINT, location);
        return OperatorSnapshotFinalizer.create(
                        harness.region().snapshotState(id, id, options, new MemCheckpointStreamFactory(64 << 20)))
                .getJobManagerOwnedState();
    }

    private static void compare(
            SharedAggregateRuntimeHarness target,
            KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> oracle,
            RootAllocator allocator,
            List<GenericRowData> live,
            int seed)
            throws Exception {
        var random = new Random(seed);
        var rows = new ArrayList<RowData>();
        var kinds = new RowKind[32];
        var present = new boolean[kinds.length];
        var timestamps = new long[kinds.length];
        for (int i = 0; i < kinds.length; i++) {
            GenericRowData row;
            if (!live.isEmpty() && random.nextBoolean()) {
                row = live.remove(random.nextInt(live.size()));
                row.setRowKind(i % 2 == 0 ? RowKind.DELETE : RowKind.UPDATE_BEFORE);
            } else {
                row = GenericRowData.of(
                        i % 7 == 0 ? null : StringData.fromString("é-" + random.nextInt(7)),
                        i % 4 == 0 ? null : (long) random.nextInt(19) - 9);
                row.setRowKind(i % 2 == 0 ? RowKind.INSERT : RowKind.UPDATE_AFTER);
                live.add(GenericRowData.of(row.getField(0), row.getField(1)));
            }
            rows.add(row);
            kinds[i] = row.getRowKind();
            present[i] = i % 3 != 0;
            timestamps[i] = 1000L * seed + i;
            oracle.processElement(present[i] ? new StreamRecord<>(row, timestamps[i]) : new StreamRecord<>(row));
        }
        var expected = new DataOutputSerializer(128);
        var expectedTimes = new ArrayList<Long>();
        var serializer = new RowDataSerializer(SharedAggregateFlinkOracle.OUTPUT);
        for (var record : oracle.extractOutputStreamRecords()) {
            serializer.serialize(record.getValue(), expected);
            expectedTimes.add(record.hasTimestamp() ? record.getTimestamp() : null);
        }
        oracle.getOutput().clear();
        try (var batch = ArrowRowDataBatch.transpose(rows, SharedAggregateFlinkOracle.INPUT, allocator)
                .withEnvelope(kinds, present, timestamps)) {
            target.processElement(0, new StreamRecord<>(batch));
        }
        assertThat(target.captured.getCopyOfBuffer()).isEqualTo(expected.getCopyOfBuffer());
        assertThat(target.times).isEqualTo(expectedTimes);
        target.captured.clear();
        target.times.clear();
    }
}
