/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.window.tvf.common.WindowAggOperator;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;

class GlobalWindowFlinkControlContractTest {
    // Flink's local stage emits a BinaryRow with a BIGINT slice-end slot. Its global
    // slice assigner reads that compact slot through getTimestamp(3), while the buffer
    // serializer reads getLong. Match the actual binary exchange representation.
    private static final org.apache.flink.table.runtime.typeutils.RowDataSerializer PARTIAL_SERIALIZER =
            new org.apache.flink.table.runtime.typeutils.RowDataSerializer(
                    org.apache.flink.table.types.logical.RowType.of(
                            new org.apache.flink.table.types.logical.BigIntType(),
                            new org.apache.flink.table.types.logical.BigIntType(false),
                            new org.apache.flink.table.types.logical.BigIntType(false)));

    @Test
    void storesOneSharedSliceAndDropsOnlyInputsWhoseLastWindowHasFired() throws Exception {
        for (boolean rocks : List.of(false, true)) {
            OperatorSubtaskState snapshot;
            try (var harness = GlobalWindowFlinkOracle.create(rocks, null)) {
                input(harness, 1, 2, 2000);
                assertThat(harness.numEventTimeTimers()).isZero();
                if (!rocks) assertThat(harness.numKeyedStateEntries()).isZero();
                harness.prepareSnapshotPreBarrier(1);
                assertThat(harness.numEventTimeTimers()).isEqualTo(1);
                if (!rocks) assertThat(harness.numKeyedStateEntries()).isEqualTo(1);
                harness.processWatermark(new Watermark(1999));
                assertThat(drain(harness)).containsExactly("1:2:-4000:2000");
                // This key has no earlier input. Its base slice has fired, but two HOP windows remain.
                input(harness, 2, 3, 2000);
                assertThat(late(harness)).isZero();
                assertThat(harness.numEventTimeTimers()).isEqualTo(2);
                harness.prepareSnapshotPreBarrier(2);
                if (!rocks) assertThat(harness.numKeyedStateEntries()).isEqualTo(2);
                snapshot = harness.snapshot(2, 0);
                remainingWindows(harness, rocks);
            }
            try (var restored = GlobalWindowFlinkOracle.create(rocks, snapshot)) {
                // The checkpointed watermark applies before the first replayed watermark.
                input(restored, 7, 13, -2000);
                assertThat(late(restored)).isEqualTo(1);
                assertThat(restored.numEventTimeTimers()).isEqualTo(2);
                restored.processWatermark(new Watermark(1999));
                assertThat(drain(restored)).isEmpty();
                remainingWindows(restored, rocks);
            } finally {
                snapshot.discardState();
            }
        }
    }

    @Test
    void originalLocalAndGlobalStagesEachDeclareOneOperatorMemoryWeight() throws Exception {
        for (String name : List.of("LocalWindowAggregate", "GlobalWindowAggregate")) {
            var stage = SlicingWindowFlinkPlan.stage(name);
            assertThat(stage.getManagedMemoryOperatorScopeUseCaseWeights())
                    .containsOnlyKeys(org.apache.flink.core.memory.ManagedMemoryUseCase.OPERATOR)
                    .containsEntry(org.apache.flink.core.memory.ManagedMemoryUseCase.OPERATOR, 1);
        }
    }

    private static void remainingWindows(
            KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness, boolean rocks) throws Exception {
        long previouslyDropped = late(harness);
        harness.processWatermark(new Watermark(3999));
        assertThat(drain(harness)).containsExactlyInAnyOrder("1:2:-2000:4000", "2:3:-2000:4000");
        harness.processWatermark(new Watermark(5999));
        assertThat(drain(harness)).containsExactlyInAnyOrder("1:2:0:6000", "2:3:0:6000");
        if (!rocks) assertThat(harness.numKeyedStateEntries()).isZero();
        // Flink schedules one final empty window per key, then stops the timer chain.
        assertThat(harness.numEventTimeTimers()).isEqualTo(2);
        harness.processWatermark(new Watermark(7999));
        assertThat(drain(harness)).isEmpty();
        assertThat(harness.numEventTimeTimers()).isZero();
        input(harness, 3, 11, 2000);
        assertThat(late(harness)).isEqualTo(previouslyDropped + 1); // One dropped partial per input.
        harness.prepareSnapshotPreBarrier(3);
        assertThat(harness.numEventTimeTimers()).isZero();
        assertThat(drain(harness)).isEmpty();
    }

    private static long late(KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness) {
        return ((WindowAggOperator<?, ?>) harness.getOperator())
                .getNumLateRecordsDropped()
                .getCount();
    }

    private static void input(
            KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness, long key, long count, long end)
            throws Exception {
        harness.processElement(new StreamRecord<>(PARTIAL_SERIALIZER.toBinaryRow(GenericRowData.of(key, count, end))));
    }

    private static List<String> drain(KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness) {
        var result = new ArrayList<String>();
        for (var event : harness.getOutput())
            if (event instanceof StreamRecord<?>) {
                var record = (StreamRecord<?>) event;
                assertThat(record.hasTimestamp()).isFalse();
                var row = (RowData) record.getValue();
                assertThat(row.getRowKind()).isEqualTo(RowKind.INSERT);
                result.add(row.getLong(0) + ":" + row.getLong(1) + ":"
                        + row.getTimestamp(2, 3).getMillisecond() + ":"
                        + row.getTimestamp(3, 3).getMillisecond());
            }
        harness.getOutput().clear();
        return result;
    }
}
