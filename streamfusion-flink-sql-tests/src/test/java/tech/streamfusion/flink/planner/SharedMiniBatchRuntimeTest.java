/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
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

/** Exercises automatic controls on the actual common Flink owner, not direct dispatcher calls. */
class SharedMiniBatchRuntimeTest {
    @Test
    void generatedChangelogSurvivesAutomaticControlFlushesAndCheckpointRestore() throws Exception {
        for (boolean rocks : List.of(false, true))
            for (int mode = 0; mode < 3; mode++) {
                byte[] plan = SharedMiniBatchControlTest.plan(10);
                var live = new ArrayList<GenericRowData>();
                try (var oracle = SharedAggregateFlinkOracle.create(rocks, 10);
                        var allocator = new RootAllocator(64L << 20)) {
                    OperatorSubtaskState snapshot;
                    try (var source = new SharedAggregateRuntimeHarness(rocks, null, plan)) {
                        arrival(source, oracle, allocator, live, 0);
                        source.eventOrder.clear();
                        oracle.processWatermark(new Watermark(100));
                        source.processWatermark(0, new Watermark(100));
                        compare(source, oracle);
                        assertThat(source.eventOrder.get(source.eventOrder.size() - 1))
                                .isEqualTo("watermark:100");
                        assertThat(source.eventOrder.get(0)).startsWith("rows:");
                        arrival(source, oracle, allocator, live, 1);
                        // The Flink task invokes this hook before taking the operator snapshot.
                        oracle.getOperator().prepareSnapshotPreBarrier(11);
                        source.region().prepareSnapshotPreBarrier(11);
                        compare(source, oracle);
                        snapshot = SharedAggregateCheckpointTest.snapshot(source, mode, 11);
                        if (rocks && mode != 0) {
                            assertThat(snapshot.getManagedKeyedState()
                                            .iterator()
                                            .next())
                                    .isInstanceOf(
                                            org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle.class);
                            source.notifyOfCompletedCheckpoint(11);
                            source.region().prepareSnapshotPreBarrier(12);
                            var reused = SharedAggregateCheckpointTest.snapshot(source, mode, 12);
                            var first = (org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle)
                                    snapshot.getManagedKeyedState().iterator().next();
                            var next = (org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle)
                                    reused.getManagedKeyedState().iterator().next();
                            assertThat(next.getCheckpointedSize()).isLessThan(first.getCheckpointedSize());
                        }
                    }
                    try (var target = new SharedAggregateRuntimeHarness(mode == 0 ? !rocks : rocks, snapshot, plan)) {
                        compareBundleGauges(target, oracle);
                        arrival(target, oracle, allocator, live, 2);
                        oracle.getOperator().finish();
                        target.region().endInput(1);
                        compare(target, oracle);
                        target.region().endInput(1);
                        target.region().finish();
                        assertThat(target.captured.length()).isZero();
                        try (var input = ArrowRowDataBatch.transpose(
                                List.of(GenericRowData.of(StringData.fromString("late"), 1L)),
                                SharedAggregateFlinkOracle.INPUT,
                                allocator)) {
                            assertThatThrownBy(() -> target.processElement(0, new StreamRecord<>(input)))
                                    .hasMessageContaining("ended");
                        }
                    }
                    assertThat(allocator.getAllocatedMemory()).isZero();
                }
            }
    }

    @Test
    void finishFlushesPendingStateWithoutAnEndInputCallback() throws Exception {
        try (var oracle = SharedAggregateFlinkOracle.create(false, 100);
                var target = new SharedAggregateRuntimeHarness(false, null, SharedMiniBatchControlTest.plan(100));
                var allocator = new RootAllocator(64L << 20)) {
            arrival(target, oracle, allocator, new ArrayList<>(), 5);
            assertThat(target.captured.length()).isZero();
            oracle.getOperator().finish();
            target.region().finish();
            compare(target, oracle);
        }
    }

    private static void arrival(
            SharedAggregateRuntimeHarness target,
            KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> oracle,
            RootAllocator allocator,
            List<GenericRowData> live,
            int seed)
            throws Exception {
        var random = new Random(seed);
        var rows = new ArrayList<RowData>();
        var kinds = new RowKind[23];
        for (int i = 0; i < kinds.length; i++) {
            GenericRowData row;
            if (!live.isEmpty() && random.nextBoolean()) {
                row = live.remove(random.nextInt(live.size()));
                row.setRowKind(i % 2 == 0 ? RowKind.DELETE : RowKind.UPDATE_BEFORE);
            } else {
                row = GenericRowData.of(
                        i % 7 == 0 ? null : StringData.fromString("é-" + random.nextInt(5)),
                        i % 4 == 0 ? null : (long) random.nextInt(17) - 8);
                row.setRowKind(i % 2 == 0 ? RowKind.INSERT : RowKind.UPDATE_AFTER);
                live.add(GenericRowData.of(row.getField(0), row.getField(1)));
            }
            rows.add(row);
            kinds[i] = row.getRowKind();
            oracle.processElement(new StreamRecord<>(row, 500L + i));
        }
        try (var batch = ArrowRowDataBatch.transpose(rows, SharedAggregateFlinkOracle.INPUT, allocator)
                .withRowKinds(kinds)) {
            target.processElement(0, new StreamRecord<>(batch));
        }
        compare(target, oracle);
    }

    private static void compare(
            SharedAggregateRuntimeHarness target,
            KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> oracle)
            throws Exception {
        var expected = new DataOutputSerializer(128);
        var serializer = new RowDataSerializer(SharedAggregateFlinkOracle.OUTPUT);
        var records = oracle.extractOutputStreamRecords();
        for (var record : records) {
            assertThat(record.hasTimestamp()).isFalse();
            serializer.serialize(record.getValue(), expected);
        }
        assertThat(target.captured.getCopyOfBuffer()).isEqualTo(expected.getCopyOfBuffer());
        assertThat(target.times).hasSize(records.size()).allMatch(java.util.Objects::isNull);
        compareBundleGauges(target, oracle);
        target.captured.clear();
        target.times.clear();
        oracle.getOutput().clear();
    }

    private static void compareBundleGauges(
            SharedAggregateRuntimeHarness target,
            KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> oracle)
            throws Exception {
        var expected =
                SharedAggregateMetricSurfaceTest.metrics(oracle.getOperator().getMetricGroup());
        var actual = SharedAggregateMetricSurfaceTest.metrics(SharedAggregateMetricSurfaceTest.stageGroup(target, 3));
        for (String name : List.of("bundleSize", "bundleRatio")) {
            assertThat(((org.apache.flink.metrics.Gauge<?>) actual.get(name)).getValue())
                    .as(name)
                    .isEqualTo(((org.apache.flink.metrics.Gauge<?>) expected.get(name)).getValue());
        }
    }
}
