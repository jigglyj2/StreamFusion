/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;
import static tech.streamfusion.flink.planner.GlobalPartialFixtures.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.metrics.WatermarkGauge;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.arrow.ArrowLocalGroupAggregateCDataBridge;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.nativebridge.NativeLocalGroupAggregateBridge;

/** Global fragment on the ordinary Flink region owner, with retained local input fixtures only. */
class SharedGlobalAggregateRuntimeTest {
    @Test
    void automaticControlsAndCanonicalAlignedUnalignedStateRestoreMatchFlink() throws Exception {
        for (boolean rocks : List.of(false, true))
            for (int mode = 0; mode < 3; mode++) {
                var memory = new SharedAggregateRegionParityTest.Memory();
                long local = NativeLocalGroupAggregateBridge.create(localPlan(), memory);
                try (var oracle = SharedAggregateFlinkOracle.create(rocks, 7);
                        var allocator = new RootAllocator(64L << 20)) {
                    OperatorSubtaskState snapshot;
                    try (var source = new SharedAggregateRuntimeHarness(rocks, null, globalPlan(7), PARTIAL)) {
                        arrival(source, oracle, allocator, local, memory, true, 23);
                        compare(source, oracle);
                        source.eventOrder.clear();
                        oracle.processWatermark(new Watermark(100));
                        source.processWatermark(0, new Watermark(100));
                        compare(source, oracle);
                        assertThat(source.eventOrder.get(0)).startsWith("rows:");
                        assertThat(source.eventOrder.get(source.eventOrder.size() - 1))
                                .isEqualTo("watermark:100");
                        arrival(source, oracle, allocator, local, memory, true, 23);
                        oracle.getOperator().prepareSnapshotPreBarrier(11);
                        source.region().prepareSnapshotPreBarrier(11);
                        compare(source, oracle);
                        snapshot = SharedAggregateCheckpointTest.snapshot(source, mode, 11);
                        if (rocks && mode != 0)
                            assertThat(snapshot.getManagedKeyedState()
                                            .iterator()
                                            .next())
                                    .isInstanceOf(
                                            org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle.class);
                    }
                    try (var target = new SharedAggregateRuntimeHarness(
                            mode == 0 ? !rocks : rocks, snapshot, globalPlan(7), PARTIAL)) {
                        arrival(target, oracle, allocator, local, memory, false, 23);
                        compare(target, oracle);
                        arrival(target, oracle, allocator, local, memory, false, 23);
                        oracle.getOperator().finish();
                        target.region().endInput(1);
                        target.region().finish();
                        compare(target, oracle);
                    }
                    assertThat(allocator.getAllocatedMemory()).isZero();
                } finally {
                    NativeLocalGroupAggregateBridge.destroy(local);
                }
                assertThat(memory.available()).isEqualTo(memory.limit());
            }
    }

    @Test
    void globalBundleDefaultMetricSurfaceMatchesEquivalentFlinkBundleBoundaries() throws Exception {
        for (boolean rocks : List.of(false, true))
            for (int trigger : List.of(1, 7, 100)) {
                var memory = new SharedAggregateRegionParityTest.Memory();
                long local = NativeLocalGroupAggregateBridge.create(localPlan(), memory);
                try (var oracle = SharedAggregateFlinkOracle.create(rocks, trigger);
                        var target = new SharedAggregateRuntimeHarness(rocks, null, globalPlan(trigger), PARTIAL);
                        var allocator = new RootAllocator(64L << 20)) {
                    // Task harnesses omit task-installed watermark gauges and record counting.
                    var inputWatermark = new WatermarkGauge();
                    var outputWatermark = new WatermarkGauge();
                    oracle.getOperator().getMetricGroup().gauge("currentInputWatermark", inputWatermark);
                    oracle.getOperator().getMetricGroup().gauge("currentOutputWatermark", outputWatermark);
                    SharedMiniBatchMetricSurfaceTest.check(target, oracle, outputWatermark);
                    for (int phase = 0; phase < 4; phase++) {
                        arrival(target, oracle, allocator, local, memory, phase < 2, 23);
                        SharedMiniBatchMetricSurfaceTest.check(target, oracle, outputWatermark);
                        arrival(target, oracle, allocator, local, memory, true, 0);
                        SharedMiniBatchMetricSurfaceTest.check(target, oracle, outputWatermark);
                        if (phase == 0) {
                            inputWatermark.setCurrentWatermark(100);
                            oracle.processWatermark(new Watermark(100));
                            target.processWatermark(0, new Watermark(100));
                        } else if (phase == 2) {
                            oracle.getOperator().prepareSnapshotPreBarrier(11);
                            target.region().prepareSnapshotPreBarrier(11);
                        } else if (phase == 3) {
                            oracle.getOperator().finish();
                            target.region().finish();
                        }
                        SharedMiniBatchMetricSurfaceTest.check(target, oracle, outputWatermark);
                    }
                } finally {
                    NativeLocalGroupAggregateBridge.destroy(local);
                }
                assertThat(memory.available()).isEqualTo(memory.limit());
            }
    }

    private static void arrival(
            SharedAggregateRuntimeHarness target,
            KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> oracle,
            RootAllocator allocator,
            long local,
            SharedAggregateRegionParityTest.Memory memory,
            boolean insert,
            int count)
            throws Exception {
        var rows = new ArrayList<RowData>();
        for (int index = 0; index < count; index++) {
            var row = GenericRowData.of(
                    index % 5 == 0 ? null : StringData.fromString("é-" + index % 3),
                    index % 4 == 0 ? null : (long) index);
            row.setRowKind(
                    insert
                            ? index % 2 == 0 ? RowKind.INSERT : RowKind.UPDATE_AFTER
                            : index % 2 == 0 ? RowKind.DELETE : RowKind.UPDATE_BEFORE);
            rows.add(row);
            oracle.getOperator()
                    .getMetricGroup()
                    .getIOMetricGroup()
                    .getNumRecordsInCounter()
                    .inc();
            oracle.processElement(new StreamRecord<>(row, index));
        }
        try (var raw = ArrowRowDataBatch.transpose(rows, SharedAggregateFlinkOracle.INPUT, allocator)
                        .withRowKinds(rows.stream().map(RowData::getRowKind).toArray(RowKind[]::new));
                var partials = ArrowLocalGroupAggregateCDataBridge.execute(
                        local, raw, null, true, PARTIAL, allocator, memory)) {
            assertThat(partials.size()).isEqualTo(rows.size());
            target.processElement(0, new StreamRecord<>(partials));
        }
    }

    private static void compare(
            SharedAggregateRuntimeHarness target,
            KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> oracle)
            throws Exception {
        var expected = new DataOutputSerializer(128);
        var records = oracle.extractOutputStreamRecords();
        var serializer = new RowDataSerializer(SharedAggregateFlinkOracle.OUTPUT);
        for (var record : records) {
            assertThat(record.hasTimestamp()).isFalse();
            serializer.serialize(record.getValue(), expected);
        }
        assertThat(target.captured.getCopyOfBuffer()).isEqualTo(expected.getCopyOfBuffer());
        assertThat(target.times).hasSize(records.size()).allMatch(java.util.Objects::isNull);
        target.captured.clear();
        target.times.clear();
        oracle.getOutput().clear();
    }
}
