/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;
import static tech.streamfusion.flink.planner.SharedStringScalarMetricFixture.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.types.RowKind;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;

final class SharedStringScalarMetricAssertions {
    static void assertParity(SharedStringScalarMetricFixture fixture, java.util.function.IntFunction<String> values)
            throws Exception {
        for (int seed = 0; seed < 3; seed++) {
            try (var first = fixture.oracle(0);
                    var expand = fixture.oracle(1);
                    var last = fixture.oracle(2);
                    var target = new NativeHarness(fixture.plan());
                    var allocator = new RootAllocator(64L << 20)) {
                var oracles = List.of(first, expand, last);
                var expected = new DataOutputSerializer(128);
                compare(oracles, target, false);
                for (int arrival = 0; arrival < 4; arrival++) {
                    int previousBatches = target.batches;
                    int count = new int[] {0, 1, 7, 3001}[arrival];
                    var rows = new ArrayList<GenericRowData>();
                    var timestamps = new boolean[count];
                    var times = new long[count];
                    for (int row = 0; row < count; row++) {
                        var value = GenericRowData.of(
                                row % 7 == 0
                                        ? null
                                        : org.apache.flink.table.data.StringData.fromString(values.apply(row + seed)));
                        value.setRowKind(RowKind.values()[(row + seed) % 4]);
                        rows.add(value);
                        timestamps[row] = row % 3 != 0;
                        times[row] = row - 9L;
                        StreamRecord<GenericRowData> record =
                                timestamps[row] ? new StreamRecord<>(value, times[row]) : new StreamRecord<>(value);
                        feed(oracles, record, expected);
                    }
                    try (var batch = ArrowRowDataBatch.transpose(rows, TYPE, allocator)
                            .withEnvelope(
                                    rows.stream()
                                            .map(GenericRowData::getRowKind)
                                            .toArray(RowKind[]::new),
                                    timestamps,
                                    times)) {
                        target.accept(batch);
                    }
                    if (arrival == 3) {
                        assertThat(target.batches - previousBatches).isPositive();
                    }
                    compare(oracles, target, arrival > 0);
                    var mark = new Watermark(100 + arrival);
                    feed(oracles, mark, expected);
                    target.processWatermark(0, mark);
                    for (var status : List.of(WatermarkStatus.IDLE, WatermarkStatus.ACTIVE)) {
                        feed(oracles, status, expected);
                        target.processWatermarkStatus(0, status);
                    }
                    var latency = new LatencyMarker(0, new OperatorID(7, 9), arrival % 2);
                    feed(oracles, latency, expected);
                    target.latency(latency);
                    target.drainControls();
                    assertThat(target.output.getCopyOfBuffer())
                            .as("seed %s arrival %s", seed, arrival)
                            .containsExactly(expected.getCopyOfBuffer());
                    compare(oracles, target, true);
                    // Pre-barrier callbacks must leave stateless stage metrics and output intact.
                    for (var oracle : oracles) oracle.harness.getOperator().prepareSnapshotPreBarrier(arrival);
                    target.region().prepareSnapshotPreBarrier(arrival);
                    compare(oracles, target, true);
                }
                for (var oracle : oracles) oracle.harness.getOperator().finish();
                target.region().endInput(1);
                target.region().finish();
                target.drainControls();
                compare(oracles, target, true);
                assertThat(target.output.getCopyOfBuffer()).containsExactly(expected.getCopyOfBuffer());
            }
        }
    }

    private static void feed(List<Oracle> stages, StreamElement event, DataOutputSerializer output) throws Exception {
        List<StreamElement> events = List.of(event);
        for (var stage : stages) {
            for (var input : events) stage.accept(input);
            events = stage.drain();
        }
        for (var result : events) encode(result, output);
    }

    private static void compare(List<Oracle> stages, NativeHarness target, boolean hasLatency) throws Exception {
        for (int stage = 0; stage < stages.size(); stage++) {
            var reference = stages.get(stage).group();
            var actual = target.stage(stage);
            assertThat(actual.isClosed()).isFalse();
            assertThat(actual.getAllVariables().get("<operator_id>"))
                    .isEqualTo(reference.getAllVariables().get("<operator_id>"));
            assertThat(actual.getAllVariables().get("<operator_name>"))
                    .isEqualTo(reference.getAllVariables().get("<operator_name>"));
            RegisteredMetricSurface.compare(
                    RegisteredMetricSurface.metrics(reference), RegisteredMetricSurface.metrics(actual));
            var expectedLatency = RegisteredMetricSurface.latency(reference.getTaskMetricGroup(), operatorId(stage));
            var actualLatency = RegisteredMetricSurface.latency(actual.getTaskMetricGroup(), operatorId(stage));
            if (hasLatency) assertThat(expectedLatency).isNotEmpty();
            RegisteredMetricSurface.compareLatency(expectedLatency, actualLatency);
        }
    }
}
