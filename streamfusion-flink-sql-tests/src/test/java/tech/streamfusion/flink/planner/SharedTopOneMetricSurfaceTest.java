/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;
import static tech.streamfusion.flink.planner.SharedTopOneFixture.*;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;

class SharedTopOneMetricSurfaceTest {
    static Stream<Arguments> modes() {
        var modes = new ArrayList<Arguments>();
        for (boolean ascending : List.of(false, true))
            for (boolean rank : List.of(false, true))
                for (boolean before : List.of(false, true))
                    for (boolean rocks : List.of(false, true)) modes.add(Arguments.of(ascending, rank, before, rocks));
        return modes.stream();
    }

    @ParameterizedTest(name = "ascending={0}, rank={1}, before={2}, rocks={3}")
    @MethodSource("modes")
    void sharedTreeMatchesFullDefaultStageMetricsAndOrderedFlinkChangelog(
            boolean ascending, boolean rank, boolean before, boolean rocks) throws Exception {
        var fixture = new SharedTopOneFixture(ascending, rank, before);
        try (var first = fixture.oracle(0, rocks);
                var middle = fixture.oracle(1, rocks);
                var tail = fixture.oracle(2, rocks);
                var target = new KeyedNativeMetricHarness(
                        rocks, fixture.plan(), List.of(fixture.top.input), fixture.top.output, List.of(id(1)));
                var allocator = new RootAllocator(64L << 20)) {
            var oracles = List.of(first, middle, tail);
            var expected = new DataOutputSerializer(128);
            compare(oracles, target, false);
            for (int arrival = 0; arrival < 4; arrival++) {
                int count = new int[] {0, 1, 7, 257}[arrival];
                var rows = new ArrayList<GenericRowData>();
                var present = new boolean[count];
                var timestamps = new long[count];
                for (int row = 0; row < count; row++) {
                    var value = GenericRowData.of(
                            row % 7 == 0 ? null : (long) (row % 11),
                            row % 13 == 0 ? null : (long) (row % 17 - 2),
                            row % 5 == 0 ? null : TimestampData.fromEpochMillis(arrival * 10L + row % 4),
                            StringData.fromString("é-" + arrival + "-" + row));
                    rows.add(value);
                    present[row] = row % 3 != 0;
                    timestamps[row] = new long[] {Long.MIN_VALUE, -19, 0, Long.MAX_VALUE}[row % 4];
                    // Flink state/output may mutate RowKinds. Isolate the source for both engines.
                    var copy = new org.apache.flink.table.runtime.typeutils.RowDataSerializer(fixture.top.input)
                            .toBinaryRow(value)
                            .copy();
                    feed(
                            oracles,
                            present[row] ? new StreamRecord<>(copy, timestamps[row]) : new StreamRecord<>(copy),
                            fixture.top.output,
                            expected);
                }
                try (var batch = ArrowRowDataBatch.transpose(rows, fixture.top.input, allocator)
                        .withEnvelope(
                                rows.stream().map(GenericRowData::getRowKind).toArray(RowKind[]::new),
                                present,
                                timestamps)) {
                    target.processElement(0, new StreamRecord<>(batch));
                }
                compare(oracles, target, arrival > 0);
                var mark = new Watermark(100 + arrival);
                feed(oracles, mark, fixture.top.output, expected);
                target.processWatermark(0, mark);
                for (var status : List.of(WatermarkStatus.IDLE, WatermarkStatus.ACTIVE)) {
                    feed(oracles, status, fixture.top.output, expected);
                    target.processWatermarkStatus(0, status);
                }
                var latency = new LatencyMarker(0, new OperatorID(7, 9), arrival % 2);
                feed(oracles, latency, fixture.top.output, expected);
                target.region().getInputs().get(0).processLatencyMarker(latency);
                target.drainControls();
                assertThat(target.output.getCopyOfBuffer())
                        .as("arrival %s", arrival)
                        .containsExactly(expected.getCopyOfBuffer());
                compare(oracles, target, true);
                for (var oracle : oracles) oracle.harness.getOperator().prepareSnapshotPreBarrier(arrival);
                target.region().prepareSnapshotPreBarrier(arrival);
                compare(oracles, target, true);
            }
            for (var oracle : oracles) oracle.harness.getOperator().finish();
            target.region().endInput(1);
            target.region().finish();
            target.drainControls();
            assertThat(target.output.getCopyOfBuffer()).containsExactly(expected.getCopyOfBuffer());
            compare(oracles, target, true);
        }
    }

    private static void feed(
            List<FlinkStageMetricOracle> stages,
            StreamElement event,
            org.apache.flink.table.types.logical.RowType type,
            DataOutputSerializer output)
            throws Exception {
        var events = List.of(event);
        for (var stage : stages) {
            for (var input : events) stage.accept(input);
            events = stage.drain();
        }
        for (var result : events) StageEventBytes.encode(type, result, output);
    }

    private static void compare(List<FlinkStageMetricOracle> oracles, KeyedNativeMetricHarness target, boolean latency)
            throws Exception {
        for (int stage = 0; stage < oracles.size(); stage++) {
            var expected = oracles.get(stage).group();
            var actual = target.stage(id(stage));
            assertThat(actual.isClosed()).isFalse();
            for (var variable : List.of("<operator_id>", "<operator_name>"))
                assertThat(actual.getAllVariables().get(variable))
                        .isEqualTo(expected.getAllVariables().get(variable));
            RegisteredMetricSurface.compare(
                    RegisteredMetricSurface.metrics(expected), RegisteredMetricSurface.metrics(actual));
            var expectedLatency = RegisteredMetricSurface.latency(expected.getTaskMetricGroup(), operatorId(stage));
            if (latency) assertThat(expectedLatency).isNotEmpty();
            RegisteredMetricSurface.compareLatency(
                    expectedLatency, RegisteredMetricSurface.latency(actual.getTaskMetricGroup(), operatorId(stage)));
        }
    }
}
