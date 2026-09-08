/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;
import static tech.streamfusion.flink.planner.SharedBinaryJoinMetricFixture.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.metrics.groups.InternalOperatorMetricGroup;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;

class SharedBinaryJoinMetricSurfaceTest {
    @ParameterizedTest
    @CsvSource({
        "false,EQUALITY",
        "true,EQUALITY",
        "false,RANGE",
        "true,RANGE",
        "false,TIMESTAMP_OFFSET",
        "true,TIMESTAMP_OFFSET",
        "false,WIDE_RANGE",
        "true,WIDE_RANGE"
    })
    void defaultMetricsAndChangelogMatchFlinkAcrossBothInputsAndBackends(boolean rocks, Predicate predicate)
            throws Exception {
        var fixture = SharedBinaryJoinMetricFixture.forPredicate(predicate);
        try (var join = fixture.join(rocks);
                var calc = fixture.calc();
                var target = new KeyedNativeMetricHarness(
                        rocks, fixture.plan(), List.of(fixture.input, fixture.input), fixture.output, List.of(id(0)));
                var allocator = new RootAllocator(64L << 20)) {
            var expected = new DataOutputSerializer(128);
            compare(join, calc, target);
            for (int arrival = 0; arrival < 8; arrival++) {
                int port = arrival % 2;
                // Narrow rows use one value with high multiplicity. Wide rows use distinct
                // keys so a single batch crosses the byte quantum with deterministic order.
                // Neither case assumes an iteration order between distinct MapState entries.
                int count = predicate == Predicate.WIDE_RANGE ? 96 : (arrival == 0 ? 5000 : 1);
                var rows = new ArrayList<GenericRowData>();
                for (int row = 0; row < count; row++) {
                    var value = fixture.row(predicate == Predicate.WIDE_RANGE ? 7L + 66L * row : 7L, port);
                    value.setRowKind(
                            arrival < 2
                                    ? RowKind.INSERT
                                    : arrival < 4
                                            ? RowKind.DELETE
                                            : arrival < 6 ? RowKind.UPDATE_AFTER : RowKind.UPDATE_BEFORE);
                    rows.add(value);
                    join.accept(
                            port,
                            new StreamRecord<>(
                                    new RowDataSerializer(fixture.input)
                                            .toBinaryRow(value)
                                            .copy(),
                                    123 + row));
                }
                drain(fixture, join, calc, expected);
                var present = new boolean[count];
                java.util.Arrays.fill(present, true);
                var timestamps = java.util.stream.IntStream.range(0, count)
                        .mapToLong(row -> 123L + row)
                        .toArray();
                try (var batch = ArrowRowDataBatch.transpose(rows, fixture.input, allocator)
                        .withEnvelope(
                                rows.stream().map(GenericRowData::getRowKind).toArray(RowKind[]::new),
                                present,
                                timestamps)) {
                    target.processElement(port, new StreamRecord<>(batch));
                }
                assertThat(target.maxOutputBatchRows).isLessThanOrEqualTo(4096);
                // Joins clear StreamRecord timestamps; arrival metadata is not SQL rowtime.
                assertThat(target.output.getCopyOfBuffer()).containsExactly(expected.getCopyOfBuffer());
                var controls = List.of(
                        new Watermark(100 + arrival),
                        WatermarkStatus.IDLE,
                        WatermarkStatus.ACTIVE,
                        new LatencyMarker(0, new OperatorID(7, 9), port));
                for (var control : controls) {
                    join.accept(port, control);
                    drain(fixture, join, calc, expected);
                    if (control instanceof Watermark) target.processWatermark(port, (Watermark) control);
                    else if (control instanceof WatermarkStatus)
                        target.processWatermarkStatus(port, (WatermarkStatus) control);
                    else target.region().getInputs().get(port).processLatencyMarker((LatencyMarker) control);
                    target.drainControls();
                    assertThat(target.output.getCopyOfBuffer()).containsExactly(expected.getCopyOfBuffer());
                    compare(join, calc, target);
                }
                join.harness.region().prepareSnapshotPreBarrier(arrival);
                calc.harness.getOperator().prepareSnapshotPreBarrier(arrival);
                target.region().prepareSnapshotPreBarrier(arrival);
                compare(join, calc, target);
            }
        }
    }

    private static void drain(
            SharedBinaryJoinMetricFixture fixture,
            FlinkMultiInputMetricOracle join,
            FlinkStageMetricOracle calc,
            DataOutputSerializer bytes)
            throws Exception {
        for (var event : join.drain()) calc.accept(event);
        for (var event : calc.drain()) StageEventBytes.encode(fixture.output, event, bytes);
    }

    private static void compare(
            FlinkMultiInputMetricOracle join, FlinkStageMetricOracle calc, KeyedNativeMetricHarness target)
            throws Exception {
        List<InternalOperatorMetricGroup> oracles = List.of(join.group(), calc.group());
        for (int stage = 0; stage < oracles.size(); stage++) {
            var expected = oracles.get(stage);
            var actual = target.stage(id(stage));
            RegisteredMetricSurface.compare(
                    RegisteredMetricSurface.metrics(expected), RegisteredMetricSurface.metrics(actual));
            for (var variable : List.of("<operator_id>", "<operator_name>"))
                assertThat(actual.getAllVariables().get(variable))
                        .isEqualTo(expected.getAllVariables().get(variable));
            RegisteredMetricSurface.compareLatency(
                    RegisteredMetricSurface.latency(expected.getTaskMetricGroup(), operatorId(stage)),
                    RegisteredMetricSurface.latency(actual.getTaskMetricGroup(), operatorId(stage)));
        }
    }
}
