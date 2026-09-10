/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;
import static tech.streamfusion.flink.planner.SharedWindowJoinFixture.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;

class SharedWindowJoinMetricSurfaceTest {
    @ParameterizedTest
    @CsvSource({
        "false,false,false",
        "false,false,true",
        "false,true,false",
        "false,true,true",
        "true,false,false",
        "true,false,true",
        "true,true,false",
        "true,true,true"
    })
    void completeMetricSurfaceAndOrderedControlsMatchFlink(boolean rocks, boolean keyed, boolean residual)
            throws Exception {
        var fixture = new SharedWindowJoinFixture(keyed, residual);
        try (var join = fixture.join(rocks);
                var calc = fixture.calc();
                var target = new KeyedNativeMetricHarness(
                        rocks, fixture.plan(), List.of(INPUT, INPUT), OUTPUT, List.of(id(0)));
                var allocator = new RootAllocator(64L << 20)) {
            var expected = new DataOutputSerializer(128);
            compare(join, calc, target);
            for (int seed = 0; seed < 4; seed++) {
                long end = (seed + 1) * 100L;
                for (int port = 0; port < 2; port++) {
                    var random = new java.util.Random(seed * 7 + port);
                    var rows = new ArrayList<GenericRowData>();
                    for (int i = 0; i < 11; i++) {
                        var row = GenericRowData.ofKind(
                                i % 3 == 0 ? RowKind.UPDATE_AFTER : RowKind.INSERT,
                                i % 5 == 0 ? null : 7L,
                                end,
                                i % 7 == 0 ? null : (long) random.nextInt(4),
                                i % 4 == 0 ? null : StringData.fromString("é-" + random.nextInt(3)));
                        rows.add(row);
                        join.accept(
                                port,
                                new StreamRecord<>(
                                        new RowDataSerializer(INPUT)
                                                .toBinaryRow(row)
                                                .copy(),
                                        777));
                    }
                    try (var batch = ArrowRowDataBatch.transpose(rows, INPUT, allocator)
                            .withRowKinds(rows.stream()
                                    .map(GenericRowData::getRowKind)
                                    .toArray(RowKind[]::new))) {
                        target.processElement(port, new StreamRecord<>(batch));
                    }
                    drain(join, calc, target, expected);
                    var controls = List.of(
                            new Watermark(end - 1),
                            WatermarkStatus.IDLE,
                            WatermarkStatus.ACTIVE,
                            new LatencyMarker(System.currentTimeMillis(), new OperatorID(7, 9), port));
                    for (var control : controls) {
                        join.accept(port, control);
                        if (control instanceof Watermark) target.processWatermark(port, (Watermark) control);
                        else if (control instanceof WatermarkStatus)
                            target.processWatermarkStatus(port, (WatermarkStatus) control);
                        else target.region().getInputs().get(port).processLatencyMarker((LatencyMarker) control);
                        drain(join, calc, target, expected);
                    }
                    if (port == 1) {
                        for (int latePort = 0; latePort < 2; latePort++) {
                            var late = GenericRowData.ofKind(RowKind.DELETE, 7L, end - 100, 1L, null);
                            join.accept(
                                    latePort,
                                    new StreamRecord<>(new RowDataSerializer(INPUT)
                                            .toBinaryRow(late)
                                            .copy()));
                            try (var batch = ArrowRowDataBatch.transpose(List.of(late), INPUT, allocator)
                                    .withRowKinds(new RowKind[] {RowKind.DELETE})) {
                                target.processElement(latePort, new StreamRecord<>(batch));
                            }
                            drain(join, calc, target, expected);
                        }
                    }
                }
                join.prepareSnapshotPreBarrier(seed);
                calc.harness.getOperator().prepareSnapshotPreBarrier(seed);
                target.region().prepareSnapshotPreBarrier(seed);
                compare(join, calc, target);
            }
        }
    }

    private static void drain(
            FlinkRegularJoinMetricOracle join,
            FlinkStageMetricOracle calc,
            KeyedNativeMetricHarness target,
            DataOutputSerializer expected)
            throws Exception {
        for (var event : join.drain()) calc.accept(event);
        for (var event : calc.drain()) StageEventBytes.encode(OUTPUT, event, expected);
        target.drainControls();
        assertThat(target.output.getCopyOfBuffer()).isEqualTo(expected.getCopyOfBuffer());
        compare(join, calc, target);
    }

    private static void compare(
            FlinkRegularJoinMetricOracle join, FlinkStageMetricOracle calc, KeyedNativeMetricHarness target)
            throws Exception {
        join.harness.setProcessingTime(12345);
        calc.harness.setProcessingTime(12345);
        target.setProcessingTime(12345);
        var groups = List.of(join.group(), calc.group());
        for (int stage = 0; stage < groups.size(); stage++) {
            var expected = groups.get(stage);
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
