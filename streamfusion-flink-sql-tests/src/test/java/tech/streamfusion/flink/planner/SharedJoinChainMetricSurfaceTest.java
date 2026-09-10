/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;
import static tech.streamfusion.flink.planner.SharedJoinChainFixture.*;

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
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.flink.arrow.ArrowExchangeCDataBridge;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.exchange.ArrowExchangeBatch;
import tech.streamfusion.flink.exchange.NativeExchangePlanSerializer;
import tech.streamfusion.flink.operator.StreamFusionNativeRegionOperatorFactory;

class SharedJoinChainMetricSurfaceTest {
    @ParameterizedTest
    @CsvSource({
        "false,false,false", "false,false,true", "false,true,false", "false,true,true",
        "true,false,false", "true,false,true", "true,true,false", "true,true,true"
    })
    void bareAndComposedJoinsOwnEnvelopesAndMatchFlinkStageMetrics(boolean rocks, boolean chain, boolean frames)
            throws Exception {
        var fixture = new SharedJoinChainFixture(chain);
        byte[] exchange = NativeExchangePlanSerializer.hash(INPUT, new int[] {0}, 16, 1, true);
        for (int seed = 0; seed < 3; seed++) {
            var factory = frames
                    ? new StreamFusionNativeRegionOperatorFactory(
                            fixture.inputs,
                            fixture.output,
                            fixture.plan(),
                            fixture.stateIds(),
                            java.util.Collections.nCopies(fixture.inputs.size(), exchange))
                    : new StreamFusionNativeRegionOperatorFactory(
                            fixture.inputs, fixture.output, fixture.plan(), fixture.stateIds());
            try (var oracle = fixture.oracle(rocks);
                    var target = new KeyedNativeMetricHarness(
                            rocks, factory, fixture.inputs.size(), fixture.output, null, 1, 0);
                    var allocator = new RootAllocator(64L << 20)) {
                var expected = new DataOutputSerializer(128);
                compare(fixture, oracle, target);
                for (int arrival = 0; arrival < fixture.inputs.size() * 4; arrival++) {
                    int port = arrival % fixture.inputs.size();
                    RowKind kind = new RowKind[] {
                                RowKind.INSERT, RowKind.DELETE, RowKind.UPDATE_AFTER, RowKind.UPDATE_BEFORE
                            }
                            [arrival / fixture.inputs.size()];
                    int count = arrival == 0 ? 5000 : 1;
                    var rows = new ArrayList<GenericRowData>();
                    var present = new boolean[count];
                    var timestamps = new long[count];
                    for (int row = 0; row < count; row++) {
                        var value = row(7L + seed, port);
                        value.setRowKind(kind);
                        rows.add(value);
                        present[row] = row % 3 != 0;
                        timestamps[row] = 123 + row;
                        var binary =
                                new RowDataSerializer(INPUT).toBinaryRow(value).copy();
                        oracle.accept(
                                port,
                                present[row]
                                        ? new StreamRecord<>(binary, timestamps[row])
                                        : new StreamRecord<>(binary));
                    }
                    for (var event : oracle.drain()) StageEventBytes.encode(fixture.output, event, expected);
                    try (var batch = ArrowRowDataBatch.transpose(rows, INPUT, allocator)
                            .withEnvelope(
                                    rows.stream()
                                            .map(GenericRowData::getRowKind)
                                            .toArray(RowKind[]::new),
                                    present,
                                    timestamps)) {
                        if (frames) {
                            try (var envelope = ArrowExchangeBatch.withEnvelope(batch, INPUT, null)) {
                                for (var frame : ArrowExchangeCDataBridge.route(
                                        exchange, envelope.batch(), allocator, target.memory))
                                    target.processElement(port, new StreamRecord<>(frame));
                            }
                        } else target.processElement(port, new StreamRecord<>(batch));
                    }
                    assertThat(target.maxOutputBatchRows).isLessThanOrEqualTo(4096);
                    assertThat(target.output.getCopyOfBuffer()).containsExactly(expected.getCopyOfBuffer());
                    compare(fixture, oracle, target);
                    for (var control : List.of(
                            new Watermark(100 + arrival),
                            WatermarkStatus.IDLE,
                            WatermarkStatus.ACTIVE,
                            new LatencyMarker(0, new OperatorID(7, 9), port))) {
                        oracle.accept(port, control);
                        for (var event : oracle.drain()) StageEventBytes.encode(fixture.output, event, expected);
                        if (control instanceof Watermark) target.processWatermark(port, (Watermark) control);
                        else if (control instanceof WatermarkStatus)
                            target.processWatermarkStatus(port, (WatermarkStatus) control);
                        else target.region().getInputs().get(port).processLatencyMarker((LatencyMarker) control);
                        target.drainControls();
                        assertThat(target.output.getCopyOfBuffer()).containsExactly(expected.getCopyOfBuffer());
                        compare(fixture, oracle, target);
                    }
                    oracle.preBarrier(arrival);
                    target.region().prepareSnapshotPreBarrier(arrival);
                    compare(fixture, oracle, target);
                }
                assertThat(target.output.length()).isPositive();
            }
        }
    }

    private static void compare(
            SharedJoinChainFixture fixture, SharedJoinChainFixture.Oracle oracle, KeyedNativeMetricHarness target)
            throws Exception {
        for (int stage = 0; stage < fixture.stages; stage++) {
            var expected = oracle.joins.get(stage).group();
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
