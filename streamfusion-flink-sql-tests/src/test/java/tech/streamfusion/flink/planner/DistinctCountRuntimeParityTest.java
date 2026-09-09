/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;

/** Direct common-runtime prerequisite; production DISTINCT admission deliberately stays closed. */
class DistinctCountRuntimeParityTest {
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void generatedFilteredDistinctCountsMatchFlinkChangelogControlsAndMetrics(boolean rocks) throws Exception {
        try (var oracle = DistinctCountFlinkOracle.create(rocks);
                var nativePlan = new KeyedNativeMetricHarness(
                        rocks,
                        DistinctCountFixture.plan(),
                        List.of(DistinctCountFlinkOracle.INPUT),
                        DistinctCountFlinkOracle.OUTPUT,
                        List.of(3L));
                var allocator = new RootAllocator(64L << 20)) {
            var expected = new DataOutputSerializer(128);
            var group = oracle.getOperator().getMetricGroup();
            var nativeGroup = SharedAggregateMetricSurfaceTest.stageGroup(nativePlan.region(), 3);
            var inputWatermark = new org.apache.flink.streaming.runtime.metrics.WatermarkGauge();
            var outputWatermark = new org.apache.flink.streaming.runtime.metrics.WatermarkGauge();
            group.gauge("currentInputWatermark", inputWatermark);
            group.gauge("currentOutputWatermark", outputWatermark);
            long timestamp = 0;
            for (int seed : new int[] {3, 29, 197}) {
                var random = new Random(seed);
                var values = new ArrayList<GenericRowData>();
                for (int index = 0; index < 257; index++) {
                    values.add(GenericRowData.of(
                            index % 7 == 0 ? null : StringData.fromString("é\u0000-" + random.nextInt(5)),
                            index % 11 == 0 ? null : (long) random.nextInt(17) - 8,
                            index % 13 == 0 ? null : index % 3 != 0));
                }
                // Revisit every value to exercise first/duplicate/last transitions and empty groups.
                for (int pass = 0; pass < 6; pass++) {
                    // The null-key group exists. Retract a value it has never seen, then cancel
                    // that negative multiplicity before retracting its ordinary records.
                    var inputs =
                            pass == 2 || pass == 3 ? List.of(GenericRowData.of(null, Long.MIN_VALUE, true)) : values;
                    for (int start = 0; start < inputs.size(); start += 31) {
                        var rows = new ArrayList<GenericRowData>();
                        int count = Math.min(31, inputs.size() - start);
                        var kinds = new RowKind[count];
                        var present = new boolean[count];
                        var times = new long[count];
                        for (int index = 0; index < count; index++) {
                            var value = inputs.get(start + index);
                            value.setRowKind(
                                    pass < 2 || pass == 3
                                            ? index % 2 == 0 ? RowKind.INSERT : RowKind.UPDATE_AFTER
                                            : index % 2 == 0 ? RowKind.DELETE : RowKind.UPDATE_BEFORE);
                            rows.add(value);
                            kinds[index] = value.getRowKind();
                            present[index] = index % 3 != 0;
                            times[index] = timestamp++;
                            group.getIOMetricGroup().getNumRecordsInCounter().inc();
                            oracle.processElement(
                                    present[index]
                                            ? new StreamRecord<>(value, times[index])
                                            : new StreamRecord<>(value));
                        }
                        group.getIOMetricGroup()
                                .getNumRecordsOutCounter()
                                .inc(oracle.extractOutputStreamRecords().size());
                        for (var event : oracle.getOutput())
                            StageEventBytes.encode(
                                    DistinctCountFlinkOracle.OUTPUT,
                                    (org.apache.flink.streaming.runtime.streamrecord.StreamElement) event,
                                    expected);
                        oracle.getOutput().clear();
                        try (var batch = ArrowRowDataBatch.transpose(rows, DistinctCountFlinkOracle.INPUT, allocator)
                                .withEnvelope(kinds, present, times)) {
                            nativePlan.processElement(0, new StreamRecord<>(batch));
                        }
                        assertThat(nativePlan.output.getCopyOfBuffer()).isEqualTo(expected.getCopyOfBuffer());
                        nativePlan.output.clear();
                        expected.clear();
                        inputWatermark.setCurrentWatermark(timestamp);
                        oracle.processWatermark(new Watermark(timestamp));
                        for (var event : oracle.getOutput()) {
                            if (event instanceof Watermark)
                                outputWatermark.setCurrentWatermark(((Watermark) event).getTimestamp());
                            StageEventBytes.encode(
                                    DistinctCountFlinkOracle.OUTPUT,
                                    (org.apache.flink.streaming.runtime.streamrecord.StreamElement) event,
                                    expected);
                        }
                        oracle.getOutput().clear();
                        nativePlan.processWatermark(0, new Watermark(timestamp));
                        var actualControls = new DataOutputSerializer(128);
                        for (var event : nativePlan.controls)
                            StageEventBytes.encode(DistinctCountFlinkOracle.OUTPUT, event, actualControls);
                        assertThat(actualControls.getCopyOfBuffer()).isEqualTo(expected.getCopyOfBuffer());
                        nativePlan.controls.clear();
                        expected.clear();
                        RegisteredMetricSurface.compare(
                                RegisteredMetricSurface.metrics(group), RegisteredMetricSurface.metrics(nativeGroup));
                    }
                }
            }
            assertThat(allocator.getAllocatedMemory()).isZero();
        }
    }
}
