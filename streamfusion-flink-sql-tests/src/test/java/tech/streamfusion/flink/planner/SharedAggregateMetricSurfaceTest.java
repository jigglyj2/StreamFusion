/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.metrics.Metric;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;

/** Compare registered metrics, not a hand-maintained list that can omit a Flink metric. */
class SharedAggregateMetricSurfaceTest {
    @Test
    void synchronousStageMatchesSqlGeneratedFlinkMetricSurfaceAndDeterministicValues() throws Exception {
        for (boolean rocks : List.of(false, true)) {
            try (var oracle = SharedAggregateFlinkOracle.create(rocks);
                    var nativeHarness = new SharedAggregateRuntimeHarness(rocks, null);
                    var allocator = new RootAllocator(64L << 20)) {
                Object flinkGroup = oracle.getOperator().getMetricGroup();
                Object nativeGroup = stageGroup(nativeHarness, 3);
                // OperatorChain/OneInputStreamTask register these, not the bare operator
                // harness. Use Flink's actual gauges and advance output only on emitted marks.
                var inputWatermark = new org.apache.flink.streaming.runtime.metrics.WatermarkGauge();
                var outputWatermark = new org.apache.flink.streaming.runtime.metrics.WatermarkGauge();
                oracle.getOperator().getMetricGroup().gauge("currentInputWatermark", inputWatermark);
                oracle.getOperator().getMetricGroup().gauge("currentOutputWatermark", outputWatermark);
                compare(metrics(flinkGroup), metrics(nativeGroup));
                for (int arrival = 0; arrival < 4; arrival++) {
                    var rows = new ArrayList<GenericRowData>();
                    for (int row = 0; row < 16; row++) {
                        var value = GenericRowData.of(
                                row % 5 == 0 ? null : StringData.fromString("é-" + row % 3),
                                row % 4 == 0 ? null : (long) row);
                        value.setRowKind(
                                arrival % 2 == 0
                                        ? row % 2 == 0
                                                ? org.apache.flink.types.RowKind.INSERT
                                                : org.apache.flink.types.RowKind.UPDATE_AFTER
                                        : row % 2 == 0
                                                ? org.apache.flink.types.RowKind.DELETE
                                                : org.apache.flink.types.RowKind.UPDATE_BEFORE);
                        rows.add(value);
                        // Harnesses omit the task's counting input/output wrappers. Reproduce
                        // their logical-record increments, using the actual Flink output only.
                        oracle.getOperator()
                                .getMetricGroup()
                                .getIOMetricGroup()
                                .getNumRecordsInCounter()
                                .inc();
                        oracle.processElement(new StreamRecord<>(value));
                    }
                    oracle.getOperator()
                            .getMetricGroup()
                            .getIOMetricGroup()
                            .getNumRecordsOutCounter()
                            .inc(oracle.extractOutputStreamRecords().size());
                    var expectedChangelog = new HashMap<String, List<String>>();
                    for (var record : oracle.extractOutputStreamRecords())
                        SharedAggregateRuntimeHarness.record(expectedChangelog, record.getValue(), null);
                    oracle.getOutput().clear();
                    try (var batch = ArrowRowDataBatch.transpose(rows, SharedAggregateFlinkOracle.INPUT, allocator)
                            .withRowKinds(rows.stream()
                                    .map(GenericRowData::getRowKind)
                                    .toArray(org.apache.flink.types.RowKind[]::new))) {
                        nativeHarness.processElement(0, new StreamRecord<>(batch));
                    }
                    assertThat(nativeHarness.changelog).containsExactlyInAnyOrderEntriesOf(expectedChangelog);
                    nativeHarness.changelog.clear();
                    inputWatermark.setCurrentWatermark(100 + arrival);
                    oracle.processWatermark(new Watermark(100 + arrival));
                    for (var output : oracle.getOutput())
                        if (output instanceof Watermark)
                            outputWatermark.setCurrentWatermark(((Watermark) output).getTimestamp());
                    nativeHarness.processWatermark(0, new Watermark(100 + arrival));
                    compare(metrics(flinkGroup), metrics(nativeGroup));
                    oracle.getOutput().clear();
                }
            }
        }
    }

    static void compare(Map<String, Metric> expected, Map<String, Metric> actual) {
        RegisteredMetricSurface.compare(expected, actual);
    }

    static Map<String, Metric> metrics(Object group) throws Exception {
        return RegisteredMetricSurface.metrics(group);
    }

    static Object stageGroup(SharedAggregateRuntimeHarness harness, long id) throws Exception {
        return stageGroup(harness.region(), id);
    }

    static Object stageGroup(Object operator, long id) throws Exception {
        var field = operator.getClass().getDeclaredField("metricTree");
        field.setAccessible(true);
        var tree = field.get(operator);
        var stages = tree.getClass().getDeclaredField("stages");
        stages.setAccessible(true);
        var stage = ((Map<?, ?>) stages.get(tree)).get(id);
        var group = stage.getClass().getDeclaredField("group");
        group.setAccessible(true);
        return group.get(stage);
    }
}
