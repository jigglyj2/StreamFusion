/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.streaming.runtime.metrics.WatermarkGauge;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;

/** Flink skips only leading absent-state retractions, not retractions after a bundle reaches zero. */
class SharedMiniBatchRetractionParityTest {
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void generatedZeroCrossingsMatchFlinkChangelogAndCompleteMetrics(boolean rocks) throws Exception {
        for (int chunkSize : List.of(1, 2, 4))
            for (int trigger : List.of(4, 100))
                try (var oracle = SharedAggregateFlinkOracle.create(rocks, trigger);
                        var target = new SharedAggregateRuntimeHarness(
                                rocks, null, SharedMiniBatchControlTest.plan(trigger));
                        var allocator = new RootAllocator(64L << 20)) {
                    var outputWatermark = new WatermarkGauge();
                    oracle.getOperator().getMetricGroup().gauge("currentInputWatermark", new WatermarkGauge());
                    oracle.getOperator().getMetricGroup().gauge("currentOutputWatermark", outputWatermark);
                    for (int seed = 0; seed < 3; seed++) {
                        String key = seed == 0 ? null : "é-" + seed;
                        Long value = seed == 1 ? null : 10L + seed;
                        var rows = List.of(
                                row(key, value, RowKind.INSERT),
                                row(key, value, RowKind.DELETE),
                                row(key, 20L + seed, RowKind.UPDATE_BEFORE),
                                row(key, 20L + seed, RowKind.UPDATE_AFTER),
                                row("absent", value, RowKind.DELETE),
                                row("absent", value, RowKind.UPDATE_BEFORE),
                                row("live", value, RowKind.INSERT),
                                row("live", 30L + seed, RowKind.UPDATE_AFTER));
                        for (int offset = 0; offset < rows.size(); offset += chunkSize) {
                            var chunk = rows.subList(offset, offset + chunkSize);
                            for (var row : chunk) {
                                // Task wrappers provide these logical-record counters in production.
                                oracle.getOperator()
                                        .getMetricGroup()
                                        .getIOMetricGroup()
                                        .getNumRecordsInCounter()
                                        .inc();
                                oracle.processElement(new StreamRecord<>(row));
                            }
                            try (var batch = ArrowRowDataBatch.transpose(
                                            chunk, SharedAggregateFlinkOracle.INPUT, allocator)
                                    .withRowKinds(chunk.stream()
                                            .map(GenericRowData::getRowKind)
                                            .toArray(RowKind[]::new))) {
                                target.processElement(0, new StreamRecord<>(batch));
                            }
                            SharedMiniBatchMetricSurfaceTest.check(target, oracle, outputWatermark);
                        }
                        oracle.getOperator().prepareSnapshotPreBarrier(seed);
                        target.region().prepareSnapshotPreBarrier(seed);
                        SharedMiniBatchMetricSurfaceTest.check(target, oracle, outputWatermark);
                    }
                    oracle.getOperator().finish();
                    target.region().endInput(1);
                    SharedMiniBatchMetricSurfaceTest.check(target, oracle, outputWatermark);
                }
    }

    private static GenericRowData row(String key, Long value, RowKind kind) {
        var row = GenericRowData.of(key == null ? null : StringData.fromString(key), value);
        row.setRowKind(kind);
        return row;
    }
}
