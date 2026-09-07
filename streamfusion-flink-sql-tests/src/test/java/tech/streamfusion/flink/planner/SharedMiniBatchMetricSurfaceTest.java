/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;
import static tech.streamfusion.flink.planner.SharedAggregateMetricSurfaceTest.compare;
import static tech.streamfusion.flink.planner.SharedAggregateMetricSurfaceTest.metrics;
import static tech.streamfusion.flink.planner.SharedAggregateMetricSurfaceTest.stageGroup;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.core.memory.DataOutputSerializer;
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
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;

/** Complete registered surface of the SQL-planned Flink mini-batch operator, at batch/control edges. */
class SharedMiniBatchMetricSurfaceTest {
    @Test
    void generatedPendingAndFlushMetricsMatchFlinkOnBothBackends() throws Exception {
        for (boolean rocks : List.of(false, true))
            for (int trigger : List.of(1, 7, 100))
                try (var oracle = SharedAggregateFlinkOracle.create(rocks, trigger);
                        var target = new SharedAggregateRuntimeHarness(
                                rocks, null, SharedMiniBatchControlTest.plan(trigger));
                        var allocator = new RootAllocator(64L << 20)) {
                    var inputWatermark = new WatermarkGauge();
                    var outputWatermark = new WatermarkGauge();
                    oracle.getOperator().getMetricGroup().gauge("currentInputWatermark", inputWatermark);
                    oracle.getOperator().getMetricGroup().gauge("currentOutputWatermark", outputWatermark);
                    check(target, oracle, outputWatermark);
                    var live = new ArrayList<GenericRowData>();
                    for (int seed = 0; seed < 6; seed++) {
                        var random = new Random(seed);
                        var rows = new ArrayList<GenericRowData>();
                        // Includes an empty Arrow arrival and tails that do not reach the count trigger.
                        for (int i = 0; i < (seed == 2 ? 0 : 23); i++) {
                            GenericRowData row;
                            if (!live.isEmpty() && random.nextBoolean()) {
                                row = live.remove(random.nextInt(live.size()));
                                row.setRowKind(i % 2 == 0 ? RowKind.DELETE : RowKind.UPDATE_BEFORE);
                            } else {
                                row = GenericRowData.of(
                                        i % 7 == 0 ? null : StringData.fromString("é-" + random.nextInt(4)),
                                        i % 5 == 0 ? null : (long) random.nextInt(11));
                                row.setRowKind(i % 2 == 0 ? RowKind.INSERT : RowKind.UPDATE_AFTER);
                                live.add(GenericRowData.of(row.getField(0), row.getField(1)));
                            }
                            rows.add(row);
                            oracle.getOperator()
                                    .getMetricGroup()
                                    .getIOMetricGroup()
                                    .getNumRecordsInCounter()
                                    .inc();
                            oracle.processElement(new StreamRecord<>(row, i));
                        }
                        try (var batch = ArrowRowDataBatch.transpose(rows, SharedAggregateFlinkOracle.INPUT, allocator)
                                .withRowKinds(rows.stream()
                                        .map(GenericRowData::getRowKind)
                                        .toArray(RowKind[]::new))) {
                            target.processElement(0, new StreamRecord<>(batch));
                        }
                        check(target, oracle, outputWatermark);
                        if (seed == 1) {
                            inputWatermark.setCurrentWatermark(100);
                            oracle.processWatermark(new Watermark(100));
                            target.processWatermark(0, new Watermark(100));
                        } else if (seed == 3) {
                            oracle.getOperator().prepareSnapshotPreBarrier(8);
                            target.region().prepareSnapshotPreBarrier(8);
                        } else if (seed == 5) {
                            oracle.getOperator().finish();
                            target.region().endInput(1);
                            target.region().finish();
                        }
                        check(target, oracle, outputWatermark);
                    }
                }
    }

    static void check(
            SharedAggregateRuntimeHarness target,
            KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> oracle,
            WatermarkGauge outputWatermark)
            throws Exception {
        var records = oracle.extractOutputStreamRecords();
        // Harnesses do not install Flink's task-level counting wrappers.
        oracle.getOperator()
                .getMetricGroup()
                .getIOMetricGroup()
                .getNumRecordsOutCounter()
                .inc(records.size());
        for (var event : oracle.getOutput())
            if (event instanceof Watermark) outputWatermark.setCurrentWatermark(((Watermark) event).getTimestamp());
        var expected = new DataOutputSerializer(128);
        var serializer = new RowDataSerializer(SharedAggregateFlinkOracle.OUTPUT);
        for (var record : records) {
            assertThat(record.hasTimestamp()).isFalse();
            serializer.serialize(record.getValue(), expected);
        }
        assertThat(target.captured.getCopyOfBuffer()).isEqualTo(expected.getCopyOfBuffer());
        assertThat(target.times).hasSize(records.size()).allMatch(java.util.Objects::isNull);
        compare(metrics(oracle.getOperator().getMetricGroup()), metrics(stageGroup(target, 3)));
        target.captured.clear();
        target.times.clear();
        oracle.getOutput().clear();
    }
}
