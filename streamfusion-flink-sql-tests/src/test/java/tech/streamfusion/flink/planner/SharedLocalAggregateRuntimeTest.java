/* Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0 */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.*;
import static tech.streamfusion.flink.planner.SharedAggregateMetricSurfaceTest.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.metrics.WatermarkGauge;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.CollectorOutput;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchSerializer;
import tech.streamfusion.flink.operator.StreamFusionArrowNativeOperator;

/** Local buffering on the ordinary unary owner: no aggregate-specific Java runtime. */
class SharedLocalAggregateRuntimeTest {
    @Test
    void defaultMetricsAndAutomaticControlsMatchSqlPlannedFlinkLocalOperator() throws Exception {
        for (int trigger : List.of(1, 7, 100)) {
            var nativeOperator = StreamFusionArrowNativeOperator.forRegion(
                    SharedAggregateFlinkOracle.INPUT,
                    GlobalPartialFixtures.PARTIAL,
                    LocalAggregateFixtures.plan(trigger),
                    "local-region-test");
            var events = new ArrayList<String>();
            try (var allocator = new RootAllocator(64L << 20);
                    var oracle = SharedLocalAggregateFlinkOracle.create(trigger);
                    var target = new OneInputStreamOperatorTestHarness<ArrowRowDataBatch, ArrowRowDataBatch>(
                            nativeOperator)) {
                target.setOutputCreator(ignored -> new CollectorOutput<ArrowRowDataBatch>(new ArrayList<>()) {
                    @Override
                    public void collect(StreamRecord<ArrowRowDataBatch> record) {
                        nativeOperator
                                .getMetricGroup()
                                .getIOMetricGroup()
                                .getNumRecordsOutCounter()
                                .inc();
                        var batch = record.getValue();
                        for (int row = 0; row < batch.size(); row++) {
                            assertThat(batch.rowKind(row)).isEqualTo(RowKind.INSERT);
                            assertThat(batch.hasTimestamp(row)).isFalse();
                        }
                        events.add("rows:" + batch.size());
                    }

                    @Override
                    public void emitWatermark(Watermark mark) {
                        events.add("watermark:" + mark.getTimestamp());
                    }
                });
                target.setup(ArrowRowDataBatchSerializer.INSTANCE);
                target.open();
                var inputWatermark = new WatermarkGauge();
                var outputWatermark = new WatermarkGauge();
                oracle.getOperator().getMetricGroup().gauge("currentInputWatermark", inputWatermark);
                oracle.getOperator().getMetricGroup().gauge("currentOutputWatermark", outputWatermark);
                var group = (OperatorMetricGroup) stageGroup(nativeOperator, (1L << 32) | 3);
                for (int phase = 0; phase < 5; phase++) {
                    var rows = new ArrayList<GenericRowData>();
                    var random = new Random(phase);
                    for (int i = 0; i < (phase == 2 ? 0 : 23); i++) {
                        var row = GenericRowData.of(
                                i % 7 == 0 ? null : StringData.fromString("é-" + random.nextInt(4)),
                                i % 5 == 0 ? null : (long) random.nextInt(11));
                        row.setRowKind(RowKind.values()[i % 4]);
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
                        nativeOperator
                                .getMetricGroup()
                                .getIOMetricGroup()
                                .getNumRecordsInCounter()
                                .inc();
                        target.processElement(new StreamRecord<>(batch));
                    }
                    check(oracle, group);
                    events.clear();
                    if (phase == 0) {
                        inputWatermark.setCurrentWatermark(100);
                        oracle.processWatermark(new Watermark(100));
                        outputWatermark.setCurrentWatermark(100);
                        target.processWatermark(new Watermark(100));
                        assertThat(events.get(events.size() - 1)).isEqualTo("watermark:100");
                    } else if (phase == 3) {
                        oracle.getOperator().prepareSnapshotPreBarrier(11);
                        nativeOperator.prepareSnapshotPreBarrier(11);
                    } else if (phase == 4) {
                        oracle.getOperator().finish();
                        nativeOperator.endInput();
                        nativeOperator.finish();
                        try (var batch = ArrowRowDataBatch.empty(SharedAggregateFlinkOracle.INPUT, allocator)) {
                            assertThatThrownBy(() -> target.processElement(new StreamRecord<>(batch)))
                                    .hasMessageContaining("input ended");
                        }
                    }
                    check(oracle, group);
                }
            }
        }
    }

    private static void check(
            OneInputStreamOperatorTestHarness<org.apache.flink.table.data.RowData, org.apache.flink.table.data.RowData>
                    oracle,
            OperatorMetricGroup actual)
            throws Exception {
        // The harness does not install the task's output counting wrapper.
        oracle.getOperator()
                .getMetricGroup()
                .getIOMetricGroup()
                .getNumRecordsOutCounter()
                .inc(oracle.extractOutputStreamRecords().size());
        oracle.getOutput().clear();
        compare(metrics(oracle.getOperator().getMetricGroup()), metrics(actual));
    }
}
