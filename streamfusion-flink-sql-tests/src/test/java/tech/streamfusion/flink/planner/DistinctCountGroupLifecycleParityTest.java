/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;

/** Group deletion must clear accumulators and DISTINCT data views before the next input row. */
class DistinctCountGroupLifecycleParityTest {
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void deletionAndRecreationMatchFlinkAcrossBatchBoundaries(boolean rocks) throws Exception {
        for (int batchSize : new int[] {1, 3, 7, 64}) {
            try (var oracle = DistinctCountFlinkOracle.create(rocks);
                    var nativePlan = new KeyedNativeMetricHarness(
                            rocks,
                            DistinctCountFixture.plan(),
                            List.of(DistinctCountFlinkOracle.INPUT),
                            DistinctCountFlinkOracle.OUTPUT,
                            List.of(3L));
                    var allocator = new RootAllocator(64L << 20)) {
                var group = oracle.getOperator().getMetricGroup();
                var nativeGroup = SharedAggregateMetricSurfaceTest.stageGroup(nativePlan.region(), 3);
                group.gauge("currentInputWatermark", new org.apache.flink.streaming.runtime.metrics.WatermarkGauge());
                group.gauge("currentOutputWatermark", new org.apache.flink.streaming.runtime.metrics.WatermarkGauge());
                var rows = changes();
                for (int start = 0; start < rows.size(); start += batchSize) {
                    var arrival = rows.subList(start, Math.min(rows.size(), start + batchSize));
                    var kinds = new RowKind[arrival.size()];
                    var present = new boolean[arrival.size()];
                    var times = new long[arrival.size()];
                    for (int index = 0; index < arrival.size(); index++) {
                        var row = arrival.get(index);
                        kinds[index] = row.getRowKind();
                        present[index] = (start + index) % 3 != 0;
                        times[index] = start + index;
                        group.getIOMetricGroup().getNumRecordsInCounter().inc();
                        oracle.processElement(
                                present[index] ? new StreamRecord<>(row, times[index]) : new StreamRecord<>(row));
                    }
                    var expected = new DataOutputSerializer(128);
                    var records = oracle.extractOutputStreamRecords();
                    group.getIOMetricGroup().getNumRecordsOutCounter().inc(records.size());
                    for (var record : records)
                        StageEventBytes.encode(DistinctCountFlinkOracle.OUTPUT, record, expected);
                    oracle.getOutput().clear();
                    try (var batch = ArrowRowDataBatch.transpose(arrival, DistinctCountFlinkOracle.INPUT, allocator)
                            .withEnvelope(kinds, present, times)) {
                        nativePlan.processElement(0, new StreamRecord<>(batch));
                    }
                    assertThat(nativePlan.output.getCopyOfBuffer())
                            .as("backend=%s batchSize=%s start=%s", rocks, batchSize, start)
                            .containsExactly(expected.getCopyOfBuffer());
                    nativePlan.output.clear();
                    RegisteredMetricSurface.compare(
                            RegisteredMetricSurface.metrics(group), RegisteredMetricSurface.metrics(nativeGroup));
                }
                assertThat(allocator.getAllocatedMemory()).isZero();
            }
        }
    }

    private static List<GenericRowData> changes() {
        var rows = new ArrayList<GenericRowData>();
        for (int seed : new int[] {3, 29, 197}) {
            var random = new Random(seed);
            for (int cycle = 0; cycle < 12; cycle++) {
                var key = cycle % 3 == 0 ? null : StringData.fromString("é\u0000-" + seed + "-" + cycle);
                long value = random.nextLong();
                // A mismatched retraction reaches row count zero while membership data remains.
                // Flink clears all data views, ignores the next orphan retract, and starts fresh.
                rows.add(row(key, value, true, RowKind.INSERT));
                rows.add(row(key, value ^ Long.MIN_VALUE, false, RowKind.DELETE));
                rows.add(row(key, value, true, RowKind.UPDATE_BEFORE));
                rows.add(row(key, value, null, RowKind.UPDATE_AFTER));
                rows.add(row(key, null, true, RowKind.INSERT));
                rows.add(row(key, value, null, RowKind.DELETE));
                rows.add(row(key, null, true, RowKind.UPDATE_BEFORE));
                rows.add(row(key, value, true, RowKind.INSERT));
                rows.add(row(key, value, true, RowKind.DELETE));
            }
        }
        return rows;
    }

    private static GenericRowData row(StringData key, Long value, Boolean selected, RowKind kind) {
        var row = GenericRowData.of(key, value, selected);
        row.setRowKind(kind);
        return row;
    }
}
