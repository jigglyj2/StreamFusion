/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;

class StreamFusionStatefulOperatorMetricsTest {
    private static final RowType ROW_TYPE = RowType.of(new BigIntType());

    @Test
    void reportsProcessingChangelogStateCheckpointRestoreAndFailureMetrics() {
        RecordingMetricGroup group = new RecordingMetricGroup();
        StreamFusionStatefulOperatorMetrics metrics = new StreamFusionStatefulOperatorMetrics(group, true);

        try (RootAllocator allocator = new RootAllocator(1 << 20);
                ArrowRowDataBatch input = batch(allocator, RowKind.INSERT, RowKind.DELETE, RowKind.INSERT);
                ArrowRowDataBatch output =
                        batch(allocator, RowKind.INSERT, RowKind.UPDATE_BEFORE, RowKind.UPDATE_AFTER, RowKind.DELETE)) {
            metrics.processed(input, output);
        }
        metrics.processingFailed();
        CheckpointStorageLocationReference location = CheckpointStorageLocationReference.getDefault();
        metrics.checkpointCompleted(
                CheckpointOptions.alignedNoTimeout(CheckpointType.CHECKPOINT, location), 100, -1, 0, 10);
        metrics.checkpointCompleted(CheckpointOptions.unaligned(CheckpointType.CHECKPOINT, location), 80, 30, 50, 20);
        metrics.checkpointCompleted(
                CheckpointOptions.alignedNoTimeout(SavepointType.savepoint(SavepointFormatType.CANONICAL), location),
                120,
                -1,
                0,
                30);
        metrics.checkpointFailed();
        metrics.restored(90, 40);
        metrics.restoreFailed();

        assertThat(group.count("processedBatches")).isEqualTo(1);
        assertThat(group.count("processedRows")).isEqualTo(3);
        assertThat(group.count("emittedRows")).isEqualTo(4);
        assertThat(group.count("emittedInserts")).isEqualTo(1);
        assertThat(group.count("emittedUpdateBefores")).isEqualTo(1);
        assertThat(group.count("emittedUpdateAfters")).isEqualTo(1);
        assertThat(group.count("emittedDeletes")).isEqualTo(1);
        assertThat(group.count("stateReadBatches")).isEqualTo(1);
        assertThat(group.count("stateWriteBatches")).isEqualTo(1);
        assertThat(group.count("processingFailures")).isEqualTo(1);
        assertThat(group.count("checkpoints")).isEqualTo(3);
        assertThat(group.count("alignedCheckpoints")).isEqualTo(1);
        assertThat(group.count("unalignedCheckpoints")).isEqualTo(1);
        assertThat(group.count("canonicalSavepoints")).isEqualTo(1);
        assertThat(group.count("incrementalCheckpoints")).isEqualTo(1);
        assertThat(group.count("checkpointBytes")).isEqualTo(300);
        assertThat(group.count("incrementalUploadedBytes")).isEqualTo(30);
        assertThat(group.count("incrementalReusedBytes")).isEqualTo(50);
        assertThat(group.count("checkpointDurationNanos")).isEqualTo(60);
        assertThat(group.count("checkpointFailures")).isEqualTo(1);
        assertThat(group.count("restores")).isEqualTo(1);
        assertThat(group.count("restoreBytes")).isEqualTo(90);
        assertThat(group.count("restoreDurationNanos")).isEqualTo(40);
        assertThat(group.count("restoreFailures")).isEqualTo(1);
        assertThat(group.gaugeValue("rocksDbBackend")).isEqualTo(1);
    }

    private static ArrowRowDataBatch batch(RootAllocator allocator, RowKind... kinds) {
        List<GenericRowData> rows = java.util.stream.IntStream.range(0, kinds.length)
                .mapToObj(index -> {
                    GenericRowData row = new GenericRowData(kinds[index], 1);
                    row.setField(0, (long) index);
                    return row;
                })
                .collect(java.util.stream.Collectors.toList());
        return ArrowRowDataBatch.transpose(rows, ROW_TYPE, allocator).withRowKinds(kinds);
    }

    private static final class RecordingMetricGroup implements MetricGroup {
        private final Map<String, Counter> counters = new HashMap<>();
        private final Map<String, Gauge<?>> gauges = new HashMap<>();

        private long count(String name) {
            return counters.get(name).getCount();
        }

        private Object gaugeValue(String name) {
            return gauges.get(name).getValue();
        }

        @Override
        public Counter counter(String name) {
            return counters.computeIfAbsent(name, ignored -> new SimpleCounter());
        }

        @Override
        public <C extends Counter> C counter(String name, C counter) {
            counters.put(name, counter);
            return counter;
        }

        @Override
        public <T, G extends Gauge<T>> G gauge(String name, G gauge) {
            gauges.put(name, gauge);
            return gauge;
        }

        @Override
        public <H extends Histogram> H histogram(String name, H histogram) {
            return histogram;
        }

        @Override
        public <M extends Meter> M meter(String name, M meter) {
            return meter;
        }

        @Override
        public MetricGroup addGroup(String name) {
            return this;
        }

        @Override
        public MetricGroup addGroup(String key, String value) {
            return this;
        }

        @Override
        public String[] getScopeComponents() {
            return new String[0];
        }

        @Override
        public Map<String, String> getAllVariables() {
            return Map.of();
        }

        @Override
        public String getMetricIdentifier(String metricName) {
            return metricName;
        }

        @Override
        public String getMetricIdentifier(String metricName, CharacterFilter filter) {
            return metricName;
        }
    }
}
