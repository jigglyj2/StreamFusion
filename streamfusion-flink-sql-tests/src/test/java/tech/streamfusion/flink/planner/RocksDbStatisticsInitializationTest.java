/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.View;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.util.KeyedMultiInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchSerializer;
import tech.streamfusion.flink.operator.StreamFusionNativeRegionOperatorFactory;
import tech.streamfusion.flink.state.StreamFusionStateBackend;

/** The production region must register its metric views during state initialization, before open. */
class RocksDbStatisticsInitializationTest {
    @Test
    void tickerViewsExistBeforeOperatorOpenAndAreNotReplacedByOpen() throws Exception {
        try (var harness = new Harness()) {
            harness.initializeEmptyState();
            var before = harness.metrics();
            assertThat(before).hasSize(11);
            before.values().forEach(metric -> {
                assertThat(metric).isInstanceOf(View.class);
                assertThat(((Gauge<?>) metric).getValue()).isEqualTo(0L);
            });
            harness.open();
            assertThat(harness.metrics()).containsExactlyInAnyOrderEntriesOf(before);
        }
    }

    @Test
    void restoredTickersCanBeSampledBeforeOpenAndFreezeOnEarlyClose() throws Exception {
        OperatorSubtaskState saved;
        try (var source = SharedAggregateRuntimeHarness.configured(
                        true, null, RocksDbStatisticsProfiles.allTickers(), 16L << 20);
                var allocator = new org.apache.arrow.memory.RootAllocator(1L << 20)) {
            var row = org.apache.flink.table.data.GenericRowData.of(
                    org.apache.flink.table.data.StringData.fromString("restored"), 7L);
            try (var batch = ArrowRowDataBatch.transpose(List.of(row), SharedAggregateFlinkOracle.INPUT, allocator)) {
                source.processElement(0, new org.apache.flink.streaming.runtime.streamrecord.StreamRecord<>(batch));
            }
            saved = source.snapshot(1, 1);
        }
        Map<String, Metric> metrics;
        Map<String, Object> values = new java.util.HashMap<>();
        try (var restored = new Harness()) {
            restored.initializeState(saved);
            metrics = restored.metrics();
            assertThat(metrics).hasSize(11);
            metrics.values().forEach(metric -> ((View) metric).update());
            assertThat((Long) ((Gauge<?>) metrics.get("rocksdb.iter_bytes_read")).getValue())
                    .isPositive();
            metrics.forEach((name, metric) -> values.put(name, ((Gauge<?>) metric).getValue()));
            // Cancel during initialization before harness.close() invokes the terminal finish hook.
            restored.closeRegion();
        } finally {
            saved.discardState();
        }
        metrics.forEach((name, metric) -> {
            ((View) metric).update();
            assertThat(((Gauge<?>) metric).getValue()).isEqualTo(values.get(name));
        });
    }

    private static final class Harness extends KeyedMultiInputStreamOperatorTestHarness<Integer, ArrowRowDataBatch> {
        Harness() throws Exception {
            super(
                    new StreamFusionNativeRegionOperatorFactory(
                            List.of(SharedAggregateFlinkOracle.INPUT),
                            SharedAggregateFlinkOracle.OUTPUT,
                            SharedAggregateRegionParityTest.plan(),
                            List.of(3L),
                            List.of(tech.streamfusion.flink.exchange.NativeExchangePlanSerializer.singleton(
                                    SharedAggregateFlinkOracle.INPUT))),
                    16,
                    1,
                    0);
            SharedAggregateHarnessMemory.configure(getEnvironment(), 16L << 20);
            config.setStateKeySerializer(org.apache.flink.api.common.typeutils.base.IntSerializer.INSTANCE);
            setKeySelector(0, ignored -> 0);
            var options = RocksDbStatisticsProfiles.allTickers();
            setStateBackend(new StreamFusionStateBackend(
                    new org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend(true)
                            .configure(options, getClass().getClassLoader()),
                    options));
            setup(ArrowRowDataBatchSerializer.INSTANCE);
        }

        void closeRegion() throws Exception {
            operator.close();
        }

        Map<String, Metric> metrics() throws Exception {
            var metrics = RegisteredMetricSurface.metrics(SharedAggregateMetricSurfaceTest.stageGroup(operator, 3));
            metrics.keySet().removeIf(name -> !name.startsWith("rocksdb."));
            return metrics;
        }
    }
}
