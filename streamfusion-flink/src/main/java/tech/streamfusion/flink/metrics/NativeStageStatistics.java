/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.metrics;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.LongFunction;
import java.util.function.Supplier;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.View;
import tech.streamfusion.proto.plan.v1.NativeGaugeSchema;
import tech.streamfusion.proto.plan.v1.NativeGaugeValueKind;
import tech.streamfusion.proto.plan.v1.NativeMetricKind;

/** Flink database ticker Views; one native sample per updater round across all fused stages. */
final class NativeStageStatistics implements AutoCloseable {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(NativeStageStatistics.class);
    private static final Set<String> NAMES = Set.of(
            "rocksdb.block_cache_hit",
            "rocksdb.block_cache_miss",
            "rocksdb.bloom_filter_useful",
            "rocksdb.bloom_filter_full_positive",
            "rocksdb.bloom_filter_full_true_positive",
            "rocksdb.bytes_read",
            "rocksdb.iter_bytes_read",
            "rocksdb.bytes_written",
            "rocksdb.compact_read_bytes",
            "rocksdb.compact_write_bytes",
            "rocksdb.stall_micros");
    private final List<TickerView> views = new ArrayList<>();
    private final BitSet sampled = new BitSet();
    private Supplier<long[]> snapshot;
    private long[] values;

    NativeStageStatistics(byte[] bytes, LongFunction<MetricGroup> scope, Supplier<long[]> snapshot) {
        final NativeGaugeSchema schema;
        try {
            schema = NativeGaugeSchema.parseFrom(bytes);
        } catch (com.google.protobuf.InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid native RocksDB statistics schema", failure);
        }
        if (schema.getProtocolVersion() != 1)
            throw new IllegalArgumentException("Unsupported statistics schema version");
        Set<String> identities = new HashSet<>();
        List<MetricGroup> groups = new ArrayList<>();
        for (var descriptor : schema.getGaugesList()) {
            var group = scope.apply(descriptor.getPlanNodeId());
            if (descriptor.getPlanNodeId() <= 0
                    || group == null
                    || !NAMES.contains(descriptor.getName())
                    || descriptor.getGroupsCount() != 0
                    || !descriptor.getMeterName().isEmpty()
                    || descriptor.getValueKind() != NativeGaugeValueKind.NATIVE_GAUGE_VALUE_KIND_INT64
                    || descriptor.getMetricKind() != NativeMetricKind.NATIVE_METRIC_KIND_GAUGE
                    || !identities.add(descriptor.getPlanNodeId() + ":" + descriptor.getName())) {
                throw new IllegalArgumentException("Invalid native RocksDB statistics descriptor: " + descriptor);
            }
            groups.add(group);
        }
        this.snapshot = java.util.Objects.requireNonNull(snapshot);
        for (int index = 0; index < schema.getGaugesCount(); index++) views.add(new TickerView(index));
        for (int index = 0; index < views.size(); index++) {
            groups.get(index).gauge(schema.getGauges(index).getName(), views.get(index));
        }
    }

    private synchronized void update(int index) {
        if (snapshot == null) return;
        // Flink iterates its View set once per round; the order is unspecified. A repeated
        // view starts a new round as well, supporting reporters/tests that update one view.
        if (sampled.isEmpty() || sampled.get(index)) {
            final long[] next;
            try {
                next = snapshot.get();
                if (next.length != views.size())
                    throw new IllegalStateException("Native statistics snapshot shape changed");
            } catch (RuntimeException failure) {
                // ViewUpdater does not catch exceptions per view. Keep an admission/reader
                // failure from stopping every Flink view in the TaskManager.
                close();
                LOG.warn("Failed to sample native RocksDB statistics; retaining the last values", failure);
                return;
            }
            values = next;
            sampled.clear();
        }
        views.get(index).value = values[index];
        sampled.set(index);
        if (sampled.cardinality() == views.size()) sampled.clear();
    }

    @Override
    public synchronized void close() {
        snapshot = null;
        values = null;
        sampled.clear();
    }

    private final class TickerView implements Gauge<Long>, View {
        private final int index;
        private volatile long value;

        private TickerView(int index) {
            this.index = index;
        }

        @Override
        public Long getValue() {
            return value;
        }

        @Override
        public void update() {
            NativeStageStatistics.this.update(index);
        }
    }
}
