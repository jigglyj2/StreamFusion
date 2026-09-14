/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.state;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.runtime.state.StateBackend;
import tech.streamfusion.proto.plan.v1.NativeStateBinding;

/** Resolves Flink's optional RocksDB memory geometry without a mandatory RocksDB dependency. */
final class NativeRocksDbMemoryConfiguration {
    private final double writeBufferRatio;
    private final double highPriorityPoolRatio;
    private final boolean partitionedIndexFilters;
    static final NativeRocksDbMemoryConfiguration DEFAULT = new NativeRocksDbMemoryConfiguration(0.5, 0.1);

    NativeRocksDbMemoryConfiguration(double writeBufferRatio, double highPriorityPoolRatio) {
        this(writeBufferRatio, highPriorityPoolRatio, false);
    }

    NativeRocksDbMemoryConfiguration(
            double writeBufferRatio, double highPriorityPoolRatio, boolean partitionedIndexFilters) {
        if (!(writeBufferRatio > 0 && writeBufferRatio < 1)
                || !(highPriorityPoolRatio > 0 && highPriorityPoolRatio < 1)
                || 2 * writeBufferRatio / (3 - writeBufferRatio) + highPriorityPoolRatio >= 1) {
            throw new IllegalArgumentException("Invalid Flink RocksDB write-buffer-ratio / high-prio-pool-ratio");
        }
        this.writeBufferRatio = writeBufferRatio;
        this.highPriorityPoolRatio = highPriorityPoolRatio;
        this.partitionedIndexFilters = partitionedIndexFilters;
    }

    double writeBufferRatio() {
        return writeBufferRatio;
    }

    double highPriorityPoolRatio() {
        return highPriorityPoolRatio;
    }

    boolean partitionedIndexFilters() {
        return partitionedIndexFilters;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof NativeRocksDbMemoryConfiguration)) return false;
        var that = (NativeRocksDbMemoryConfiguration) other;
        return writeBufferRatio == that.writeBufferRatio
                && highPriorityPoolRatio == that.highPriorityPoolRatio
                && partitionedIndexFilters == that.partitionedIndexFilters;
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(writeBufferRatio, highPriorityPoolRatio, partitionedIndexFilters);
    }

    static boolean propagates(String key) {
        return key.equals("state.backend.rocksdb.memory.write-buffer-ratio")
                || key.equals("state.backend.rocksdb.memory.high-prio-pool-ratio")
                || key.equals("state.backend.rocksdb.memory.partitioned-index-filters");
    }

    static NativeRocksDbMemoryConfiguration fromConfig(ReadableConfig config) throws ReflectiveOperationException {
        Class<?> options = Class.forName(
                "org.apache.flink.state.rocksdb.RocksDBOptions",
                false,
                Thread.currentThread().getContextClassLoader());
        return new NativeRocksDbMemoryConfiguration(
                (Double) config.get(
                        (ConfigOption<?>) options.getField("WRITE_BUFFER_RATIO").get(null)),
                (Double) config.get((ConfigOption<?>)
                        options.getField("HIGH_PRIORITY_POOL_RATIO").get(null)),
                (Boolean) config.get((ConfigOption<?>)
                        options.getField("USE_PARTITIONED_INDEX_FILTERS").get(null)));
    }

    static NativeRocksDbMemoryConfiguration fromBackend(StateBackend backend) throws ReflectiveOperationException {
        Object memory = backend.getClass().getMethod("getMemoryConfiguration").invoke(backend);
        return new NativeRocksDbMemoryConfiguration(
                (Double) memory.getClass().getMethod("getWriteBufferRatio").invoke(memory),
                (Double) memory.getClass().getMethod("getHighPriorityPoolRatio").invoke(memory),
                (Boolean) memory.getClass()
                        .getMethod("isUsingPartitionedIndexFilters")
                        .invoke(memory));
    }

    NativeStateBinding bind(NativeStateBinding binding) {
        if (!binding.hasRocksdb()) return binding;
        return binding.toBuilder()
                .setRocksdb(binding.getRocksdb().toBuilder()
                        .setWriteBufferRatio(writeBufferRatio)
                        .setHighPriorityPoolRatio(highPriorityPoolRatio)
                        .setPartitionedIndexFilters(partitionedIndexFilters))
                .build();
    }
}
