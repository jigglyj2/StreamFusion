/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.state;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend;
import org.apache.flink.state.rocksdb.RocksDBMemoryConfiguration;
import org.apache.flink.state.rocksdb.RocksDBOptions;
import org.junit.jupiter.api.Test;
import tech.streamfusion.nativebridge.NativeStateResources;
import tech.streamfusion.proto.plan.v1.NativeRocksDbState;
import tech.streamfusion.proto.plan.v1.NativeStateBinding;
import tech.streamfusion.proto.plan.v1.NativeStateBindings;

class NativeRocksDbMemoryConfigurationTest {
    @Test
    void typedResolutionMatchesFlinkAndSurvivesVersionedTaskBindings() throws Exception {
        for (double write : new double[] {0.2, 0.5, 0.7}) {
            for (double high : new double[] {0.1, 0.2}) {
                var config = new Configuration();
                config.set(StateBackendOptions.STATE_BACKEND, "rocksdb");
                config.set(RocksDBOptions.WRITE_BUFFER_RATIO, write);
                config.set(RocksDBOptions.HIGH_PRIORITY_POOL_RATIO, high);
                config.set(RocksDBOptions.USE_PARTITIONED_INDEX_FILTERS, high == 0.2);
                var flink = new EmbeddedRocksDBStateBackend()
                        .configure(config, getClass().getClassLoader());
                var actual = NativeRocksDbMemoryConfiguration.fromConfig(config);
                assertThat(actual).isEqualTo(NativeRocksDbMemoryConfiguration.fromBackend(flink));
                assertThat(actual.writeBufferRatio())
                        .isEqualTo(flink.getMemoryConfiguration().getWriteBufferRatio());
                assertThat(actual.partitionedIndexFilters())
                        .isEqualTo(flink.getMemoryConfiguration().isUsingPartitionedIndexFilters());
                assertThat(actual.highPriorityPoolRatio())
                        .isEqualTo(flink.getMemoryConfiguration().getHighPriorityPoolRatio());
                assertThat(NativeStateConfigurationSupport.unsupportedReason(config))
                        .isNull();
                var binding = actual.bind(NativeStateBinding.newBuilder()
                        .setPlanNodeId(1)
                        .setRocksdb(NativeRocksDbState.newBuilder().setMemoryLimit(1 << 20))
                        .build());
                var decoded = NativeStateBindings.parseFrom(NativeStateResources.serialize(List.of(binding)));
                assertThat(decoded.getProtocolVersion()).isEqualTo(8);
                assertThat(decoded.getBindings(0).getRocksdb().getPartitionedIndexFilters())
                        .isEqualTo(high == 0.2);
                assertThat(decoded.getBindings(0).getRocksdb().getWriteBufferRatio())
                        .isEqualTo(write);
                assertThat(decoded.getBindings(0).getRocksdb().getHighPriorityPoolRatio())
                        .isEqualTo(high);
            }
        }
    }

    @Test
    void invalidGeometryRetainsFallbackInsteadOfAllocatingAConflictingCache() throws Exception {
        for (double[] ratios : new double[][] {{0.8, 0.5}, {0, 0.1}, {0.5, 1}, {Double.NaN, 0.1}}) {
            var config = new Configuration();
            config.set(StateBackendOptions.STATE_BACKEND, "rocksdb");
            config.set(RocksDBOptions.WRITE_BUFFER_RATIO, ratios[0]);
            config.set(RocksDBOptions.HIGH_PRIORITY_POOL_RATIO, ratios[1]);
            assertThat(NativeStateConfigurationSupport.unsupportedReason(config))
                    .contains("RocksDB configuration");
            assertThatThrownBy(() -> new NativeRocksDbMemoryConfiguration(ratios[0], ratios[1]))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        var invalid = new RocksDBMemoryConfiguration();
        invalid.setWriteBufferRatio(0.8);
        invalid.setHighPriorityPoolRatio(0.5);
        assertThatThrownBy(invalid::validate).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void resourceGeometryFollowsFlinksFirstSharedOwnerAndLastRelease() throws Exception {
        var manager = MemoryManager.create(16 << 20, 32 << 10);
        var first = new NativeRocksDbMemoryConfiguration(0.7, 0.2, true);
        var second = new NativeRocksDbMemoryConfiguration(0.2, 0.1);
        try {
            try (var a = NativeRocksDbMemoryLease.reserve(manager, 0.5, first);
                    var b = NativeRocksDbMemoryLease.reserve(manager, 0.5, second)) {
                assertThat(a.scopeId()).isEqualTo(b.scopeId());
                assertThat(a.configuration()).isEqualTo(first);
                assertThat(b.configuration()).isEqualTo(first);
                assertThat(a.size()).isEqualTo(8 << 20);
            }
            assertThat(manager.verifyEmpty()).isTrue();
            try (var next = NativeRocksDbMemoryLease.reserve(manager, 0.5, second)) {
                assertThat(next.configuration()).isEqualTo(second);
            }
            assertThat(manager.verifyEmpty()).isTrue();
        } finally {
            manager.shutdown();
        }
    }
}
