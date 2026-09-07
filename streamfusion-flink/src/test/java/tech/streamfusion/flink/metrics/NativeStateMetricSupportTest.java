/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Modifier;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateLatencyTrackOptions;
import org.apache.flink.state.rocksdb.RocksDBNativeMetricOptions;
import org.junit.jupiter.api.Test;

class NativeStateMetricSupportTest {
    @Test
    void defaultAndKeyedLatencyContractsUseTheSharedStateGuard() {
        var config = new Configuration();
        assertThat(NativeStateMetricSupport.unsupportedReason(config)).isNull();
        config.set(StateLatencyTrackOptions.LATENCY_TRACK_ENABLED, true);
        assertThat(NativeStateMetricSupport.unsupportedReason(config)).contains("keyed-state latency histograms");
        config.set(StateLatencyTrackOptions.LATENCY_TRACK_ENABLED, false);
        assertThat(NativeStateMetricSupport.unsupportedReason(config)).isNull();
    }

    @Test
    void everyUpstreamBooleanRocksMetricOptionUsesFlinksOwnResolver() throws Exception {
        int checked = 0;
        for (var field : RocksDBNativeMetricOptions.class.getFields()) {
            if (!Modifier.isStatic(field.getModifiers()) || !ConfigOption.class.isAssignableFrom(field.getType()))
                continue;
            var option = (ConfigOption<?>) field.get(null);
            if (!(option.defaultValue() instanceof Boolean)) continue;
            var config = new Configuration();
            config.setString(option.key(), "true");
            boolean enabled = RocksDBNativeMetricOptions.fromConfig(config).isEnabled();
            String reason = NativeStateMetricSupport.unsupportedReason(config);
            if (enabled) assertThat(reason).as(option.key()).contains("enabled RocksDB native metrics");
            else assertThat(reason).as(option.key()).isNull();
            checked++;
        }
        assertThat(checked).isGreaterThan(0);
    }
}
