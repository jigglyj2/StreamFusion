/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.state;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend;
import org.apache.flink.state.rocksdb.RocksDBNativeMetricOptions;
import org.apache.flink.util.InstantiationUtil;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.metrics.NativeRocksDbStatisticsConfiguration;

class NativeRocksDbStatisticsConfigurationTest {
    @Test
    void allUpstreamTickerOptionsMapToStableAbiCodesAndSurviveBackendSerialization() throws Exception {
        var names = Files.readAllLines(
                Path.of("../streamfusion-state-rocksdb/tests/fixtures/flink-rocksdb-ticker-names.txt"));
        var all = new Configuration();
        var checked = new ArrayList<Integer>();
        for (var field : RocksDBNativeMetricOptions.class.getFields()) {
            if (!Modifier.isStatic(field.getModifiers()) || !ConfigOption.class.isAssignableFrom(field.getType()))
                continue;
            var option = (ConfigOption<?>) field.get(null);
            if (!(option.defaultValue() instanceof Boolean)) continue;
            var config = new Configuration();
            config.setString(option.key(), "true");
            var actual = RocksDBNativeMetricOptions.fromConfig(config).getMonitorTickerTypes();
            if (actual.isEmpty()) continue;
            assertThat(actual).hasSize(1);
            int code =
                    names.indexOf("rocksdb." + actual.iterator().next().name().toLowerCase(java.util.Locale.ROOT));
            assertThat(code).isNotNegative();
            assertThat(NativeRocksDbStatisticsConfiguration.fromConfig(config)).containsExactly(code);
            checked.add(code);
            all.setString(option.key(), "true");
        }
        assertThat(checked).containsExactlyInAnyOrder(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        var selected = NativeRocksDbStatisticsConfiguration.fromConfig(all);
        var backend =
                new EmbeddedRocksDBStateBackend().configure(all, getClass().getClassLoader());
        var wrapper = InstantiationUtil.clone(new StreamFusionStateBackend(backend, all));
        var field = StreamFusionStateBackend.class.getDeclaredField("nativeRocksDbStatistics");
        field.setAccessible(true);
        assertThat(field.get(wrapper)).isEqualTo(selected);
        assertThat(selected).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        assertThat(NativeRocksDbStatisticsConfiguration.fromConfig(new Configuration()))
                .isEqualTo(List.of());
    }
}
