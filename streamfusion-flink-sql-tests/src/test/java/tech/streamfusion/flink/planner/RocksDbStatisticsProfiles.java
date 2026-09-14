/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import java.lang.reflect.Modifier;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.state.rocksdb.RocksDBNativeMetricOptions;

/** Selects the complete upstream ticker surface without enabling column-family properties. */
public final class RocksDbStatisticsProfiles {
    private RocksDbStatisticsProfiles() {}

    public static Configuration allTickers() {
        var result = new Configuration();
        try {
            for (var field : RocksDBNativeMetricOptions.class.getFields()) {
                if (!Modifier.isStatic(field.getModifiers()) || !ConfigOption.class.isAssignableFrom(field.getType()))
                    continue;
                var option = (ConfigOption<?>) field.get(null);
                if (!(option.defaultValue() instanceof Boolean)) continue;
                var single = new Configuration();
                single.setString(option.key(), "true");
                if (!RocksDBNativeMetricOptions.fromConfig(single)
                        .getMonitorTickerTypes()
                        .isEmpty()) {
                    result.setString(option.key(), "true");
                }
            }
        } catch (ReflectiveOperationException failure) {
            throw new AssertionError(failure);
        }
        return result;
    }
}
