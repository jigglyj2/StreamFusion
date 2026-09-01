/*
 * Copyright 2026 StreamFusion Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.github.nexmark.flink.source;

import com.github.nexmark.flink.NexmarkConfiguration;
import com.github.nexmark.flink.generator.GeneratorConfig;
import java.util.LinkedHashSet;
import java.util.Set;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.FactoryUtil;

/** Factory for the finite adapter around the local Nexmark RowData source. */
public final class StreamFusionBoundedNexmarkTableSourceFactory extends NexmarkTableSourceFactory {
    private static final ConfigOption<Long> EVENTS_START_TIME = ConfigOptions.key("events.start-time")
            .longType()
            .defaultValue(1_600_000_000_000L)
            .withDescription("Epoch milliseconds used as the deterministic first event time.");

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        ReadableConfig options = helper.getOptions();
        helper.validate();

        int parallelism = context.getConfiguration().get(CoreOptions.DEFAULT_PARALLELISM);
        NexmarkConfiguration configuration = NexmarkSourceOptions.convertToNexmarkConfiguration(options);
        configuration.numEventGenerators = parallelism;
        GeneratorConfig generator = new GeneratorConfig(
                configuration,
                options.get(EVENTS_START_TIME),
                1,
                configuration.numEvents,
                configuration.stopAtEvent,
                1);
        return new StreamFusionBoundedNexmarkTableSource(generator);
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new LinkedHashSet<>(super.optionalOptions());
        options.add(EVENTS_START_TIME);
        return options;
    }

    @Override
    public String factoryIdentifier() {
        return "streamfusion-nexmark-bounded";
    }
}
