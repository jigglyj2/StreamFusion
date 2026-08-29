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
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.FactoryUtil;

/** Factory for the finite adapter around the local Nexmark RowData source. */
public final class StreamFusionBoundedNexmarkTableSourceFactory extends NexmarkTableSourceFactory {
    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        ReadableConfig options = helper.getOptions();
        helper.validate();

        int parallelism = context.getConfiguration().get(CoreOptions.DEFAULT_PARALLELISM);
        NexmarkConfiguration configuration = NexmarkSourceOptions.convertToNexmarkConfiguration(options);
        configuration.numEventGenerators = parallelism;
        GeneratorConfig generator = new GeneratorConfig(
                configuration, System.currentTimeMillis(), 1, configuration.numEvents, configuration.stopAtEvent, 1);
        return new StreamFusionBoundedNexmarkTableSource(generator);
    }

    @Override
    public String factoryIdentifier() {
        return "streamfusion-nexmark-bounded";
    }
}
