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

import com.github.nexmark.flink.generator.GeneratorConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.data.RowData;

/** Table source for the finite, bounded adapter around Nexmark's RowData generator. */
final class StreamFusionBoundedNexmarkTableSource implements ScanTableSource {
    private final GeneratorConfig config;

    StreamFusionBoundedNexmarkTableSource(GeneratorConfig config) {
        this.config = config;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext context) {
        TypeInformation<RowData> outputType =
                context.createTypeInformation(NexmarkTableSource.RESOLVED_SCHEMA.toPhysicalRowDataType());
        return SourceProvider.of(new StreamFusionBoundedNexmarkSource(config, outputType));
    }

    @Override
    public StreamFusionBoundedNexmarkTableSource copy() {
        return new StreamFusionBoundedNexmarkTableSource(config);
    }

    @Override
    public String asSummaryString() {
        return "Bounded Nexmark RowData Source";
    }
}
