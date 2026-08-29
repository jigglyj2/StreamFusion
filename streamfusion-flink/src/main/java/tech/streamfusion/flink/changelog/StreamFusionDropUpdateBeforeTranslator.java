/*
 * Copyright 2026 StreamFusion Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package tech.streamfusion.flink.changelog;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.data.RowData;

/** Runtime translation entry point for StreamFusion's changelog metadata filter. */
public final class StreamFusionDropUpdateBeforeTranslator {
    private StreamFusionDropUpdateBeforeTranslator() {}

    public static Transformation<RowData> translate(Transformation<RowData> input) {
        return new OneInputTransformation<>(
                input,
                "streamfusion-drop-update-before",
                new StreamFusionDropUpdateBeforeOperator(),
                input.getOutputType(),
                input.getParallelism(),
                false);
    }
}
