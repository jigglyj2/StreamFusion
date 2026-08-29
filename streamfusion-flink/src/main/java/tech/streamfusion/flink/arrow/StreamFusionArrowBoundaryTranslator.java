/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.arrow;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

/** Reflection entry point used by the planner for the sink-side Arrow boundary. */
public final class StreamFusionArrowBoundaryTranslator {
    private StreamFusionArrowBoundaryTranslator() {}

    public static Transformation<RowData> toRowData(Transformation<RowData> input, RowType rowType) {
        return StreamFusionArrowBoundaries.toRowData(input, rowType);
    }
}
