/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.spec.JoinSpec;
import org.apache.flink.table.types.logical.RowType;

/** Distinct StreamFusion physical node replacing Flink's bounded sort-merge join. */
public final class StreamFusionBatchExecSortMergeJoin extends StreamFusionBatchExecHashJoin {
    public StreamFusionBatchExecSortMergeJoin(
            ReadableConfig config,
            JoinSpec joinSpec,
            InputProperty leftInput,
            InputProperty rightInput,
            RowType outputType,
            String description) {
        super(
                config,
                joinSpec,
                leftInput,
                rightInput,
                outputType,
                description,
                "streamfusion-batch-exec-sort-merge-join_1");
    }
}
