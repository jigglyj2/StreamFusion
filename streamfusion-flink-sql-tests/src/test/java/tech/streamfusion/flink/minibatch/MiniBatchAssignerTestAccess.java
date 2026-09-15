/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.minibatch;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;

/** Package access to the real Arrow control operators for SQL-region conformance tests. */
public final class MiniBatchAssignerTestAccess {
    private MiniBatchAssignerTestAccess() {}

    public static AbstractStreamOperator<ArrowRowDataBatch> create(boolean processingTime, long interval) {
        return processingTime
                ? new StreamFusionArrowProcTimeMiniBatchAssignerOperator(interval)
                : new StreamFusionArrowRowTimeMiniBatchAssignerOperator(interval);
    }
}
