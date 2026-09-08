/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

/** Runs the shared window conformance matrix with unshared two-second TUMBLE slices. */
class SharedTumblingWindowMetricSurfaceTest extends SharedWindowMetricSurfaceTest {
    @Override
    protected boolean tumbling() {
        return true;
    }
}
