/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import org.junit.jupiter.api.Test;

class SharedSplitIndexMetricSurfaceTest {
    @Test
    void generatedChangelogControlsAndRegisteredStageMetricsMatchFlink() throws Exception {
        SharedStringScalarMetricAssertions.assertParity(
                new SharedSplitIndexMetricFixture(),
                row -> row % 11 == 0 ? "missing" : "prefix/" + (row % 5 == 0 ? "" : "漢😀é" + row + "::tail") + "/end");
    }
}
