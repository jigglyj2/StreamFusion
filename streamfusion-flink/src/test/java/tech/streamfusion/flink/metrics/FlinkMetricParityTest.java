/*
 * Copyright 2026 StreamFusion Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package tech.streamfusion.flink.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.flink.metrics.SimpleCounter;
import org.junit.jupiter.api.Test;

class FlinkMetricParityTest {
    @Test
    void replacesTransportFrameCountsWithLogicalRowCounts() {
        SimpleCounter counter = new SimpleCounter();
        counter.inc(2);

        FlinkMetricParity.replacePhysicalRecords(counter, 2, 11);

        assertThat(counter.getCount()).isEqualTo(11);
    }
}
