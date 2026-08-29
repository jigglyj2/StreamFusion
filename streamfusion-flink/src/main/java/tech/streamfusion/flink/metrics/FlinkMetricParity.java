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

import org.apache.flink.metrics.Counter;

/** Preserves Flink's logical-record metric semantics across internal batch transports. */
public final class FlinkMetricParity {
    private FlinkMetricParity() {}

    /** Replaces a framework-counted number of transport records with its logical row count. */
    public static void replacePhysicalRecords(Counter counter, long physicalRecords, long logicalRecords) {
        if (physicalRecords < 0 || logicalRecords < 0) {
            throw new IllegalArgumentException("Metric record counts must be non-negative");
        }
        counter.inc(logicalRecords - physicalRecords);
    }
}
