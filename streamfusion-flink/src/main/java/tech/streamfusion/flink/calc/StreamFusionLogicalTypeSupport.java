/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.calc;

import org.apache.flink.table.types.logical.LogicalType;

final class StreamFusionLogicalTypeSupport {
    private StreamFusionLogicalTypeSupport() {}

    static boolean sameTypeIgnoringNullability(LogicalType left, LogicalType right) {
        if (left == null || right == null) {
            return false;
        }
        return left.copy(true).asSerializableString().equals(right.copy(true).asSerializableString());
    }
}
