/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.aggregate;

/** Relative Flink OPERATOR managed-memory weights for native aggregate stages. */
final class AggregateManagedMemoryWeights {
    static final int LOCAL = 2;
    static final int STATEFUL = 8;
    static final int BATCH = 128;

    private AggregateManagedMemoryWeights() {}
}
