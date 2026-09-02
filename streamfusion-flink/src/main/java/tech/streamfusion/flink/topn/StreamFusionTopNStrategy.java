/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.topn;

/** Flink's three streaming RankProcessStrategy shapes. */
public enum StreamFusionTopNStrategy {
    APPEND_FAST,
    UPDATE_FAST,
    RETRACT
}
