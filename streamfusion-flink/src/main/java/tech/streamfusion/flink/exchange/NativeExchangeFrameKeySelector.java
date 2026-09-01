/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.exchange;

import java.io.Serializable;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;

/** Supplies a synthetic Flink key whose assigned key group matches an already-routed native frame. */
public final class NativeExchangeFrameKeySelector implements KeySelector<NativeExchangeFrame, Integer>, Serializable {
    private static final long serialVersionUID = 1L;

    private final int maxParallelism;
    private final int[] keysByKeyGroup;

    public NativeExchangeFrameKeySelector(int maxParallelism) {
        if (maxParallelism <= 0) {
            throw new IllegalArgumentException("Maximum parallelism must be positive");
        }
        this.maxParallelism = maxParallelism;
        this.keysByKeyGroup = findKeys(maxParallelism);
    }

    @Override
    public Integer getKey(NativeExchangeFrame frame) {
        int keyGroup = frame.keyGroup();
        if (keyGroup < 0 || keyGroup >= maxParallelism) {
            throw new IllegalArgumentException(
                    "Native exchange key group " + keyGroup + " exceeds maximum parallelism " + maxParallelism);
        }
        return keysByKeyGroup[keyGroup];
    }

    private static int[] findKeys(int maxParallelism) {
        int[] keys = new int[maxParallelism];
        boolean[] found = new boolean[maxParallelism];
        int remaining = maxParallelism;
        for (int candidate = 0; remaining > 0; candidate++) {
            int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(candidate, maxParallelism);
            if (!found[keyGroup]) {
                found[keyGroup] = true;
                keys[keyGroup] = candidate;
                remaining--;
            }
            if (candidate == Integer.MAX_VALUE && remaining > 0) {
                throw new IllegalStateException("Could not synthesize a Flink key for every key group");
            }
        }
        return keys;
    }
}
