/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.exchange;

import org.apache.flink.runtime.io.network.api.writer.SubtaskStateMapper;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Preconditions;

/** Routes stable native key-group frames using Flink's current parallelism. */
public final class NativeExchangePartitioner extends StreamPartitioner<NativeExchangeFrame> {
    private static final long serialVersionUID = 1L;

    private final int maxParallelism;

    public NativeExchangePartitioner(int maxParallelism) {
        Preconditions.checkArgument(maxParallelism > 0, "Maximum parallelism must be positive");
        this.maxParallelism = maxParallelism;
    }

    @Override
    public int selectChannel(SerializationDelegate<StreamRecord<NativeExchangeFrame>> record) {
        int keyGroup = record.getInstance().getValue().keyGroup();
        Preconditions.checkState(
                keyGroup < maxParallelism,
                "Native exchange key group %s exceeds maximum parallelism %s",
                keyGroup,
                maxParallelism);
        return KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(maxParallelism, numberOfChannels, keyGroup);
    }

    @Override
    public StreamPartitioner<NativeExchangeFrame> copy() {
        return this;
    }

    @Override
    public SubtaskStateMapper getDownstreamSubtaskStateMapper() {
        return SubtaskStateMapper.RANGE;
    }

    @Override
    public boolean isPointwise() {
        return false;
    }

    @Override
    public String toString() {
        return "STREAMFUSION_KEY_GROUP";
    }
}
