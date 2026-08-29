/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.exchange;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.flink.runtime.io.network.api.writer.SubtaskStateMapper;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.junit.jupiter.api.Test;

class NativeExchangePartitionerTest {
    @Test
    void remapsStableKeyGroupsAtTheCurrentParallelism() {
        NativeExchangePartitioner partitioner = new NativeExchangePartitioner(128);
        SerializationDelegate<StreamRecord<NativeExchangeFrame>> record = new SerializationDelegate<>(null);
        record.setInstance(new StreamRecord<>(new NativeExchangeFrame(96, new byte[0], new byte[0])));

        partitioner.setup(4);
        assertThat(partitioner.selectChannel(record)).isEqualTo(3);

        partitioner.setup(8);
        assertThat(partitioner.selectChannel(record)).isEqualTo(6);
        assertThat(partitioner.getDownstreamSubtaskStateMapper()).isEqualTo(SubtaskStateMapper.RANGE);
        assertThat(partitioner.isSupportsUnalignedCheckpoint()).isTrue();
    }
}
