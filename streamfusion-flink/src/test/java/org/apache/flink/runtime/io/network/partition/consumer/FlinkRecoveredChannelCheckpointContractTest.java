/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package org.apache.flink.runtime.io.network.partition.consumer;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.checkpoint.channel.RecordingChannelStateWriter;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Documents the upstream sequence that prevents admitting checkpointing during recovery. */
class FlinkRecoveredChannelCheckpointContractTest {
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void unreadRecoveredBufferCanBeCapturedAgainWhileItsChannelsBarrierIsPending(boolean barrierPending)
            throws Exception {
        var recordBytes = new DataOutputSerializer(16);
        new StreamElementSerializer<>(IntSerializer.INSTANCE).serialize(new StreamRecord<>(42), recordBytes);
        var channelBytes = new DataOutputSerializer(32);
        channelBytes.writeInt(recordBytes.length());
        channelBytes.write(recordBytes.getCopyOfBuffer());
        byte[] payload = channelBytes.getCopyOfBuffer();
        int[] releases = {0};
        Buffer recoveredRecord = new NetworkBuffer(MemorySegmentFactory.wrap(payload), segment -> {
            releases[0]++;
            segment.free();
        });
        recoveredRecord.setSize(payload.length);
        var writer = new RecordingChannelStateWriter();
        var gate = new SingleInputGateBuilder()
                .setNumberOfChannels(1)
                .setCheckpointingDuringRecoveryEnabled(true)
                .build();
        var group = new org.apache.flink.metrics.groups.UnregisteredMetricsGroup();
        var recovered = new LocalRecoveredInputChannel(
                gate,
                0,
                new org.apache.flink.runtime.io.network.partition.ResultPartitionID(),
                new org.apache.flink.runtime.io.network.partition.ResultSubpartitionIndexSet(0),
                new org.apache.flink.runtime.io.network.partition.ResultPartitionManager(),
                new org.apache.flink.runtime.io.network.TaskEventDispatcher(),
                0,
                0,
                2,
                new org.apache.flink.runtime.io.network.metrics.InputChannelMetrics(group, group));
        recovered.setChannelStateWriter(writer);
        gate.setInputChannels(recovered);
        InputChannel live = null;
        try {
            recovered.onRecoveredStateBuffer(recoveredRecord);
            recovered.finishReadRecoveredState();
            // This is Flink's actual recovery-to-live-channel handoff, with an unread record.
            live = recovered.toInputChannel();
            gate.setInputChannels(live);
            var options = CheckpointOptions.unaligned(
                    CheckpointType.CHECKPOINT, CheckpointStorageLocationReference.getDefault());
            writer.start(2, options);
            // A first barrier on another input starts checkpointing on every input channel.
            live.checkpointStarted(new CheckpointBarrier(2, 2, options));
            assertThat(writer.getAddedInput().get(live.getChannelInfo())).hasSize(1);
            if (!barrierPending) live.checkpointStopped(2);
            Buffer consumed = live.getNextBuffer().orElseThrow().buffer();
            try {
                assertThat(bytes(consumed)).containsExactly(payload);
            } finally {
                consumed.recycleBuffer();
            }
            var captured = writer.getAddedInput().get(live.getChannelInfo());
            // In Flink 2.3, LocalInputChannel.checkpointStarted captures the queued buffer,
            // and getBufferAndAvailability calls maybePersist on it again until this channel's
            // barrier arrives. The UNKNOWN sequence number cannot deduplicate these writes.
            assertThat(captured).hasSize(barrierPending ? 2 : 1);
            for (Buffer buffer : captured) assertThat(bytes(buffer)).containsExactly(payload);
        } finally {
            writer.reset();
            gate.close();
            recovered.releaseAllResources();
        }
        assertThat(releases[0]).isEqualTo(1);
    }

    private static byte[] bytes(Buffer buffer) {
        byte[] bytes = new byte[buffer.readableBytes()];
        buffer.getNioBufferReadable().get(bytes);
        return bytes;
    }
}
