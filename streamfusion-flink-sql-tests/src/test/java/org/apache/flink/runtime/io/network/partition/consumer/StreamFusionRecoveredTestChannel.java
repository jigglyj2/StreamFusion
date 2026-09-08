/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package org.apache.flink.runtime.io.network.partition.consumer;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.core.memory.MemorySegmentProvider;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.io.network.buffer.Buffer;

/** Bridges Flink's mailbox harness test queues to its real channel-state reader. */
public final class StreamFusionRecoveredTestChannel extends RecoveredInputChannel {
    private final InputChannel live;

    private StreamFusionRecoveredTestChannel(SingleInputGate gate, int index, InputChannel live) {
        super(
                gate,
                index,
                live.getPartitionId(),
                live.getConsumedSubpartitionIndexSet(),
                0,
                0,
                new SimpleCounter(),
                new SimpleCounter(),
                10);
        this.live = live;
    }

    public static void install(SingleInputGate gate) {
        var recovered = new InputChannel[gate.getNumberOfInputChannels()];
        for (int index = 0; index < recovered.length; index++) {
            var live = gate.getChannel(index);
            try {
                // Prime the reusable mailbox provider before converting this channel.
                if (live.getNextBuffer().isPresent()) throw new AssertionError("Live test channel must be empty");
            } catch (Exception failure) {
                throw new RuntimeException(failure);
            }
            recovered[index] = new StreamFusionRecoveredTestChannel(gate, index, live);
        }
        gate.setInputChannels(recovered);
    }

    public static MemorySegmentProvider memorySegments(int size) {
        return new MemorySegmentProvider() {
            @Override
            public Collection<MemorySegment> requestUnpooledMemorySegments(int count) {
                return IntStream.range(0, count)
                        .mapToObj(i -> MemorySegmentFactory.allocateUnpooledSegment(size))
                        .collect(Collectors.toList());
            }

            @Override
            public void recycleUnpooledMemorySegments(Collection<MemorySegment> segments) {
                segments.forEach(MemorySegment::free);
            }
        };
    }

    @Override
    protected InputChannel toInputChannelInternal(ArrayDeque<Buffer> remainingBuffers) {
        if (!remainingBuffers.isEmpty()) throw new AssertionError("Recovery must consume every saved buffer first");
        return live;
    }
}
