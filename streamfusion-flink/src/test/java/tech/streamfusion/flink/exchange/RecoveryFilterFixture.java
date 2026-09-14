/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.exchange;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptor;
import org.apache.flink.runtime.checkpoint.RescaleMappings;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateFilteringHandler;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGateBuilder;
import org.apache.flink.streaming.runtime.io.recovery.RecordFilterContext;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;

/** Exercises Flink 2.3's production unspilling filter with independently fragmented input gates. */
final class RecoveryFilterFixture {
    private RecoveryFilterFixture() {}

    static byte[][] filter(
            byte[][] input,
            TypeSerializer<?>[] serializers,
            StreamPartitioner<?>[] partitioners,
            int subtask,
            int parallelism,
            int sourceBufferSize,
            int outputBufferSize,
            boolean expectSpill,
            Path temporaryDirectory)
            throws Exception {
        var configs = new RecordFilterContext.InputFilterConfig[input.length];
        var descriptors =
                new InflightDataRescalingDescriptor.InflightDataGateOrPartitionRescalingDescriptor[input.length];
        var gates = new InputGate[input.length];
        var output = new ByteArrayOutputStream[input.length];
        for (int gate = 0; gate < input.length; gate++) {
            configs[gate] =
                    new RecordFilterContext.InputFilterConfig(serializers[gate], partitioners[gate], parallelism);
            // One old downstream subtask is redistributed to every new subtask. Its channel
            // is ambiguous, so Flink must apply the real partitioner to each decoded record.
            descriptors[gate] = new InflightDataRescalingDescriptor.InflightDataGateOrPartitionRescalingDescriptor(
                    new int[] {0},
                    RescaleMappings.identity(1, 1),
                    Set.of(0),
                    InflightDataRescalingDescriptor.InflightDataGateOrPartitionRescalingDescriptor.MappingType
                            .RESCALING);
            gates[gate] = new SingleInputGateBuilder().setNumberOfChannels(1).build();
            output[gate] = new ByteArrayOutputStream();
        }
        var context = new RecordFilterContext(
                configs,
                new InflightDataRescalingDescriptor(descriptors),
                subtask,
                128,
                new String[] {temporaryDirectory.toString()},
                true);
        var buffers = new Buffers();
        boolean sawSpill = false;
        try (var filter = ChannelStateFilteringHandler.createFromContext(context, gates)) {
            assertThat(filter).isNotNull();
            int[] offsets = new int[input.length];
            boolean remaining;
            do {
                remaining = false;
                for (int gate = 0; gate < input.length; gate++) {
                    int length = Math.min(sourceBufferSize, input[gate].length - offsets[gate]);
                    if (length == 0) continue;
                    remaining = true;
                    Buffer source = buffers.create(length);
                    source.getMemorySegment().put(0, input[gate], offsets[gate], length);
                    source.setSize(length);
                    offsets[gate] += length;
                    for (Buffer result :
                            filter.filterAndRewrite(gate, 0, 0, source, () -> buffers.create(outputBufferSize))) {
                        try {
                            byte[] bytes = new byte[result.readableBytes()];
                            result.getNioBufferReadable().get(bytes);
                            output[gate].write(bytes);
                        } finally {
                            result.recycleBuffer();
                        }
                    }
                    if (expectSpill && !sawSpill) {
                        try (var files = java.nio.file.Files.list(temporaryDirectory)) {
                            sawSpill = files.findAny().isPresent();
                        }
                    }
                }
            } while (remaining);
        } finally {
            for (InputGate gate : gates) gate.close();
        }
        buffers.assertReleased();
        if (expectSpill)
            assertThat(sawSpill)
                    .as("Flink spanning deserializer created a spill file")
                    .isTrue();
        byte[][] result = new byte[input.length][];
        for (int gate = 0; gate < input.length; gate++) result[gate] = output[gate].toByteArray();
        return result;
    }

    static <T> byte[] encode(TypeSerializer<T> serializer, List<StreamElement> events) throws Exception {
        var output = new DataOutputSerializer(128);
        var elementBytes = new DataOutputSerializer(128);
        var elements = new StreamElementSerializer<>(serializer);
        for (StreamElement event : events) {
            elementBytes.clear();
            elements.serialize(event, elementBytes);
            output.writeInt(elementBytes.length());
            output.write(elementBytes.getSharedBuffer(), 0, elementBytes.length());
        }
        return output.getCopyOfBuffer();
    }

    static <T> List<StreamElement> decode(TypeSerializer<T> serializer, byte[] bytes) throws Exception {
        var input = new DataInputDeserializer(bytes);
        var elements = new StreamElementSerializer<>(serializer);
        var result = new ArrayList<StreamElement>();
        while (input.available() > 0) {
            int size = input.readInt();
            assertThat(size).isPositive().isLessThanOrEqualTo(input.available());
            int before = input.available();
            result.add(elements.deserialize(input));
            assertThat(before - input.available()).isEqualTo(size);
        }
        return result;
    }

    private static final class Buffers {
        private final List<int[]> releases = new ArrayList<>();

        Buffer create(int size) {
            int[] release = {0};
            releases.add(release);
            var buffer = new NetworkBuffer(MemorySegmentFactory.allocateUnpooledSegment(size), segment -> {
                release[0]++;
                segment.free();
            });
            buffer.setSize(0);
            return buffer;
        }

        void assertReleased() {
            assertThat(releases).isNotEmpty();
            for (int[] release : releases) assertThat(release[0]).isEqualTo(1);
        }
    }
}
