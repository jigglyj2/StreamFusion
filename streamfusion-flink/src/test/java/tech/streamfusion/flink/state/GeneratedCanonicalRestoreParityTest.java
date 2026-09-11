/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.state;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.operators.deduplicate.ProcTimeDeduplicateKeepFirstRowFunction;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import tech.streamfusion.flink.TestingNativeMemoryManager;
import tech.streamfusion.flink.arrow.ArrowNativePlanBridge;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.nativebridge.NativeExecutionContext;
import tech.streamfusion.nativebridge.NativeMemoryManager;
import tech.streamfusion.nativebridge.NativeStateResources;
import tech.streamfusion.proto.plan.v1.Deduplicate;
import tech.streamfusion.proto.plan.v1.Input;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;

/** Generated Flink changelog parity through the production, bounded canonical restore JNI edge. */
class GeneratedCanonicalRestoreParityTest {
    private static final RowType TYPE = RowType.of(new VarCharType());

    @Test
    void largeKeyGroupRestoresUnderPressureAndPreservesFlinkChangelog(@TempDir Path directory) throws Exception {
        for (boolean rocks : List.of(false, true)) {
            var serializer = new RowDataSerializer(TYPE);
            try (var oracle = new KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData>(
                            new KeyedProcessOperator<>(new ProcTimeDeduplicateKeepFirstRowFunction(0)),
                            row -> serializer.toBinaryRow(GenericRowData.of(row.getString(0))),
                            InternalTypeInfo.of(TYPE),
                            1,
                            1,
                            0);
                    var source = new Region(false, directory.resolve("source-" + rocks));
                    var target = new Region(rocks, directory.resolve("target-" + rocks))) {
                oracle.setup(serializer);
                oracle.open();
                var random = new Random(rocks ? 103 : 59);
                var keys = new ArrayList<String>();
                for (int index = 0; index < 3201; index++) {
                    keys.add(index + ":" + random.nextLong() + "é".repeat(2048));
                }
                compare(source, oracle, keys);
                var bytes = new ByteArrayOutputStream();
                long written = source.context.state().writeSnapshot(2, 0, new DataOutputStream(bytes));
                assertThat(written).isGreaterThan(12L << 20);
                long pressure = target.memory.available() - ((rocks ? 4L : 24L) << 20);
                assertThat(target.memory.tryReserve(pressure)).isTrue();
                try {
                    var input = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()));
                    assertThat(target.context.state().restoreFrame(2, 0, input)).isEqualTo(written);
                    assertThat(input.read()).isEqualTo(-1);
                } finally {
                    target.memory.release(pressure);
                }
                // The Flink oracle retains the same pre-checkpoint state. Replayed keys must
                // disappear and new keys must retain their exact serialized changelog bytes.
                var replay = new ArrayList<>(keys.subList(0, 96));
                for (int index = 0; index < 50; index++) replay.add("new-" + random.nextLong());
                java.util.Collections.shuffle(replay, random);
                compare(target, oracle, replay);
                try (var paths = Files.walk(target.directory)) {
                    assertThat(paths.filter(
                                    path -> path.getFileName().toString().contains("spill")))
                            .isEmpty();
                }
            }
        }
    }

    private static void compare(
            Region region, KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> oracle, List<String> keys)
            throws Exception {
        var serializer = new RowDataSerializer(TYPE);
        for (int start = 0; start < keys.size(); start += 32) {
            var rows = keys.subList(start, Math.min(start + 32, keys.size())).stream()
                    .map(key -> GenericRowData.of(StringData.fromString(key)))
                    .collect(java.util.stream.Collectors.toList());
            var expected = new DataOutputSerializer(128);
            for (var row : rows) oracle.processElement(new StreamRecord<>(serializer.copy(row)));
            var records = oracle.extractOutputStreamRecords();
            region.inputs += rows.size();
            region.outputs += records.size();
            for (var record : records) serializer.serialize(record.getValue(), expected);
            oracle.getOutput().clear();
            var actual = new DataOutputSerializer(128);
            try (var input = ArrowRowDataBatch.transpose(rows, TYPE, region.allocator);
                    var stream = region.edge.executeStream(List.of(input))) {
                ArrowRowDataBatch next;
                while ((next = stream.next()) != null) {
                    try (var batch = next) {
                        for (int row = 0; row < batch.size(); row++) serializer.serialize(batch.rowView(row), actual);
                    }
                }
            }
            assertThat(actual.getCopyOfBuffer()).isEqualTo(expected.getCopyOfBuffer());
            assertThat(region.context.metricSnapshot())
                    .containsExactly(2L, region.inputs, region.outputs, 1L, 0L, region.inputs);
        }
    }

    private static final class Region implements AutoCloseable {
        final NativeMemoryManager memory = TestingNativeMemoryManager.create(128L << 20);
        final RootAllocator allocator = new RootAllocator(64L << 20);
        final NativeExecutionContext context;
        final ArrowNativePlanBridge edge;
        final Path directory;
        long inputs;
        long outputs;

        Region(boolean rocks, Path directory) throws Exception {
            this.directory = directory;
            Files.createDirectories(directory);
            var input = Operator.newBuilder()
                    .setPlanNodeId(1)
                    .setInput(Input.newBuilder())
                    .build();
            var node = Operator.newBuilder()
                    .setPlanNodeId(2)
                    .setDeduplicate(Deduplicate.newBuilder()
                            .setInput(input)
                            .addKeyIndices(0)
                            .setProcessingTime(true)
                            .setGenerateInsert(true))
                    .build();
            var binding = rocks
                    ? NativeStateResources.rocksDb(2, 1, 0, 0, directory.resolve("state"), 8L << 20)
                    : NativeStateResources.memory(2, 1, 0, 0);
            context = new NativeExecutionContext(
                    NativePlan.newBuilder()
                            .setProtocolVersion(2)
                            .setRoot(node)
                            .build()
                            .toByteArray(),
                    memory,
                    NativeStateResources.serialize(List.of(binding), List.of(directory)));
            edge = new ArrowNativePlanBridge(context, TYPE, allocator);
        }

        @Override
        public void close() {
            context.close();
            assertThat(memory.available()).isEqualTo(memory.limit());
            assertThat(allocator.getAllocatedMemory()).isZero();
            allocator.close();
        }
    }
}
