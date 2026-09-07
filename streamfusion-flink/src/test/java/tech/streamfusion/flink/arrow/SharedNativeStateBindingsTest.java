/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.arrow;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.api.java.functions.KeySelector;
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
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import tech.streamfusion.flink.TestingNativeMemoryManager;
import tech.streamfusion.nativebridge.NativeExecutionContext;
import tech.streamfusion.nativebridge.NativeStateResources;
import tech.streamfusion.proto.plan.v1.Deduplicate;
import tech.streamfusion.proto.plan.v1.Input;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.NativeStateBinding;
import tech.streamfusion.proto.plan.v1.Operator;

class SharedNativeStateBindingsTest {
    private static final RowType TYPE = RowType.of(new BigIntType(false), new BigIntType(false), new VarCharType());
    private static final int GROUPS = 16;

    @Test
    void twoStateOwnersUseOneArrowEdgeAndRestoreAcrossBothBackends(@TempDir Path directory) throws Exception {
        for (boolean rocksFirst : new boolean[] {false, true}) {
            for (int seed = 0; seed < 3; seed++) {
                var memory = TestingNativeMemoryManager.create();
                Path run = directory.resolve(rocksFirst + "-" + seed);
                var expected = new DataOutputSerializer(128);
                var actual = new DataOutputSerializer(128);
                var serializer = new RowDataSerializer(TYPE);
                try (var allocator = new RootAllocator(64L << 20);
                        var first = flink(0);
                        var second = flink(1)) {
                    var random = new Random(seed);
                    byte[][][] snapshots = new byte[2][GROUPS][];
                    for (int phase = 0; phase < 2; phase++) {
                        try (var context = new NativeExecutionContext(
                                plan(),
                                memory,
                                bindings(phase == 0 ? rocksFirst : !rocksFirst, run.resolve("phase-" + phase)))) {
                            if (phase == 1) {
                                for (int node = 0; node < 2; node++)
                                    for (int group = 0; group < GROUPS; group++) {
                                        context.state().restore(node + 2, group, snapshots[node][group]);
                                    }
                            }
                            var edge = new ArrowNativePlanBridge(context, TYPE, allocator);
                            long firstOutput = 0;
                            long secondOutput = 0;
                            for (int arrival = 0; arrival < 3; arrival++) {
                                List<GenericRowData> rows = new ArrayList<>();
                                for (int index = 0; index < 12; index++) {
                                    var row = GenericRowData.of(
                                            (long) random.nextInt(17),
                                            (long) random.nextInt(9),
                                            index % 3 == 0 ? null : StringData.fromString("é-" + phase + "-" + index));
                                    rows.add(row);
                                    first.processElement(new StreamRecord<>(serializer.copy(row)));
                                }
                                firstOutput +=
                                        first.extractOutputStreamRecords().size();
                                for (var record : first.extractOutputStreamRecords())
                                    second.processElement(new StreamRecord<>(record.getValue()));
                                first.getOutput().clear();
                                secondOutput +=
                                        second.extractOutputStreamRecords().size();
                                for (var record : second.extractOutputStreamRecords())
                                    serializer.serialize(record.getValue(), expected);
                                second.getOutput().clear();
                                try (var input = ArrowRowDataBatch.transpose(rows, TYPE, allocator);
                                        var stream = edge.executeStream(List.of(input))) {
                                    assertThatThrownBy(() -> context.state().snapshot(2, 0))
                                            .hasMessageContaining("active");
                                    ArrowRowDataBatch batch;
                                    while ((batch = stream.next()) != null) {
                                        try (var output = batch) {
                                            for (int row = 0; row < output.size(); row++) {
                                                var value = output.rowView(row);
                                                value.setRowKind(output.rowKind(row));
                                                serializer.serialize(value, actual);
                                            }
                                        }
                                    }
                                }
                                assertThat(actual.getCopyOfBuffer()).isEqualTo(expected.getCopyOfBuffer());
                            }
                            assertThat(context.metricSnapshot())
                                    .containsExactly(3, firstOutput, secondOutput, 2, 36, firstOutput, 1, 0, 36);
                            for (int node = 0; node < 2; node++)
                                for (int group = 0; group < GROUPS; group++) {
                                    snapshots[node][group] = context.state().snapshot(node + 2, group);
                                }
                        }
                        assertThat(memory.available()).isEqualTo(memory.limit());
                        assertThat(allocator.getAllocatedMemory()).isZero();
                    }
                }
            }
        }
    }

    @Test
    void invalidBindingsFailBeforeExecutionWithoutLeakingReservations() {
        var memory = TestingNativeMemoryManager.create();
        var binding = NativeStateResources.memory(2, GROUPS, 0, GROUPS - 1);
        for (List<NativeStateBinding> invalid : List.<List<NativeStateBinding>>of(
                List.of(),
                List.of(binding),
                List.of(binding, binding),
                List.of(NativeStateResources.memory(999, GROUPS, 0, GROUPS - 1)),
                List.of(NativeStateResources.memory(1, GROUPS, 0, GROUPS - 1)))) {
            assertThatThrownBy(
                            () -> new NativeExecutionContext(plan(), memory, NativeStateResources.serialize(invalid)))
                    .isInstanceOf(IllegalStateException.class);
            assertThat(memory.available()).isEqualTo(memory.limit());
        }
    }

    @Test
    void rejectedInputReleasesAlreadyAttachedMetadataWithoutPoisoningState() {
        var memory = TestingNativeMemoryManager.create();
        var reservedType = RowType.of(
                TYPE.getChildren().toArray(org.apache.flink.table.types.logical.LogicalType[]::new),
                new String[] {"key", "other", "__streamfusion_row_kind"});
        try (var allocator = new RootAllocator(64L << 20)) {
            try (var context = new NativeExecutionContext(plan(), memory, bindings(false, Path.of("unused")));
                    var input = ArrowRowDataBatch.transpose(List.of(GenericRowData.of(1L, 2L, null)), TYPE, allocator);
                    var invalid = ArrowRowDataBatch.transpose(
                            List.of(GenericRowData.of(1L, 2L, null)), reservedType, allocator)) {
                long inputBytes = allocator.getAllocatedMemory();
                var edge = new ArrowNativePlanBridge(context, TYPE, allocator);
                // The first port has already acquired an envelope when the second is rejected.
                assertThatThrownBy(() -> edge.executeStream(List.of(input, invalid)))
                        .hasMessageContaining("reserved metadata field");
                assertThat(allocator.getAllocatedMemory()).isEqualTo(inputBytes);
                assertThat(context.state().snapshot(2, 0)).isNotEmpty();
                try (var stream = edge.executeStream(List.of(input))) {
                    try (var output = stream.next()) {
                        assertThat(output.size()).isEqualTo(1);
                    }
                    assertThat(stream.next()).isNull();
                }
            }
            assertThat(allocator.getAllocatedMemory()).isZero();
        }
        assertThat(memory.available()).isEqualTo(memory.limit());
    }

    @Test
    void failedRestoreRequiresFreshContextAndReleasesReservations() {
        var memory = TestingNativeMemoryManager.create();
        try (var context = new NativeExecutionContext(plan(), memory, bindings(false, Path.of("unused")))) {
            assertThatThrownBy(() -> context.state().restore(2, 0, new byte[] {1, 2, 3}))
                    .isInstanceOf(IllegalStateException.class);
            // Even another state owner must not be used after a potentially partial restore.
            assertThatThrownBy(() -> context.state().snapshot(3, 0)).hasMessageContaining("failed");
        }
        assertThat(memory.available()).isEqualTo(memory.limit());
    }

    @Test
    void cancelledStatefulStreamRequiresRecoveryAndReleasesInputEnvelopeAllocations() {
        var memory = TestingNativeMemoryManager.create();
        try (var allocator = new RootAllocator(64L << 20)) {
            try (var context = new NativeExecutionContext(plan(), memory, bindings(false, Path.of("unused")));
                    var input =
                            ArrowRowDataBatch.transpose(List.of(GenericRowData.of(1L, 2L, null)), TYPE, allocator)) {
                long inputBytes = allocator.getAllocatedMemory();
                var edge = new ArrowNativePlanBridge(context, TYPE, allocator);
                try (var stream = edge.executeStream(List.of(input))) {
                    assertThatThrownBy(() -> context.state().restore(2, 0, new byte[0]))
                            .hasMessageContaining("active");
                    try (var batch = stream.next()) {
                        assertThat(batch.size()).isEqualTo(1);
                    }
                    // Deliberately do not poll EOF: state has changed, so cancellation must poison it.
                }
                assertThatThrownBy(() -> context.state().snapshot(2, 0)).hasMessageContaining("failed");
                assertThat(allocator.getAllocatedMemory()).isEqualTo(inputBytes);
            }
            assertThat(allocator.getAllocatedMemory()).isZero();
        }
        assertThat(memory.available()).isEqualTo(memory.limit());
    }

    private static byte[] bindings(boolean rocks, Path directory) {
        List<NativeStateBinding> bindings = new ArrayList<>();
        for (int id = 2; id <= 3; id++)
            bindings.add(
                    rocks
                            ? NativeStateResources.rocksDb(
                                    id, GROUPS, 0, GROUPS - 1, directory.resolve("node-" + id), 8L << 20)
                            : NativeStateResources.memory(id, GROUPS, 0, GROUPS - 1));
        return NativeStateResources.serialize(bindings);
    }

    private static byte[] plan() {
        Operator node = Operator.newBuilder()
                .setPlanNodeId(1)
                .setInput(Input.newBuilder())
                .build();
        for (int key = 0; key < 2; key++)
            node = Operator.newBuilder()
                    .setPlanNodeId(key + 2)
                    .setDeduplicate(Deduplicate.newBuilder()
                            .setInput(node)
                            .addKeyIndices(key)
                            .setProcessingTime(true)
                            .setGenerateInsert(true))
                    .build();
        return NativePlan.newBuilder()
                .setProtocolVersion(2)
                .setRoot(node)
                .build()
                .toByteArray();
    }

    private static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> flink(int key) throws Exception {
        var keyType = RowType.of(new BigIntType(false));
        var keySerializer = new RowDataSerializer(keyType);
        KeySelector<RowData, RowData> selector = row ->
                keySerializer.toBinaryRow(GenericRowData.of(row.getLong(key))).copy();
        var harness = new KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData>(
                new KeyedProcessOperator<>(new ProcTimeDeduplicateKeepFirstRowFunction(0L)),
                selector,
                InternalTypeInfo.of(keyType),
                GROUPS,
                1,
                0);
        harness.setup(new RowDataSerializer(TYPE));
        harness.open();
        return harness;
    }
}
