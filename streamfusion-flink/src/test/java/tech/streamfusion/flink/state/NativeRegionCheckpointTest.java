/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.state;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupStatePartitionStreamProvider;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContextSynchronousImpl;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;
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
import tech.streamfusion.flink.arrow.ArrowNativePlanBridge;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.nativebridge.NativeExecutionContext;
import tech.streamfusion.nativebridge.NativeMemoryManager;
import tech.streamfusion.nativebridge.NativeStateResources;
import tech.streamfusion.proto.plan.v1.Deduplicate;
import tech.streamfusion.proto.plan.v1.Input;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;

class NativeRegionCheckpointTest {
    private static final RowType TYPE = RowType.of(new BigIntType(false), new VarCharType());
    private static final int GROUPS = 16;
    private static final KeyGroupRange ALL = new KeyGroupRange(0, GROUPS - 1);
    private static final List<Long> IDS = List.of(2L, 3L);

    @Test
    void malformedRawFramesCannotAllocateUnboundedBuffersOrLeakRestoreReservations(@TempDir Path directory)
            throws Exception {
        try (var region = new Region(false, ALL, directory)) {
            var oversized = new DataOutputSerializer(32);
            oversized.writeInt(0x53465231);
            oversized.writeInt(IDS.size());
            for (long id : IDS) oversized.writeLong(id);
            oversized.writeInt(Integer.MAX_VALUE);
            var truncated = new DataOutputSerializer(32);
            truncated.write(oversized.getCopyOfBuffer(), 0, 24);
            truncated.writeInt(4);
            truncated.writeByte(1);
            for (byte[] bytes : List.of(new byte[8], oversized.getCopyOfBuffer(), truncated.getCopyOfBuffer())) {
                long available = region.memory.available();
                var provider = new KeyGroupStatePartitionStreamProvider(new java.io.ByteArrayInputStream(bytes), 0);
                var initialization =
                        supplied(StateInitializationContext.class, "getRawKeyedStateInputs", List.of(provider));
                assertThatThrownBy(() -> region.participant.restoreRawState(initialization))
                        .isInstanceOf(java.io.IOException.class);
                assertThat(region.memory.available()).isEqualTo(available);
            }
        }
    }

    @Test
    void checkpointImportChecksAssignmentAndAdmitsReaderBeforeOpeningDatabase(@TempDir Path directory)
            throws Exception {
        try (var region = new Region(false, new KeyGroupRange(8, 15), directory)) {
            Path missing = directory.resolve("missing-checkpoint");
            assertThatThrownBy(() -> region.context.state().importCheckpoint(2, missing, 0, 7, 256L * 1024))
                    .hasMessageContaining("range");
            // Range validation is non-mutating; a denied admitted restore poisons the context.
            assertThat(region.context.state().snapshot(2, 8)).isNotEmpty();
            assertThatThrownBy(
                            () -> region.context.state().importCheckpoint(2, missing, 8, 15, 2 * region.memory.limit()))
                    .hasMessageContaining("Flink denied");
            assertThat(Files.exists(missing)).isFalse();
            assertThatThrownBy(() -> region.context.state().snapshot(3, 8)).hasMessageContaining("failed");
        }
        try (var region = new Region(false, ALL, directory.resolve("incomplete"))) {
            Path missing = directory.resolve("missing-checkpoint");
            assertThatThrownBy(() -> region.context.state().importCheckpoint(2, missing, 0, 15, 256L * 1024))
                    .hasMessageContaining("CURRENT");
            assertThat(Files.exists(missing)).isFalse();
            assertThatThrownBy(() -> region.context.state().snapshot(3, 0)).hasMessageContaining("failed");
        }
    }

    @Test
    void canonicalRawStateRescalesBothOwnersAndRestoresAcrossBackends(@TempDir Path directory) throws Exception {
        for (boolean sourceRocks : List.of(false, true)) {
            for (int seed = 0; seed < 3; seed++) {
                Path run = Files.createDirectory(directory.resolve(sourceRocks + "-" + seed));
                KeyGroupsStateHandle handle;
                byte[][][] expectedState = new byte[2][GROUPS][];
                try (var oracle = flink();
                        var source = new Region(sourceRocks, ALL, run.resolve("source"))) {
                    compare(source, oracle, rows(seed));
                    for (int node = 0; node < IDS.size(); node++)
                        for (int group : ALL)
                            expectedState[node][group] = source.context.state().snapshot(IDS.get(node), group);
                    try (var registry = new CloseableRegistry()) {
                        var snapshot = new StateSnapshotContextSynchronousImpl(
                                1, 1, new MemCheckpointStreamFactory(64 << 20), ALL, registry);
                        long bytes = source.participant.writeRawSnapshot(snapshot);
                        var future = snapshot.getKeyedStateStreamFuture();
                        future.run();
                        handle = (KeyGroupsStateHandle) future.get().getJobManagerOwnedSnapshot();
                        assertThat(handle.getStateSize()).isEqualTo(bytes);
                    }
                    // Flink intersects its keyed handle for each new subtask. Both namespaces
                    // must follow that same assignment, independently of the destination backend.
                    for (var range : List.of(new KeyGroupRange(0, 7), new KeyGroupRange(8, 15))) {
                        try (var target =
                                new Region(!sourceRocks, range, run.resolve("split-" + range.getStartKeyGroup()))) {
                            restoreRaw(target, handle.getIntersection(range));
                            for (int node = 0; node < IDS.size(); node++)
                                for (int group : range)
                                    assertThat(target.context.state().snapshot(IDS.get(node), group))
                                            .isEqualTo(expectedState[node][group]);
                        }
                    }
                    try (var target = new Region(!sourceRocks, ALL, run.resolve("target"))) {
                        restoreRaw(target, handle);
                        compare(target, oracle, rows(seed + 10));
                    }
                }
                handle.discardState();
            }
        }
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
    void flinkFileHandlesKeepNamespacesHonorReuseAndRestoreForBothAlignmentModes(
            boolean incremental, @TempDir Path directory) throws Exception {
        for (boolean unaligned : List.of(false, true)) {
            Path run = Files.createDirectory(directory.resolve("unaligned-" + unaligned));
            try (var oracle = flink();
                    var source = new Region(true, ALL, run.resolve("source"))) {
                compare(source, oracle, rows(1));
                var backend = backend(ALL, List.of(), incremental);
                backend.registerNativeStateParticipant(source.participant, true);
                var location = CheckpointStorageLocationReference.getDefault();
                var options = unaligned
                        ? CheckpointOptions.unaligned(CheckpointType.CHECKPOINT, location)
                        : CheckpointOptions.alignedNoTimeout(CheckpointType.CHECKPOINT, location);
                var first = checkpoint(backend, 1, options);
                backend.notifyCheckpointComplete(1);
                var second = checkpoint(backend, 2, options);
                assertThat(backend.usesNativeFileCheckpoints()).isTrue();
                assertThat(backend.usesNativeIncrementalCheckpoints()).isEqualTo(incremental);
                var files = incremental ? first.getSharedState() : first.getPrivateState();
                assertThat(files).isNotEmpty();
                for (long id : IDS)
                    assertThat(files).anyMatch(file -> file.getLocalPath().startsWith("node-" + id + "/"));
                if (incremental) {
                    assertThat(second.getSharedState()).hasSameSizeAs(first.getSharedState());
                    assertThat(second.getCheckpointedSize()).isLessThan(first.getCheckpointedSize());
                    for (int index = 0; index < first.getSharedState().size(); index++)
                        assertThat(second.getSharedState().get(index).getHandle())
                                .isSameAs(first.getSharedState().get(index).getHandle());
                } else {
                    assertThat(first.getSharedState()).isEmpty();
                    assertThat(second.getSharedState()).isEmpty();
                    assertThat(second.getPrivateState()).hasSameSizeAs(first.getPrivateState());
                    assertThat(second.getCheckpointedSize()).isEqualTo(first.getCheckpointedSize());
                    for (int index = 0; index < first.getPrivateState().size(); index++)
                        assertThat(second.getPrivateState().get(index).getHandle())
                                .isNotSameAs(first.getPrivateState().get(index).getHandle());
                }
                var durable = metadataRoundTrip(second);
                assertThat(durable.getStateHandleId()).isEqualTo(second.getStateHandleId());
                assertThat(durable.getBackendIdentifier()).isEqualTo(second.getBackendIdentifier());
                assertThat(durable.getCheckpointedSize()).isEqualTo(second.getCheckpointedSize());
                assertThat(durable.getSharedState()).hasSameSizeAs(second.getSharedState());
                assertThat(durable.getPrivateState()).hasSameSizeAs(second.getPrivateState());
                try (var target = new Region(true, ALL, run.resolve("target"))) {
                    backend(ALL, List.of(durable), !incremental)
                            .registerNativeStateParticipant(target.participant, true);
                    compare(target, oracle, rows(11));
                }
                for (var range : List.of(new KeyGroupRange(0, 7), new KeyGroupRange(8, 15))) {
                    try (var target = new Region(true, range, run.resolve("split-" + range.getStartKeyGroup()))) {
                        backend(
                                        range,
                                        List.of((IncrementalRemoteKeyedStateHandle) durable.getIntersection(range)),
                                        !incremental)
                                .registerNativeStateParticipant(target.participant, true);
                        for (long id : IDS)
                            for (int group : range)
                                assertThat(target.context.state().snapshot(id, group))
                                        .isEqualTo(source.context.state().snapshot(id, group));
                    }
                }
                // Shared handles are intentionally reused; discard only after every restore.
                second.discardState();
                first.discardState();
            }
        }
    }

    private static IncrementalRemoteKeyedStateHandle metadataRoundTrip(IncrementalRemoteKeyedStateHandle handle)
            throws Exception {
        var bytes = new java.io.ByteArrayOutputStream();
        try (var output = new java.io.DataOutputStream(bytes)) {
            org.apache.flink.runtime.checkpoint.metadata.MetadataV3Serializer.INSTANCE.serializeKeyedStateHandleUtil(
                    handle, output);
        }
        try (var input = new java.io.DataInputStream(new java.io.ByteArrayInputStream(bytes.toByteArray()))) {
            return (IncrementalRemoteKeyedStateHandle)
                    org.apache.flink.runtime.checkpoint.metadata.MetadataV3Serializer.INSTANCE
                            .deserializeKeyedStateHandleUtil(input);
        }
    }

    private static IncrementalRemoteKeyedStateHandle checkpoint(
            StreamFusionKeyedStateBackend<?> backend, long id, CheckpointOptions options) throws Exception {
        var future = backend.snapshot(id, id, new MemCheckpointStreamFactory(64 << 20), options);
        future.run();
        return (IncrementalRemoteKeyedStateHandle) future.get().getJobManagerOwnedSnapshot();
    }

    private static StreamFusionKeyedStateBackend<?> backend(
            KeyGroupRange range, List<IncrementalRemoteKeyedStateHandle> restored, boolean incremental) {
        // Only Flink's assigned range is stubbed. Native checkpoint upload, materialization,
        // shared-state reuse and key-group restoration use the real backend adapter below.
        var delegate = supplied(CheckpointableKeyedStateBackend.class, "getKeyGroupRange", range);
        return new StreamFusionKeyedStateBackend<>(delegate, restored, "rocksdb", null, incremental);
    }

    private static void restoreRaw(Region region, KeyGroupsStateHandle handle) throws Exception {
        try (var registry = new CloseableRegistry()) {
            List<KeyGroupStatePartitionStreamProvider> providers = new ArrayList<>();
            for (var offset : handle.getGroupRangeOffsets()) {
                var input = handle.openInputStream();
                registry.registerCloseable(input);
                input.seek(offset.f1);
                providers.add(new KeyGroupStatePartitionStreamProvider(input, offset.f0));
            }
            var context = supplied(StateInitializationContext.class, "getRawKeyedStateInputs", providers);
            region.participant.restoreRawState(context);
        }
    }

    private static <T> T supplied(Class<T> type, String getter, Object value) {
        return type.cast(java.lang.reflect.Proxy.newProxyInstance(
                type.getClassLoader(), new Class<?>[] {type}, (proxy, method, arguments) -> {
                    if (method.getName().equals(getter)) return value;
                    throw new UnsupportedOperationException(method.toString());
                }));
    }

    private static List<GenericRowData> rows(int seed) {
        var random = new Random(seed);
        List<GenericRowData> rows = new ArrayList<>();
        for (int index = 0; index < 100; index++)
            rows.add(GenericRowData.of(
                    (long) random.nextInt(150),
                    index % 3 == 0 ? null : StringData.fromString("é-" + seed + "-" + index)));
        return rows;
    }

    private static void compare(
            Region region,
            KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> oracle,
            List<GenericRowData> rows)
            throws Exception {
        var serializer = new RowDataSerializer(TYPE);
        var expected = new DataOutputSerializer(128);
        for (var row : rows) oracle.processElement(new StreamRecord<>(serializer.copy(row)));
        for (var record : oracle.extractOutputStreamRecords()) serializer.serialize(record.getValue(), expected);
        oracle.getOutput().clear();
        var actual = new DataOutputSerializer(128);
        try (var input = ArrowRowDataBatch.transpose(rows, TYPE, region.allocator);
                var stream = region.edge.executeStream(List.of(input))) {
            ArrowRowDataBatch next;
            while ((next = stream.next()) != null)
                try (var batch = next) {
                    for (int row = 0; row < batch.size(); row++) serializer.serialize(batch.rowView(row), actual);
                }
        }
        assertThat(actual.getCopyOfBuffer()).isEqualTo(expected.getCopyOfBuffer());
    }

    private static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> flink() throws Exception {
        var keyType = RowType.of(new BigIntType(false));
        var keySerializer = new RowDataSerializer(keyType);
        var harness = new KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData>(
                new KeyedProcessOperator<>(new ProcTimeDeduplicateKeepFirstRowFunction(0)),
                row -> keySerializer.toBinaryRow(GenericRowData.of(row.getLong(0))),
                InternalTypeInfo.of(keyType),
                GROUPS,
                1,
                0);
        harness.setup(new RowDataSerializer(TYPE));
        harness.open();
        return harness;
    }

    private static final class Region implements AutoCloseable {
        final NativeMemoryManager memory = TestingNativeMemoryManager.create();
        final RootAllocator allocator = new RootAllocator(64L << 20);
        final NativeExecutionContext context;
        final NativeRegionStateParticipant participant;
        final ArrowNativePlanBridge edge;

        Region(boolean rocks, KeyGroupRange range, Path directory) throws Exception {
            Files.createDirectories(directory);
            Operator node = Operator.newBuilder()
                    .setPlanNodeId(1)
                    .setInput(Input.newBuilder())
                    .build();
            for (long id : IDS)
                node = Operator.newBuilder()
                        .setPlanNodeId(id)
                        .setDeduplicate(Deduplicate.newBuilder()
                                .setInput(node)
                                .addKeyIndices(0)
                                .setProcessingTime(true)
                                .setGenerateInsert(true))
                        .build();
            byte[] plan = NativePlan.newBuilder()
                    .setProtocolVersion(2)
                    .setRoot(node)
                    .build()
                    .toByteArray();
            var bindings = IDS.stream()
                    .map(id -> rocks
                            ? NativeStateResources.rocksDb(
                                    id,
                                    GROUPS,
                                    range.getStartKeyGroup(),
                                    range.getEndKeyGroup(),
                                    directory.resolve("node-" + id),
                                    8L << 20)
                            : NativeStateResources.memory(id, GROUPS, range.getStartKeyGroup(), range.getEndKeyGroup()))
                    .collect(java.util.stream.Collectors.toList());
            context = new NativeExecutionContext(plan, memory, NativeStateResources.serialize(bindings));
            participant = new NativeRegionStateParticipant(context.state(), IDS, range, directory, memory);
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
