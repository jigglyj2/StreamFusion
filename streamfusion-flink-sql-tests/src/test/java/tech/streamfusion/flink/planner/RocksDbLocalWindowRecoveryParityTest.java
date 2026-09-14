/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;
import static tech.streamfusion.flink.planner.SharedWindowRuntimeRecoveryTest.input;
import static tech.streamfusion.flink.planner.SharedWindowRuntimeRecoveryTest.watermark;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.IncrementalLocalKeyedStateHandle;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.LocalSnapshotDirectoryProviderImpl;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFinalizer;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.flink.operator.StreamFusionNativeRegionOperatorFactory;

/** Local-first restoration must initialize native windows with Flink's saved clocks before input. */
class RocksDbLocalWindowRecoveryParityTest {
    @TempDir
    Path temporary;

    @ParameterizedTest
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    void localAndRemoteRetryRestoreWindowClocksAndLateRecordMetrics(boolean attached, boolean unaligned)
            throws Exception {
        for (boolean corrupt : new boolean[] {false, true}) {
            var provider = new LocalSnapshotDirectoryProviderImpl(
                    temporary.resolve("backup-" + corrupt).toFile(),
                    new org.apache.flink.api.common.JobID(),
                    new org.apache.flink.runtime.jobgraph.JobVertexID(),
                    0);
            var local = LocalRecoveryConfig.backupAndRecoveryEnabled(provider);
            var location = CheckpointStorageLocationReference.getDefault();
            var options = unaligned
                    ? CheckpointOptions.unaligned(CheckpointType.CHECKPOINT, location)
                    : CheckpointOptions.alignedNoTimeout(CheckpointType.CHECKPOINT, location);
            OperatorSnapshotFinalizer snapshot;
            OperatorSubtaskState flinkState;
            try (var allocator = new RootAllocator(64L << 20)) {
                try (var flink = oracle(attached, null);
                        var source =
                                region(attached, null, null, local, new RocksDbLocalRecoveryParityTest.Backend(true))) {
                    input(attached, flink, source, allocator, 2, 2000);
                    input(attached, flink, source, allocator, 5, 4000);
                    watermark(attached, flink, source, 1999);
                    input(attached, flink, source, allocator, 9, 2000);
                    source.region().prepareSnapshotPreBarrier(7);
                    snapshot = OperatorSnapshotFinalizer.create(
                            source.region().snapshotState(7, 7, options, new MemCheckpointStreamFactory(64 << 20)));
                    flink.prepareSnapshotPreBarrier(7);
                    flinkState = flink.snapshot(7, 7);
                }
                var remote = snapshot.getJobManagerOwnedState();
                var backup = snapshot.getTaskLocalState();
                try {
                    if (corrupt) {
                        var handle = (IncrementalLocalKeyedStateHandle)
                                backup.getManagedKeyedState().iterator().next();
                        Files.writeString(
                                handle.getDirectoryStateHandle().getDirectory().resolve("node-3/CURRENT"),
                                "invalid-manifest\n");
                    }
                    var backend = new RocksDbLocalRecoveryParityTest.Backend(true);
                    try (var flink = oracle(attached, flinkState);
                            var target = region(
                                    attached,
                                    corrupt ? remote : RocksDbLocalRecoveryParityTest.forbidRemoteFileReads(remote),
                                    backup,
                                    local,
                                    backend)) {
                        assertThat(backend.attempts)
                                .containsExactlyElementsOf(corrupt ? List.of(true, false) : List.of(true));
                        assertThat(backend.failures).isEqualTo(corrupt ? 1 : 0);
                        input(attached, flink, target, allocator, 99, -2000);
                        watermark(attached, flink, target, 999);
                        input(attached, flink, target, allocator, 7, 4000);
                        var random = new java.util.Random((attached ? 17 : 0) + (unaligned ? 1 : 0));
                        for (int phase = 0; phase < 6; phase++) {
                            for (int row = 0; row < 5; row++)
                                input(
                                        attached,
                                        flink,
                                        target,
                                        allocator,
                                        random.nextInt(99) + 1,
                                        (phase + random.nextInt(7) - 3) * 2000L);
                            watermark(attached, flink, target, 3999 + phase * 2000L);
                        }
                        watermark(attached, flink, target, Long.MAX_VALUE);
                    }
                } finally {
                    flinkState.discardState();
                    backup.discardState();
                    remote.discardState();
                }
                assertThat(allocator.getAllocatedMemory()).isZero();
            }
        }
    }

    private static org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.RowData>
            oracle(boolean attached, OperatorSubtaskState state) throws Exception {
        return attached
                ? GlobalWindowFlinkOracle.create(
                        SlicingWindowFlinkPlan.stage("GlobalWindowAggregate", AttachedSlicingWindowFixture.sql(true)),
                        true,
                        state)
                : GlobalWindowFlinkOracle.create(true, state, false);
    }

    private static KeyedNativeMetricHarness region(
            boolean attached,
            OperatorSubtaskState remote,
            OperatorSubtaskState backup,
            LocalRecoveryConfig local,
            RocksDbLocalRecoveryParityTest.Backend backend)
            throws Exception {
        var output = attached ? AttachedSlicingWindowFixture.OUTPUT : SharedSlicingWindowFixture.OUTPUT;
        var factory = new StreamFusionNativeRegionOperatorFactory(
                List.of(SharedSlicingWindowFixture.INPUT),
                output,
                attached ? AttachedSlicingWindowFixture.plan(true) : SharedSlicingWindowFixture.plan(false),
                List.of(3L));
        return new KeyedNativeMetricHarness(
                true, factory, 1, List.of(output), remote, 1, 0, true, local, backup, backend);
    }
}
