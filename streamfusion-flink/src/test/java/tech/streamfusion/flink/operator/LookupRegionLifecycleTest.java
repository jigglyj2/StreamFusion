/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.operator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static tech.streamfusion.flink.operator.LookupRegionRuntimeTest.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.util.InstantiationUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.CsvLookupSnapshotSource;
import tech.streamfusion.flink.join.NativeLookupSources;
import tech.streamfusion.flink.memory.FlinkManagedMemory;

class LookupRegionLifecycleTest {
    @TempDir
    Path directory;

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
    void serializedFactoryOpensAtTaskOpenAndRestoreReloadsTheSourceLikeFlink(boolean unaligned) throws Exception {
        Path file = directory.resolve("later.csv");
        var table = table(file);
        // Planning and shipping the factory must succeed before the file exists.
        byte[] shipped = InstantiationUtil.serializeObject(factory(table));
        var probe = GenericRowData.of(1L, StringData.fromString("probe"));
        OperatorSubtaskState nativeSnapshot;
        OperatorSubtaskState flinkSnapshot;
        Files.writeString(file, "1,first\n1,second\n");
        try (var allocator = new RootAllocator(64L << 20);
                var input = ArrowRowDataBatch.transpose(List.of(probe), PROBE, allocator);
                var nativeHarness = new NativeRegionTestHarness(deserialize(shipped), List.of(OUTPUT));
                var flink = flink(table, true)) {
            nativeHarness.open();
            // Neither cache refreshes when the source changes during a running task.
            Files.writeString(file, "1,recovered\n");
            nativeHarness.accept(0, input);
            flink.processElement(new StreamRecord<>(probe));
            assertThat(bytes(nativeHarness.rows)).containsExactly(bytes(flink.extractOutputValues()));
            assertThat(nativeHarness.rows).hasSize(2);
            nativeHarness.prepareSnapshotPreBarrier(11);
            flink.prepareSnapshotPreBarrier(11);
            nativeSnapshot = snapshot(nativeHarness.region(), unaligned);
            flinkSnapshot = snapshot(flink.getOperator(), unaligned);
            assertThat(nativeSnapshot.hasState())
                    .isEqualTo(flinkSnapshot.hasState())
                    .isFalse();
        }
        FlinkManagedMemory memory;
        try (var allocator = new RootAllocator(64L << 20);
                var input = ArrowRowDataBatch.transpose(List.of(probe), PROBE, allocator);
                var nativeHarness = new NativeRegionTestHarness(deserialize(shipped), List.of(OUTPUT));
                var flink = flink(table, false, flinkSnapshot)) {
            nativeHarness.initializeState(nativeSnapshot);
            nativeHarness.open();
            memory = memory(nativeHarness);
            nativeHarness.accept(0, input);
            flink.processElement(new StreamRecord<>(probe));
            assertThat(bytes(nativeHarness.rows)).containsExactly(bytes(flink.extractOutputValues()));
            assertThat(nativeHarness.rows).hasSize(1);
            assertThat(nativeHarness.rows.get(0).getString(3).toString()).isEqualTo("recovered");
        }
        assertThat(memory.reserved()).isZero();
    }

    @Test
    void downstreamFailureCancelsTheLookupInvocationAndReturnsSnapshotCredit() throws Exception {
        Path file = directory.resolve("side.csv");
        Files.writeString(file, "1,first\n1,second\n");
        FlinkManagedMemory memory;
        var failure = new IllegalStateException("lookup sink failed");
        try (var allocator = new RootAllocator(64L << 20);
                var input = ArrowRowDataBatch.transpose(
                        List.of(GenericRowData.of(1L, StringData.fromString("probe"))), PROBE, allocator);
                var harness = new NativeRegionTestHarness(factory(table(file)), List.of(OUTPUT))) {
            harness.open();
            memory = memory(harness);
            assertThat(memory.reserved()).isPositive();
            harness.sinkFailure = failure;
            harness.cancelOnClose = true;
            assertThatThrownBy(() -> harness.accept(0, input)).isSameAs(failure);
            assertThatThrownBy(() -> harness.accept(0, input)).hasMessageContaining("active or failed invocation");
        }
        assertThat(memory.reserved()).isZero();
    }

    @Test
    void unboundAndMixedKeyedResourcesAreRejectedBeforeReadingTheSource() throws Exception {
        var source = CsvLookupSnapshotSource.from(table(directory.resolve("absent.csv")));
        assertThatThrownBy(() -> NativeLookupSources.NONE.validate(plan(), false))
                .hasMessageContaining("match the physical plan exactly");
        assertThatThrownBy(
                        () -> new StreamFusionNativeRegionOperatorFactory(List.of(PROBE), OUTPUT, plan(), List.of(99L))
                                .withLookupSources(new NativeLookupSources(Map.of(LOOKUP, source))))
                .hasMessageContaining("keyed state initialization");
    }

    private static StreamFusionNativeRegionOperatorFactory deserialize(byte[] bytes) throws Exception {
        return InstantiationUtil.deserializeObject(bytes, LookupRegionLifecycleTest.class.getClassLoader());
    }

    private static OperatorSubtaskState snapshot(
            org.apache.flink.streaming.api.operators.StreamOperator<?> operator, boolean unaligned) throws Exception {
        var location = org.apache.flink.runtime.state.CheckpointStorageLocationReference.getDefault();
        var type = org.apache.flink.runtime.checkpoint.CheckpointType.CHECKPOINT;
        var options = unaligned
                ? org.apache.flink.runtime.checkpoint.CheckpointOptions.unaligned(type, location)
                : org.apache.flink.runtime.checkpoint.CheckpointOptions.alignedNoTimeout(type, location);
        return org.apache.flink.streaming.api.operators.OperatorSnapshotFinalizer.create(operator.snapshotState(
                        11,
                        100,
                        options,
                        new org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory(1 << 20)))
                .getJobManagerOwnedState();
    }
}
