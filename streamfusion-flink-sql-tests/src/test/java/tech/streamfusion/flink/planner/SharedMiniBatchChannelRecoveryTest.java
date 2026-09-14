/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/** A checkpoint flushes one bundle; captured Arrow input belongs to a new bundle after recovery. */
class SharedMiniBatchChannelRecoveryTest {
    @ParameterizedTest
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    void checkpointAndReplayPreserveTheFlinkBundleFrontier(boolean rocks, boolean unaligned) throws Exception {
        byte[] plan = SharedMiniBatchControlTest.plan(3);
        var fixture = new SharedAggregateChannelRecoveryTest() {
            @Override
            protected byte[] plan() {
                return plan;
            }
        };
        try (var oracle = SharedAggregateFlinkOracle.create(rocks, 3);
                var allocator = new RootAllocator(64L << 20)) {
            var memory = new SharedChannelStateIO.RoutingMemory();
            var expected = new DataOutputSerializer(128);
            TaskStateSnapshot checkpoint;
            try (var task = fixture.create(rocks, unaligned, null)) {
                var first = fixture.row(10L, RowKind.INSERT);
                oracle.processElement(new StreamRecord<>(first));
                fixture.send(task, allocator, memory, 0, first, false);
                assertThat(oracle.getOutput()).isEmpty();
                assertThat(fixture.bytes(task)).isEmpty();

                var location = CheckpointStorageLocationReference.getDefault();
                var options = unaligned
                        ? CheckpointOptions.unaligned(CheckpointType.CHECKPOINT, location)
                        : CheckpointOptions.alignedNoTimeout(CheckpointType.CHECKPOINT, location);
                var barrier = new CheckpointBarrier(1, 1, options);
                task.processEvent(barrier, 0, 0);
                // Unaligned snapshots flush at the first barrier. Aligned snapshots wait
                // for the other channel, so its next row still belongs to the old bundle.
                if (unaligned) oracle.getOperator().prepareSnapshotPreBarrier(1);
                fixture.drain(oracle.getOutput(), expected);
                assertThat(fixture.bytes(task)).containsExactly(expected.getCopyOfBuffer());
                expected.clear();
                task.getOutput().clear();

                var inflight = fixture.row(1L, RowKind.UPDATE_AFTER);
                oracle.processElement(new StreamRecord<>(inflight));
                fixture.send(task, allocator, memory, 1, inflight, unaligned);
                task.processEvent(barrier, 0, 1);
                if (!unaligned) oracle.getOperator().prepareSnapshotPreBarrier(1);
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
                while (task.getTaskStateManager().getReportedCheckpointId() != 1 && System.nanoTime() < deadline) {
                    task.processAll();
                    Thread.sleep(5);
                }
                assertThat(task.getTaskStateManager().getReportedCheckpointId()).isEqualTo(1);
                checkpoint = task.getTaskStateManager().getLastJobManagerTaskStateSnapshot();
                assertThat(checkpoint
                                .getSubtaskStateByOperatorID(SharedKeyedChannelHarness.REGION)
                                .getInputChannelState()
                                .isEmpty())
                        .isEqualTo(!unaligned);
                fixture.drain(oracle.getOutput(), expected);
                assertThat(fixture.bytes(task)).containsExactly(expected.getCopyOfBuffer());
                expected.clear();
                // Discard this attempt's terminal output. The continuation below must
                // reconstruct pending input solely from the checkpoint and captured channel.
                task.endInput();
                task.waitForTaskCompletion();
            }

            try (var restored = fixture.create(rocks, unaligned, checkpoint)) {
                restored.processAll();
                assertThat(fixture.bytes(restored)).isEmpty();
                for (var row : List.of(fixture.row(10L, RowKind.UPDATE_BEFORE), fixture.row(5L, RowKind.INSERT))) {
                    oracle.processElement(new StreamRecord<>(row));
                    fixture.send(restored, allocator, memory, 0, row, false);
                    fixture.drain(oracle.getOutput(), expected);
                    assertThat(fixture.bytes(restored)).containsExactly(expected.getCopyOfBuffer());
                }
                oracle.getOperator().finish();
                restored.endInput();
                restored.waitForTaskCompletion();
                fixture.drain(oracle.getOutput(), expected);
                assertThat(expected.length()).isPositive();
                assertThat(fixture.bytes(restored)).containsExactly(expected.getCopyOfBuffer());
            }
            assertThat(memory.available()).isEqualTo(memory.limit());
            assertThat(allocator.getAllocatedMemory()).isZero();
        }
    }
}
