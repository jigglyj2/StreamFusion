/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashSet;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.runtime.metrics.WatermarkGauge;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.types.RowKind;
import tech.streamfusion.flink.arrow.ArrowExchangeCDataBridge;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.exchange.ArrowExchangeBatch;

/** One real Flink bundle owner for each native subtask; the oracle is routed independently of Rust. */
final class MiniBatchRescalingFixture implements AutoCloseable {
    private static final int TRIGGER = 7;
    final SharedAggregateRuntimeHarness nativeTask;
    final KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> flink;
    private final WatermarkGauge outputWatermark = new WatermarkGauge();

    MiniBatchRescalingFixture(boolean rocks, int parallelism, int subtask, Snapshots state) throws Exception {
        flink = SharedAggregateFlinkOracle.restored(
                rocks, TRIGGER, parallelism, subtask, state == null ? null : state.flinkState());
        try {
            nativeTask = new SharedAggregateRuntimeHarness(
                    rocks,
                    state == null ? null : state.nativeState(),
                    parallelism,
                    subtask,
                    true,
                    SharedMiniBatchControlTest.plan(TRIGGER));
        } catch (Exception failure) {
            flink.close();
            throw failure;
        }
        flink.getOperator().getMetricGroup().gauge("currentInputWatermark", new WatermarkGauge());
        flink.getOperator().getMetricGroup().gauge("currentOutputWatermark", outputWatermark);
        check();
    }

    static void input(List<MiniBatchRescalingFixture> tasks, RootAllocator allocator, List<RowData> rows, int phase)
            throws Exception {
        var selector = KeySelectorUtil.getRowDataSelector(
                MiniBatchRescalingFixture.class.getClassLoader(),
                new int[] {0},
                InternalTypeInfo.of(SharedAggregateFlinkOracle.INPUT));
        var keyGroups = new HashSet<Integer>();
        var subtasks = new HashSet<Integer>();
        for (int start = 0; start < rows.size(); start += 64) {
            var arrival = rows.subList(start, Math.min(rows.size(), start + 64));
            var kinds = new RowKind[arrival.size()];
            var present = new boolean[arrival.size()];
            var timestamps = new long[arrival.size()];
            for (int index = 0; index < arrival.size(); index++) {
                var row = arrival.get(index);
                kinds[index] = row.getRowKind();
                present[index] = index % 3 != 0;
                timestamps[index] = phase * 10000L + start + index;
                int group = KeyGroupRangeAssignment.assignToKeyGroup(selector.getKey(row), 16);
                int subtask = KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(16, tasks.size(), group);
                keyGroups.add(group);
                subtasks.add(subtask);
                var oracle = tasks.get(subtask).flink;
                oracle.getOperator()
                        .getMetricGroup()
                        .getIOMetricGroup()
                        .getNumRecordsInCounter()
                        .inc();
                oracle.processElement(
                        present[index] ? new StreamRecord<>(row, timestamps[index]) : new StreamRecord<>(row));
            }
            try (var batch = ArrowRowDataBatch.transpose(arrival, SharedAggregateFlinkOracle.INPUT, allocator)
                            .withEnvelope(kinds, present, timestamps);
                    var envelope = ArrowExchangeBatch.withEnvelope(batch, SharedAggregateFlinkOracle.INPUT, null)) {
                for (var frame : ArrowExchangeCDataBridge.route(
                        SharedAggregateRuntimeHarness.exchangePlan(tasks.size()),
                        envelope.batch(),
                        allocator,
                        tasks.get(0).nativeTask.nativeMemory)) {
                    int subtask =
                            KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(16, tasks.size(), frame.keyGroup());
                    tasks.get(subtask).nativeTask.processElement(0, new StreamRecord<>(frame));
                }
            }
            for (var task : tasks) task.check();
        }
        assertThat(subtasks).hasSize(tasks.size());
        if (phase == 0) assertThat(keyGroups).hasSize(16);
    }

    Snapshots snapshot(int mode, long id) throws Exception {
        flink.getOperator().prepareSnapshotPreBarrier(id);
        nativeTask.region().prepareSnapshotPreBarrier(id);
        check();
        // Flink's own canonical savepoint is repartitioned separately from native state.
        var original = flink.snapshotWithLocalState(id, id, SavepointType.savepoint(SavepointFormatType.CANONICAL))
                .getJobManagerOwnedState();
        return new Snapshots(SharedAggregateCheckpointTest.snapshot(nativeTask, mode, id), original);
    }

    void finish() throws Exception {
        flink.getOperator().finish();
        nativeTask.region().endInput(1);
        check();
    }

    private void check() throws Exception {
        SharedMiniBatchMetricSurfaceTest.check(nativeTask, flink, outputWatermark);
        nativeTask.changelog.clear();
    }

    @Override
    public void close() throws Exception {
        try {
            nativeTask.close();
        } finally {
            flink.close();
        }
    }

    static final class Snapshots {
        private final OperatorSubtaskState nativeState;
        private final OperatorSubtaskState flinkState;

        Snapshots(OperatorSubtaskState nativeState, OperatorSubtaskState flinkState) {
            this.nativeState = nativeState;
            this.flinkState = flinkState;
        }

        OperatorSubtaskState nativeState() {
            return nativeState;
        }

        OperatorSubtaskState flinkState() {
            return flinkState;
        }

        Snapshots assign(int oldParallelism, int newParallelism, int subtask) throws Exception {
            return new Snapshots(
                    AbstractStreamOperatorTestHarness.repartitionOperatorState(
                            nativeState, 16, oldParallelism, newParallelism, subtask),
                    AbstractStreamOperatorTestHarness.repartitionOperatorState(
                            flinkState, 16, oldParallelism, newParallelism, subtask));
        }

        static Snapshots combine(Snapshots left, Snapshots right) throws Exception {
            return new Snapshots(
                    AbstractStreamOperatorTestHarness.repackageState(left.nativeState, right.nativeState),
                    AbstractStreamOperatorTestHarness.repackageState(left.flinkState, right.flinkState));
        }

        void discard() throws Exception {
            nativeState.discardState();
            flinkState.discardState();
        }
    }
}
