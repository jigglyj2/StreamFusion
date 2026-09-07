/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.operator;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;
import org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFinalizer;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedMultiInputStreamOperatorTestHarness;
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
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchSerializer;
import tech.streamfusion.flink.memory.FlinkManagedMemory;
import tech.streamfusion.flink.memory.StreamFusionTaskMemory;
import tech.streamfusion.flink.state.StreamFusionStateBackend;
import tech.streamfusion.proto.plan.v1.Deduplicate;
import tech.streamfusion.proto.plan.v1.Input;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;

/** Exercises the production region operator, not an operator-specific state/JNI test adapter. */
class StatefulNativeRegionRuntimeTest {
    private static final RowType TYPE = RowType.of(new BigIntType(false), new VarCharType());
    private static final int GROUPS = 16;

    @Test
    void translatedKeyedRuntimeConsumesHashFramesAndRestoresBothBackends() throws Exception {
        for (boolean rocks : List.of(false, true)) {
            try (var oracle = flink();
                    var inputs = new RootAllocator(64L << 20)) {
                OperatorSubtaskState snapshot;
                try (var source = new Harness(rocks, null, true)) {
                    compare(source, oracle, inputs, 0);
                    snapshot = snapshot(source, 0, 1);
                }
                try (var target = new Harness(!rocks, snapshot, true)) {
                    compare(target, oracle, inputs, 1);
                }
                assertThat(inputs.getAllocatedMemory()).isZero();
            }
        }
    }

    @Test
    void stableCheckpointFilesOutliveTheNativeRuntimeUntilFlinkFinishesUpload() throws Exception {
        try (var oracle = flink();
                var inputs = new RootAllocator(64L << 20);
                var source = new Harness(true, null)) {
            compare(source, oracle, inputs, 0);
            var options = CheckpointOptions.alignedNoTimeout(
                    CheckpointType.CHECKPOINT, CheckpointStorageLocationReference.getDefault());
            var futures = source.region().snapshotState(50, 50, options, new MemCheckpointStreamFactory(64 << 20));
            // Finish the independent Flink control snapshots first. Closing a Flink operator
            // correctly cancels its unfinished control snapshots; only native SST upload remains.
            org.apache.flink.util.concurrent.FutureUtils.runIfNotDoneAndGet(futures.getKeyedStateRawFuture());
            org.apache.flink.util.concurrent.FutureUtils.runIfNotDoneAndGet(futures.getOperatorStateManagedFuture());
            org.apache.flink.util.concurrent.FutureUtils.runIfNotDoneAndGet(futures.getOperatorStateRawFuture());
            assertThat(futures.getKeyedStateManagedFuture().isDone()).isFalse();
            // Native state is captured, but Flink's asynchronous native upload has not run yet.
            var lifecycleField = StreamFusionArrowNativeRegionOperator.class.getDeclaredField("stateLifecycle");
            lifecycleField.setAccessible(true);
            var dispatcherField = StreamFusionArrowNativeRegionOperator.class.getDeclaredField("dispatcher");
            dispatcherField.setAccessible(true);
            ((tech.streamfusion.flink.arrow.ArrowNativePlanDispatcher) dispatcherField.get(source.region())).close();
            // Closing only the native state owner must preserve the independent Flink upload.
            // Closing the Flink backend itself now deliberately cancels that upload (tested below).
            ((tech.streamfusion.flink.state.NativeRegionStateLifecycle) lifecycleField.get(source.region())).close();
            assertThat(source.nativeMemory.reserved()).isZero();
            var snapshot = OperatorSnapshotFinalizer.create(futures).getJobManagerOwnedState();
            try (var target = new Harness(true, snapshot)) {
                compare(target, oracle, inputs, 1);
            }
        }
    }

    @Test
    void closingFlinkRuntimeCancelsItsPendingNativeCheckpoint() throws Exception {
        try (var inputs = new RootAllocator(64L << 20);
                var oracle = flink();
                var source = new Harness(true, null)) {
            compare(source, oracle, inputs, 0);
            var options = CheckpointOptions.alignedNoTimeout(
                    CheckpointType.CHECKPOINT, CheckpointStorageLocationReference.getDefault());
            var futures = source.region().snapshotState(51, 51, options, new MemCheckpointStreamFactory(64 << 20));
            source.region().close();
            assertThat(futures.getKeyedStateManagedFuture().isCancelled()).isTrue();
            assertThat(source.nativeMemory.reserved()).isZero();
        }
    }

    @Test
    void sharedRuntimeRestoresBothOwnersAndPreservesGeneratedFlinkRecordsAndTimestamps() throws Exception {
        for (boolean rocks : List.of(false, true))
            for (int mode = 0; mode < 3; mode++) {
                try (var oracle = flink();
                        var inputs = new RootAllocator(64L << 20)) {
                    OperatorSubtaskState snapshot;
                    try (var source = new Harness(rocks, null)) {
                        for (int seed = 0; seed < 3; seed++) compare(source, oracle, inputs, seed);
                        source.processWatermark(0, new Watermark(1000));
                        assertThat(source.getOutput()).containsExactly(new Watermark(1000));
                        source.getOutput().clear();
                        snapshot = snapshot(source, mode, 1);
                        if (rocks && mode != 0) {
                            assertThat(snapshot.getRawKeyedState()).isEmpty();
                            var first = (IncrementalRemoteKeyedStateHandle)
                                    snapshot.getManagedKeyedState().iterator().next();
                            source.notifyOfCompletedCheckpoint(1);
                            var second = snapshot(source, mode, 2);
                            var next = (IncrementalRemoteKeyedStateHandle)
                                    second.getManagedKeyedState().iterator().next();
                            assertThat(next.getCheckpointedSize()).isLessThan(first.getCheckpointedSize());
                        } else {
                            assertThat(snapshot.getRawKeyedState()).hasSize(1);
                        }
                    }
                    try (var target = new Harness(mode == 0 ? !rocks : rocks, snapshot)) {
                        for (int seed = 3; seed < 6; seed++) compare(target, oracle, inputs, seed);
                    }
                    assertThat(inputs.getAllocatedMemory()).isZero();
                }
            }
    }

    private static OperatorSubtaskState snapshot(Harness harness, int mode, long id) throws Exception {
        if (mode == 0)
            return harness.snapshotWithLocalState(id, id, SavepointType.savepoint(SavepointFormatType.CANONICAL))
                    .getJobManagerOwnedState();
        var location = CheckpointStorageLocationReference.getDefault();
        var options = mode == 1
                ? CheckpointOptions.alignedNoTimeout(CheckpointType.CHECKPOINT, location)
                : CheckpointOptions.unaligned(CheckpointType.CHECKPOINT, location);
        return OperatorSnapshotFinalizer.create(
                        harness.region().snapshotState(id, id, options, new MemCheckpointStreamFactory(64 << 20)))
                .getJobManagerOwnedState();
    }

    private static void compare(
            Harness target,
            KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> oracle,
            RootAllocator allocator,
            int seed)
            throws Exception {
        var random = new Random(seed);
        var serializer = new RowDataSerializer(TYPE);
        var expected = new DataOutputSerializer(128);
        List<Long> times = new ArrayList<>();
        List<GenericRowData> rows = new ArrayList<>();
        long[] timestamps = new long[30];
        boolean[] present = new boolean[30];
        RowKind[] kinds = new RowKind[30];
        Arrays.fill(present, true);
        Arrays.fill(kinds, RowKind.INSERT);
        for (int index = 0; index < timestamps.length; index++) {
            var row = GenericRowData.of(
                    (long) random.nextInt(50),
                    index % 3 == 0 ? null : StringData.fromString("é-" + seed + "-" + index));
            rows.add(row);
            timestamps[index] = seed * 100L + index;
            oracle.processElement(new StreamRecord<>(serializer.copy(row), timestamps[index]));
        }
        for (var record : oracle.extractOutputStreamRecords()) {
            serializer.serialize(record.getValue(), expected);
            times.add(record.getTimestamp());
        }
        oracle.getOutput().clear();
        try (var batch = ArrowRowDataBatch.transpose(rows, TYPE, allocator).withEnvelope(kinds, present, timestamps)) {
            if (target.framed) {
                try (var envelope =
                        tech.streamfusion.flink.exchange.ArrowExchangeBatch.withEnvelope(batch, TYPE, null)) {
                    var frames = tech.streamfusion.flink.arrow.ArrowExchangeCDataBridge.route(
                            exchangePlan(), envelope.batch(), allocator, target.nativeMemory);
                    assertThat(frames).hasSize(1);
                    target.processElement(0, new StreamRecord<>(frames.get(0)));
                }
            } else target.processElement(0, new StreamRecord<>(batch));
        }
        assertThat(target.captured.getCopyOfBuffer()).isEqualTo(expected.getCopyOfBuffer());
        assertThat(target.capturedTimes).isEqualTo(times);
        target.captured.clear();
        target.capturedTimes.clear();
    }

    private static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> flink() throws Exception {
        var keyType = RowType.of(new BigIntType(false));
        var keys = new RowDataSerializer(keyType);
        var result = new KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData>(
                new KeyedProcessOperator<>(new ProcTimeDeduplicateKeepFirstRowFunction(0)),
                row -> keys.toBinaryRow(GenericRowData.of(row.getLong(0))),
                InternalTypeInfo.of(keyType),
                GROUPS,
                1,
                0);
        result.setup(new RowDataSerializer(TYPE));
        result.open();
        return result;
    }

    private static byte[] plan() {
        Operator root = Operator.newBuilder()
                .setPlanNodeId(1)
                .setInput(Input.newBuilder())
                .build();
        for (long id : List.of(2L, 3L))
            root = Operator.newBuilder()
                    .setPlanNodeId(id)
                    .setDeduplicate(Deduplicate.newBuilder()
                            .setInput(root)
                            .addKeyIndices(0)
                            .setProcessingTime(true)
                            .setGenerateInsert(true))
                    .build();
        return NativePlan.newBuilder()
                .setProtocolVersion(3)
                .setRoot(root)
                .build()
                .toByteArray();
    }

    private static final class Harness extends KeyedMultiInputStreamOperatorTestHarness<Integer, ArrowRowDataBatch> {
        private final boolean framed;
        private final FlinkManagedMemory nativeMemory;
        private final DataOutputSerializer captured = new DataOutputSerializer(128);
        private final List<Long> capturedTimes = new ArrayList<>();

        Harness(boolean rocks, OperatorSubtaskState restore) throws Exception {
            this(rocks, restore, false);
        }

        Harness(boolean rocks, OperatorSubtaskState restore, boolean framed) throws Exception {
            super(
                    framed
                            ? translatedFactory()
                            : new StreamFusionNativeRegionOperatorFactory(List.of(TYPE), TYPE, plan(), List.of(2L, 3L)),
                    GROUPS,
                    1,
                    0);
            this.framed = framed;
            config.setStateKeySerializer(IntSerializer.INSTANCE);
            var frameKeys = new tech.streamfusion.flink.exchange.NativeExchangeFrameKeySelector(GROUPS);
            setKeySelector(
                    0,
                    (Object batch) -> batch instanceof tech.streamfusion.flink.exchange.NativeExchangeFrame
                            ? frameKeys.getKey((tech.streamfusion.flink.exchange.NativeExchangeFrame) batch)
                            : 0);
            setStateBackend(new StreamFusionStateBackend(
                    rocks ? new EmbeddedRocksDBStateBackend(true) : new HashMapStateBackend()));
            // Production consumers borrow a batch synchronously. The default harness output
            // retains the same Java wrapper after the native dispatcher has correctly closed it.
            setOutputCreator(
                    ignored -> new org.apache.flink.streaming.api.operators.Output<StreamRecord<ArrowRowDataBatch>>() {
                        @Override
                        public void collect(StreamRecord<ArrowRowDataBatch> record) {
                            var serializer = new RowDataSerializer(TYPE);
                            var batch = record.getValue();
                            for (int row = 0; row < batch.size(); row++) {
                                try {
                                    serializer.serialize(batch.rowView(row), captured);
                                } catch (java.io.IOException failure) {
                                    throw new java.io.UncheckedIOException(failure);
                                }
                                assertThat(batch.hasTimestamp(row)).isTrue();
                                capturedTimes.add(batch.timestamp(row));
                            }
                        }

                        @Override
                        public void emitWatermark(Watermark mark) {
                            getOutput().add(mark);
                        }

                        @Override
                        public void emitWatermarkStatus(
                                org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus status) {
                            getOutput().add(status);
                        }

                        @Override
                        public void emitLatencyMarker(
                                org.apache.flink.streaming.runtime.streamrecord.LatencyMarker marker) {
                            getOutput().add(marker);
                        }

                        @Override
                        public void emitRecordAttributes(
                                org.apache.flink.streaming.runtime.streamrecord.RecordAttributes attributes) {
                            getOutput().add(attributes);
                        }

                        @Override
                        public void emitWatermark(org.apache.flink.runtime.event.WatermarkEvent event) {
                            getOutput().add(event);
                        }

                        @Override
                        public <X> void collect(org.apache.flink.util.OutputTag<X> tag, StreamRecord<X> record) {
                            throw new UnsupportedOperationException("side output");
                        }

                        @Override
                        public void close() {}
                    });
            setup(ArrowRowDataBatchSerializer.INSTANCE);
            if (restore != null) initializeState(restore);
            open();
            var field = StreamFusionArrowNativeRegionOperator.class.getDeclaredField("memory");
            field.setAccessible(true);
            nativeMemory = (FlinkManagedMemory) ((StreamFusionTaskMemory) field.get(region())).nativeMemoryManager();
        }

        StreamFusionArrowNativeRegionOperator region() {
            return (StreamFusionArrowNativeRegionOperator) operator;
        }

        @Override
        public void close() throws Exception {
            super.close();
            assertThat(nativeMemory.reserved()).isZero();
        }
    }

    private static byte[] exchangePlan() {
        return tech.streamfusion.flink.exchange.NativeExchangePlanSerializer.hash(
                TYPE, new int[] {0}, GROUPS, 1, false);
    }

    private static StreamFusionNativeRegionOperatorFactory translatedFactory() {
        var exchange = tech.streamfusion.flink.exchange.StreamFusionExchangeTranslator.hash(
                NativeRegionInputTest.arrowSource(), TYPE, new int[] {0}, GROUPS, 1, false);
        var transformation = (org.apache.flink.streaming.api.transformations.KeyedMultipleInputTransformation<?>)
                StreamFusionNativeRegionTranslator.translateKeyedInputs(
                        List.of(exchange),
                        List.of(TYPE),
                        TYPE,
                        plan(),
                        List.of(2L, 3L),
                        org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
                                .getExecutionEnvironment());
        return (StreamFusionNativeRegionOperatorFactory) transformation.getOperatorFactory();
    }
}
