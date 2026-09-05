/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.join;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.runtime.event.WatermarkEvent;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;
import org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFinalizer;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.RecordAttributes;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.arrow.ArrowExchangeCDataBridge;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchSerializer;
import tech.streamfusion.flink.exchange.ArrowExchangeBatch;
import tech.streamfusion.flink.exchange.NativeExchangeFrame;
import tech.streamfusion.flink.exchange.NativeExchangeFrameKeySelector;
import tech.streamfusion.flink.exchange.NativeExchangePlanSerializer;
import tech.streamfusion.flink.state.StreamFusionStateBackend;

class StreamFusionArrowTemporalJoinOperatorTest {
    private static final int MAX_PARALLELISM = 128;
    private static final RowType INPUT_TYPE = RowType.of(
            new LogicalType[] {
                new BigIntType(false), new TimestampType(false, 3), new VarCharType(true, VarCharType.MAX_LENGTH)
            },
            new String[] {"id", "event_time", "payload"});
    private static final RowType OUTPUT_TYPE = RowType.of(new LogicalType[] {
        new BigIntType(false),
        new TimestampType(false, 3),
        new VarCharType(true, VarCharType.MAX_LENGTH),
        new BigIntType(true),
        new TimestampType(true, 3),
        new VarCharType(true, VarCharType.MAX_LENGTH)
    });
    private static final byte[] EXCHANGE_PLAN =
            NativeExchangePlanSerializer.hash(INPUT_TYPE, new int[] {0}, MAX_PARALLELISM);

    @Test
    void emitsLatestVersionAtTheCombinedWatermarkWithProbeRowKinds() throws Exception {
        try (RootAllocator allocator = new RootAllocator(64L << 20);
                Harness harness = harness(null, false)) {
            process(harness.operator, allocator, 1, row(7, 1_000, "old", RowKind.INSERT));
            process(harness.operator, allocator, 1, row(7, 2_000, "new", RowKind.UPDATE_AFTER));
            process(harness.operator, allocator, 1, row(7, 3_000, "gone", RowKind.DELETE));
            process(harness.operator, allocator, 0, row(7, 1_500, "first", RowKind.INSERT));
            process(harness.operator, allocator, 0, row(7, 2_500, "second", RowKind.UPDATE_BEFORE));
            process(harness.operator, allocator, 0, row(7, 3_500, "third", RowKind.DELETE));

            assertThat(harness.take()).isEmpty();
            harness.operator.processWatermark1(new Watermark(3_500));
            assertThat(harness.take()).isEmpty();
            harness.operator.processWatermark2(new Watermark(3_500));
            List<GenericRowData> output = harness.take();
            assertThat(output)
                    .extracting(GenericRowData::getRowKind)
                    .containsExactly(RowKind.INSERT, RowKind.UPDATE_BEFORE, RowKind.DELETE);
            assertThat(output.get(0).getString(5).toString()).isEqualTo("old");
            assertThat(output.get(1).getString(5).toString()).isEqualTo("new");
            assertThat(output.get(2).isNullAt(5)).isTrue();
        }
    }

    @Test
    void restoresRowsAndTimersAcrossBackendsAndCheckpointFormats() throws Exception {
        try (RootAllocator allocator = new RootAllocator(64L << 20)) {
            for (boolean sourceRocks : new boolean[] {false, true}) {
                for (boolean targetRocks : new boolean[] {false, true}) {
                    for (SnapshotKind kind : SnapshotKind.values()) {
                        if (kind != SnapshotKind.CANONICAL && sourceRocks != targetRocks) {
                            continue;
                        }
                        OperatorSubtaskState snapshot;
                        try (Harness source = harness(null, sourceRocks)) {
                            process(source.operator, allocator, 1, row(9, 1_000, "version", RowKind.INSERT));
                            process(source.operator, allocator, 0, row(9, 1_500, "probe", RowKind.INSERT));
                            snapshot = snapshot(source.operator, kind, 42);
                        }
                        try (Harness target = harness(snapshot, targetRocks)) {
                            target.operator.processWatermark1(new Watermark(1_500));
                            target.operator.processWatermark2(new Watermark(1_500));
                            List<GenericRowData> output = target.take();
                            assertThat(output).hasSize(1);
                            assertThat(output.get(0).getString(5).toString()).isEqualTo("version");
                        }
                    }
                }
            }
        }
    }

    @Test
    void appliesResidualConditionAndPreservesLeftOuterRows() throws Exception {
        String code = "public class TemporalPayloadCondition "
                + "extends org.apache.flink.api.common.functions.AbstractRichFunction "
                + "implements org.apache.flink.table.runtime.generated.JoinCondition {"
                + "public TemporalPayloadCondition(Object[] references) {}"
                + "public boolean apply(org.apache.flink.table.data.RowData left, "
                + "org.apache.flink.table.data.RowData right) {"
                + "return right.getString(2).toString().equals(\"accepted\");"
                + "}}";
        GeneratedJoinCondition condition = new GeneratedJoinCondition("TemporalPayloadCondition", code, new Object[0]);
        try (RootAllocator allocator = new RootAllocator(64L << 20);
                Harness harness = harness(null, false, condition)) {
            process(harness.operator, allocator, 1, row(1, 1_000, "rejected", RowKind.INSERT));
            process(harness.operator, allocator, 1, row(2, 1_000, "accepted", RowKind.INSERT));
            process(harness.operator, allocator, 0, row(1, 1_500, "left-1", RowKind.INSERT));
            process(harness.operator, allocator, 0, row(2, 1_500, "left-2", RowKind.INSERT));
            harness.operator.processWatermark1(new Watermark(1_500));
            harness.operator.processWatermark2(new Watermark(1_500));

            List<GenericRowData> output = harness.take();
            assertThat(output).hasSize(2);
            GenericRowData rejected = output.stream()
                    .filter(row -> row.getLong(0) == 1)
                    .findFirst()
                    .orElseThrow();
            GenericRowData accepted = output.stream()
                    .filter(row -> row.getLong(0) == 2)
                    .findFirst()
                    .orElseThrow();
            assertThat(rejected.isNullAt(5)).isTrue();
            assertThat(accepted.getString(5).toString()).isEqualTo("accepted");
        }
    }

    @Test
    void processingTimeCarriesProbeRetractionsAndExpiresTheCurrentVersion() throws Exception {
        try (RootAllocator allocator = new RootAllocator(64L << 20);
                Harness harness = harness(null, false, null, true, FlinkJoinType.INNER, 10, 20)) {
            harness.operator.setProcessingTime(100);
            process(harness.operator, allocator, 1, row(7, 1_000, "current", RowKind.INSERT));
            process(harness.operator, allocator, 0, row(7, 1_500, "before", RowKind.UPDATE_BEFORE));
            assertThat(harness.take()).singleElement().satisfies(row -> {
                assertThat(row.getRowKind()).isEqualTo(RowKind.UPDATE_BEFORE);
                assertThat(row.getString(5).toString()).isEqualTo("current");
            });

            process(harness.operator, allocator, 1, row(7, 1_000, "current", RowKind.DELETE));
            process(harness.operator, allocator, 0, row(7, 1_500, "after-delete", RowKind.DELETE));
            assertThat(harness.take()).isEmpty();

            process(harness.operator, allocator, 1, row(7, 2_000, "expires", RowKind.UPDATE_AFTER));
            harness.operator.setProcessingTime(121);
            process(harness.operator, allocator, 0, row(7, 2_500, "after-ttl", RowKind.INSERT));
            assertThat(harness.take()).isEmpty();
        }
    }

    @Test
    void rocksCheckpointsReuseSstsAndRestoreVersionAndTimerState() throws Exception {
        try (RootAllocator allocator = new RootAllocator(64L << 20)) {
            OperatorSubtaskState second;
            try (Harness source = harness(null, true)) {
                process(source.operator, allocator, 1, row(7, 1_000, "right", RowKind.INSERT));
                process(source.operator, allocator, 0, row(7, 1_500, "left", RowKind.INSERT));
                OperatorSubtaskState first = source.operator.snapshot(20, 20);
                IncrementalRemoteKeyedStateHandle firstHandle = incremental(first);
                source.operator.notifyOfCompletedCheckpoint(20);
                second = source.operator.snapshot(21, 21);
                IncrementalRemoteKeyedStateHandle secondHandle = incremental(second);
                assertThat(secondHandle.getSharedState()).hasSameSizeAs(firstHandle.getSharedState());
                assertThat(secondHandle.getCheckpointedSize()).isLessThan(firstHandle.getCheckpointedSize());
            }
            try (Harness restored = harness(second, true)) {
                restored.operator.processWatermark1(new Watermark(1_500));
                restored.operator.processWatermark2(new Watermark(1_500));
                assertThat(restored.take()).singleElement().satisfies(row -> assertThat(
                                row.getString(5).toString())
                        .isEqualTo("right"));
            }
        }
    }

    private static IncrementalRemoteKeyedStateHandle incremental(OperatorSubtaskState state) {
        assertThat(state.getRawKeyedState()).isEmpty();
        assertThat(state.getManagedKeyedState()).hasSize(1);
        return (IncrementalRemoteKeyedStateHandle)
                state.getManagedKeyedState().iterator().next();
    }

    private static OperatorSubtaskState snapshot(
            KeyedTwoInputStreamOperatorTestHarness<Integer, NativeExchangeFrame, NativeExchangeFrame, ArrowRowDataBatch>
                    harness,
            SnapshotKind kind,
            long checkpointId)
            throws Exception {
        if (kind == SnapshotKind.CANONICAL) {
            return harness.snapshotWithLocalState(
                            checkpointId, checkpointId, SavepointType.savepoint(SavepointFormatType.CANONICAL))
                    .getJobManagerOwnedState();
        }
        CheckpointStorageLocationReference location = CheckpointStorageLocationReference.getDefault();
        CheckpointOptions options = kind == SnapshotKind.UNALIGNED
                ? CheckpointOptions.unaligned(CheckpointType.CHECKPOINT, location)
                : CheckpointOptions.alignedNoTimeout(CheckpointType.CHECKPOINT, location);
        return OperatorSnapshotFinalizer.create(harness.getOperator()
                        .snapshotState(checkpointId, checkpointId, options, new MemCheckpointStreamFactory(64 << 20)))
                .getJobManagerOwnedState();
    }

    private static Harness harness(OperatorSubtaskState state, boolean rocks) throws Exception {
        return harness(state, rocks, null);
    }

    private static Harness harness(OperatorSubtaskState state, boolean rocks, GeneratedJoinCondition condition)
            throws Exception {
        return harness(state, rocks, condition, false, FlinkJoinType.LEFT, 0, 0);
    }

    private static Harness harness(
            OperatorSubtaskState state,
            boolean rocks,
            GeneratedJoinCondition condition,
            boolean processingTime,
            FlinkJoinType joinType,
            long minRetention,
            long maxRetention)
            throws Exception {
        NativeExchangeFrameKeySelector selector = new NativeExchangeFrameKeySelector(MAX_PARALLELISM);
        KeyedTwoInputStreamOperatorTestHarness<Integer, NativeExchangeFrame, NativeExchangeFrame, ArrowRowDataBatch>
                harness = new KeyedTwoInputStreamOperatorTestHarness<>(
                        operator(condition, processingTime, joinType, minRetention, maxRetention),
                        selector,
                        selector,
                        Types.INT,
                        MAX_PARALLELISM,
                        1,
                        0);
        harness.setStateBackend(new StreamFusionStateBackend(
                rocks ? new EmbeddedRocksDBStateBackend(true) : new HashMapStateBackend()));
        List<GenericRowData> captured = new ArrayList<>();
        harness.setOutputCreator(ignored -> new CapturingOutput(captured));
        harness.setup(ArrowRowDataBatchSerializer.INSTANCE);
        if (state != null) {
            harness.initializeState(state);
        }
        harness.open();
        return new Harness(harness, captured);
    }

    private static StreamFusionArrowTemporalJoinOperator operator(GeneratedJoinCondition condition) {
        return operator(condition, false, FlinkJoinType.LEFT, 0, 0);
    }

    private static StreamFusionArrowTemporalJoinOperator operator(
            GeneratedJoinCondition condition,
            boolean processingTime,
            FlinkJoinType joinType,
            long minRetention,
            long maxRetention) {
        RowDataKeySelector selector = KeySelectorUtil.getRowDataSelector(
                StreamFusionArrowTemporalJoinOperatorTest.class.getClassLoader(),
                new int[] {0},
                InternalTypeInfo.of(INPUT_TYPE));
        return new StreamFusionArrowTemporalJoinOperator(
                INPUT_TYPE,
                INPUT_TYPE,
                OUTPUT_TYPE,
                new int[] {0},
                new int[] {0},
                StreamFusionTemporalJoinPlan.create(
                        INPUT_TYPE,
                        INPUT_TYPE,
                        new int[] {0},
                        new int[] {0},
                        new boolean[] {true},
                        joinType,
                        processingTime,
                        processingTime ? -1 : 1,
                        processingTime ? -1 : 1,
                        minRetention,
                        maxRetention),
                selector,
                selector,
                EXCHANGE_PLAN,
                EXCHANGE_PLAN,
                processingTime,
                joinType,
                condition);
    }

    private static GenericRowData row(long key, long time, String payload, RowKind kind) {
        GenericRowData row =
                GenericRowData.of(key, TimestampData.fromEpochMillis(time), StringData.fromString(payload));
        row.setRowKind(kind);
        return row;
    }

    private static void process(
            KeyedTwoInputStreamOperatorTestHarness<Integer, NativeExchangeFrame, NativeExchangeFrame, ArrowRowDataBatch>
                    harness,
            RootAllocator allocator,
            int side,
            GenericRowData row)
            throws Exception {
        try (ArrowRowDataBatch input = ArrowRowDataBatch.transpose(List.of(row), INPUT_TYPE, allocator)
                        .withEnvelope(new RowKind[] {row.getRowKind()}, new boolean[] {false}, new long[] {0});
                ArrowExchangeBatch.EnvelopeBatch envelope = ArrowExchangeBatch.withEnvelope(input, INPUT_TYPE)) {
            List<NativeExchangeFrame> frames = ArrowExchangeCDataBridge.route(
                    EXCHANGE_PLAN,
                    envelope.batch(),
                    allocator,
                    tech.streamfusion.flink.TestingNativeMemoryManager.create());
            assertThat(frames).hasSize(1);
            StreamRecord<NativeExchangeFrame> record = new StreamRecord<>(frames.get(0));
            if (side == 0) {
                harness.processElement1(record);
            } else {
                harness.processElement2(record);
            }
        }
    }

    private enum SnapshotKind {
        ALIGNED,
        UNALIGNED,
        CANONICAL
    }

    private static final class Harness implements AutoCloseable {
        private final KeyedTwoInputStreamOperatorTestHarness<
                        Integer, NativeExchangeFrame, NativeExchangeFrame, ArrowRowDataBatch>
                operator;
        private final List<GenericRowData> captured;

        private Harness(
                KeyedTwoInputStreamOperatorTestHarness<
                                Integer, NativeExchangeFrame, NativeExchangeFrame, ArrowRowDataBatch>
                        operator,
                List<GenericRowData> captured) {
            this.operator = operator;
            this.captured = captured;
        }

        private List<GenericRowData> take() {
            List<GenericRowData> output = List.copyOf(captured);
            captured.clear();
            return output;
        }

        @Override
        public void close() throws Exception {
            operator.close();
        }
    }

    /** Copies the borrowed Arrow output before the producer-owned release callback runs. */
    private static final class CapturingOutput implements Output<StreamRecord<ArrowRowDataBatch>> {
        private final List<GenericRowData> captured;

        private CapturingOutput(List<GenericRowData> captured) {
            this.captured = captured;
        }

        @Override
        public void collect(StreamRecord<ArrowRowDataBatch> record) {
            ArrowRowDataBatch batch = record.getValue();
            for (int row = 0; row < batch.size(); row++) {
                RowData view = batch.rowView(row);
                GenericRowData copy = new GenericRowData(OUTPUT_TYPE.getFieldCount());
                copy.setRowKind(batch.rowKind(row));
                for (int field = 0; field < OUTPUT_TYPE.getFieldCount(); field++) {
                    copy.setField(
                            field,
                            RowData.createFieldGetter(OUTPUT_TYPE.getTypeAt(field), field)
                                    .getFieldOrNull(view));
                }
                captured.add(copy);
            }
        }

        @Override
        public void close() {}

        @Override
        public void emitWatermark(Watermark mark) {}

        @Override
        public void emitWatermarkStatus(WatermarkStatus watermarkStatus) {}

        @Override
        public <X> void collect(OutputTag<X> outputTag, StreamRecord<X> record) {}

        @Override
        public void emitLatencyMarker(LatencyMarker latencyMarker) {}

        @Override
        public void emitRecordAttributes(RecordAttributes recordAttributes) {}

        @Override
        public void emitWatermark(WatermarkEvent watermarkEvent) {}
    }
}
