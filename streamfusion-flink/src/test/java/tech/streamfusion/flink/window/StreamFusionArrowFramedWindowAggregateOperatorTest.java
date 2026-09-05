/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.window;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
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
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.TestingNativeMemoryManager;
import tech.streamfusion.flink.arrow.ArrowExchangeCDataBridge;
import tech.streamfusion.flink.arrow.ArrowLocalWindowAggregateCDataBridge;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchSerializer;
import tech.streamfusion.flink.exchange.ArrowExchangeBatch;
import tech.streamfusion.flink.exchange.NativeExchangeFrame;
import tech.streamfusion.flink.exchange.NativeExchangeFrameKeySelector;
import tech.streamfusion.flink.exchange.NativeExchangePlanSerializer;
import tech.streamfusion.flink.proto.FlinkLogicalTypeProto;
import tech.streamfusion.flink.state.StreamFusionStateBackend;
import tech.streamfusion.nativebridge.NativeLocalWindowAggregateBridge;
import tech.streamfusion.nativebridge.NativeMemoryManager;
import tech.streamfusion.proto.plan.v1.AggregateCall;
import tech.streamfusion.proto.plan.v1.AggregateFunction;
import tech.streamfusion.proto.plan.v1.EmptyType;
import tech.streamfusion.proto.plan.v1.Field;
import tech.streamfusion.proto.plan.v1.Input;
import tech.streamfusion.proto.plan.v1.LocalWindowAggregate;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;
import tech.streamfusion.proto.plan.v1.Schema;
import tech.streamfusion.proto.plan.v1.WindowAggregate;
import tech.streamfusion.proto.plan.v1.WindowKind;
import tech.streamfusion.proto.plan.v1.WindowProperty;

class StreamFusionArrowFramedWindowAggregateOperatorTest {
    private static final int MAX_PARALLELISM = 128;
    private static final RowType RAW_INPUT_TYPE = RowType.of(
            new LogicalType[] {new BigIntType(false), new TimestampType(false, 3)}, new String[] {"key", "ts"});
    private static final RowType PARTIAL_TYPE = RowType.of(
            new LogicalType[] {
                new BigIntType(false),
                new VarBinaryType(false, VarBinaryType.MAX_LENGTH),
                new BigIntType(false),
                new BigIntType(false)
            },
            new String[] {"key", "accumulator", "window_start", "slice_end"});
    private static final RowType OUTPUT_TYPE = RowType.of(
            new LogicalType[] {
                new BigIntType(false), new BigIntType(false), new TimestampType(false, 3), new TimestampType(false, 3)
            },
            new String[] {"key", "count", "window_start", "window_end"});

    @Test
    void mergesFramedLocalPartialsAndFinishesOnBothBackends() throws Exception {
        assertThat(operator().getOperatorAttributes().isInternalSorterSupported())
                .isTrue();
        assertThat(operator().getOperatorAttributes().isOutputOnlyAfterEndOfStream())
                .isTrue();
        try (RootAllocator inputs = new RootAllocator(64L << 20)) {
            for (boolean rocks : new boolean[] {false, true}) {
                try (Harness harness = harness(null, rocks)) {
                    process(harness, inputs, row(7, 1_000), row(7, 2_000));
                    assertThat(harness.take()).isEmpty();
                    harness.endInput();
                    assertThat(harness.take()).containsExactly("+I:7:2:0:10000");
                }
            }
        }
    }

    @Test
    void boundedGlobalIgnoresIntermediateWatermarksUntilEndOfInput() throws Exception {
        try (RootAllocator inputs = new RootAllocator(64L << 20);
                Harness harness = harness(null, false)) {
            process(harness, inputs, row(7, 1_000));
            harness.processWatermark(new Watermark(9_999));
            assertThat(harness.take()).isEmpty();

            process(harness, inputs, row(7, 2_000));
            harness.endInput();
            assertThat(harness.take()).containsExactly("+I:7:2:0:10000");
        }
    }

    @Test
    void restoresFramedPartialsAcrossCheckpointFormatsAndBackends() throws Exception {
        try (RootAllocator inputs = new RootAllocator(64L << 20)) {
            for (boolean sourceRocks : new boolean[] {false, true}) {
                for (boolean targetRocks : new boolean[] {false, true}) {
                    for (SnapshotKind kind : SnapshotKind.values()) {
                        if (kind != SnapshotKind.CANONICAL && sourceRocks != targetRocks) {
                            continue;
                        }
                        OperatorSubtaskState snapshot;
                        try (Harness source = harness(null, sourceRocks)) {
                            process(source, inputs, row(7, 1_000));
                            snapshot = snapshot(source, kind);
                        }
                        try (Harness target = harness(snapshot, targetRocks)) {
                            process(target, inputs, row(7, 2_000));
                            target.endInput();
                            assertThat(target.take()).containsExactly("+I:7:2:0:10000");
                        }
                    }
                }
            }
        }
    }

    @Test
    void redistributesFramedWindowStateOneToTwoAndBack() throws Exception {
        try (RootAllocator inputs = new RootAllocator(64L << 20)) {
            Map<Integer, GenericRowData> rows = rowsForOwners(inputs, 2, 1_000);
            for (boolean rocks : new boolean[] {false, true}) {
                OperatorSubtaskState one;
                try (Harness initial = harness(null, rocks, 1, 0)) {
                    for (GenericRowData row : rows.values()) {
                        process(initial, inputs, row);
                    }
                    one = initial.snapshot(30, 30);
                }
                OperatorSubtaskState packaged = AbstractStreamOperatorTestHarness.repackageState(one);
                List<OperatorSubtaskState> two = new ArrayList<>();
                for (int subtask = 0; subtask < 2; subtask++) {
                    OperatorSubtaskState assigned = AbstractStreamOperatorTestHarness.repartitionOperatorState(
                            packaged, MAX_PARALLELISM, 1, 2, subtask);
                    try (Harness scaled = harness(assigned, rocks, 2, subtask)) {
                        process(scaled, inputs, row(rows.get(subtask).getLong(0), 11_000));
                        scaled.processWatermark(new Watermark(9_999));
                        assertThat(scaled.take()).isEmpty();
                        two.add(scaled.snapshot(31, 31));
                    }
                }

                OperatorSubtaskState packagedTwo =
                        AbstractStreamOperatorTestHarness.repackageState(two.toArray(new OperatorSubtaskState[0]));
                OperatorSubtaskState assignedBack = AbstractStreamOperatorTestHarness.repartitionOperatorState(
                        packagedTwo, MAX_PARALLELISM, 2, 1, 0);
                try (Harness back = harness(assignedBack, rocks, 1, 0)) {
                    back.endInput();
                    assertThat(back.take()).hasSize(4);
                }
            }
        }
    }

    private static OperatorSubtaskState snapshot(Harness harness, SnapshotKind kind) throws Exception {
        if (kind == SnapshotKind.CANONICAL) {
            return harness.snapshotWithLocalState(12, 12, SavepointType.savepoint(SavepointFormatType.CANONICAL))
                    .getJobManagerOwnedState();
        }
        CheckpointStorageLocationReference location = CheckpointStorageLocationReference.getDefault();
        CheckpointOptions options = kind == SnapshotKind.UNALIGNED
                ? CheckpointOptions.unaligned(CheckpointType.CHECKPOINT, location)
                : CheckpointOptions.alignedNoTimeout(CheckpointType.CHECKPOINT, location);
        return OperatorSnapshotFinalizer.create(
                        harness.getOperator().snapshotState(11, 11, options, new MemCheckpointStreamFactory(64 << 20)))
                .getJobManagerOwnedState();
    }

    private static Harness harness(OperatorSubtaskState state, boolean rocks) throws Exception {
        return harness(state, rocks, 1, 0);
    }

    private static Harness harness(OperatorSubtaskState state, boolean rocks, int parallelism, int subtask)
            throws Exception {
        byte[] exchangePlan = exchangePlan(parallelism);
        Harness harness = new Harness(operator(exchangePlan), exchangePlan, parallelism, subtask);
        harness.setStateBackend(new StreamFusionStateBackend(
                rocks ? new EmbeddedRocksDBStateBackend(true) : new HashMapStateBackend()));
        harness.setup(ArrowRowDataBatchSerializer.INSTANCE);
        if (state != null) {
            harness.initializeState(state);
        }
        harness.open();
        return harness;
    }

    private static Map<Integer, GenericRowData> rowsForOwners(RootAllocator allocator, int parallelism, long timestamp)
            throws Exception {
        Map<Integer, GenericRowData> rows = new HashMap<>();
        for (long key = 0; rows.size() < parallelism; key++) {
            GenericRowData row = row(key, timestamp);
            NativeExchangeFrame frame =
                    frames(allocator, exchangePlan(parallelism), row).get(0);
            int owner = KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(
                    MAX_PARALLELISM, parallelism, frame.keyGroup());
            rows.putIfAbsent(owner, row);
        }
        return rows;
    }

    private static StreamFusionArrowFramedWindowAggregateOperator operator() {
        return operator(exchangePlan(1));
    }

    private static StreamFusionArrowFramedWindowAggregateOperator operator(byte[] exchangePlan) {
        RowDataKeySelector selector = KeySelectorUtil.getRowDataSelector(
                StreamFusionArrowFramedWindowAggregateOperatorTest.class.getClassLoader(),
                new int[] {0},
                InternalTypeInfo.of(PARTIAL_TYPE));
        return new StreamFusionArrowFramedWindowAggregateOperator(
                PARTIAL_TYPE, OUTPUT_TYPE, new int[] {0}, globalPlan(), selector, exchangePlan);
    }

    private static byte[] exchangePlan(int parallelism) {
        return NativeExchangePlanSerializer.hash(PARTIAL_TYPE, new int[] {0}, MAX_PARALLELISM, parallelism, false);
    }

    private static void process(Harness harness, RootAllocator allocator, GenericRowData... rows) throws Exception {
        for (NativeExchangeFrame frame : frames(allocator, harness.exchangePlan, rows)) {
            harness.processElement(new StreamRecord<>(frame));
        }
    }

    private static List<NativeExchangeFrame> frames(
            RootAllocator allocator, byte[] exchangePlan, GenericRowData... rows) throws Exception {
        NativeMemoryManager memory = TestingNativeMemoryManager.create();
        long localHandle = NativeLocalWindowAggregateBridge.create(localPlan(), memory);
        try (ArrowRowDataBatch input = ArrowRowDataBatch.transpose(List.of(rows), RAW_INPUT_TYPE, allocator);
                ArrowRowDataBatch partial = ArrowLocalWindowAggregateCDataBridge.execute(
                        localHandle, input, false, PARTIAL_TYPE, allocator, memory);
                ArrowExchangeBatch.EnvelopeBatch envelope = ArrowExchangeBatch.withEnvelope(partial, PARTIAL_TYPE)) {
            return ArrowExchangeCDataBridge.route(exchangePlan, envelope.batch(), allocator, memory);
        } finally {
            NativeLocalWindowAggregateBridge.destroy(localHandle);
        }
    }

    private static GenericRowData row(long key, long timestamp) {
        return GenericRowData.of(key, TimestampData.fromEpochMillis(timestamp));
    }

    private static byte[] localPlan() {
        LocalWindowAggregate aggregate = LocalWindowAggregate.newBuilder()
                .setInput(Operator.newBuilder().setInput(Input.newBuilder()))
                .addGroupingIndices(0)
                .addAggregateCalls(countCall())
                .setTimeAttributeIndex(1)
                .setKind(WindowKind.WINDOW_KIND_TUMBLE)
                .setSizeMillis(10_000)
                .setShiftTimeZone("UTC")
                .setInputSchema(schema(RAW_INPUT_TYPE))
                .setOutputSchema(schema(PARTIAL_TYPE))
                .build();
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setLocalWindowAggregate(aggregate))
                .build()
                .toByteArray();
    }

    private static byte[] globalPlan() {
        WindowAggregate aggregate = WindowAggregate.newBuilder()
                .setInput(Operator.newBuilder().setInput(Input.newBuilder()))
                .addGroupingIndices(0)
                .addAggregateCalls(countCall())
                .setTimeAttributeIndex(0)
                .setKind(WindowKind.WINDOW_KIND_TUMBLE)
                .setSizeMillis(10_000)
                .setShiftTimeZone("UTC")
                .addWindowProperties(WindowProperty.WINDOW_PROPERTY_START)
                .addWindowProperties(WindowProperty.WINDOW_PROPERTY_END)
                .setInputSchema(schema(PARTIAL_TYPE))
                .setOutputSchema(schema(OUTPUT_TYPE))
                .setPartialAccumulatorIndex(1)
                .setPartialWindowStartIndex(2)
                .setPartialSliceEndIndex(3)
                .setPartialWindowsAreSlices(true)
                .build();
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setWindowAggregate(aggregate))
                .build()
                .toByteArray();
    }

    private static AggregateCall countCall() {
        return AggregateCall.newBuilder()
                .setFunction(AggregateFunction.AGGREGATE_FUNCTION_COUNT_STAR)
                .setOutputType(tech.streamfusion.proto.plan.v1.LogicalType.newBuilder()
                        .setBigint(EmptyType.getDefaultInstance()))
                .build();
    }

    private static Schema schema(RowType type) {
        Schema.Builder schema = Schema.newBuilder();
        for (RowType.RowField field : type.getFields()) {
            schema.addFields(Field.newBuilder()
                    .setName(field.getName())
                    .setType(FlinkLogicalTypeProto.serialize(field.getType())));
        }
        return schema.build();
    }

    private enum SnapshotKind {
        ALIGNED,
        UNALIGNED,
        CANONICAL
    }

    private static final class Harness
            extends KeyedOneInputStreamOperatorTestHarness<Integer, NativeExchangeFrame, ArrowRowDataBatch> {
        private final List<String> captured = new ArrayList<>();
        private final byte[] exchangePlan;

        private Harness(
                StreamFusionArrowFramedWindowAggregateOperator operator,
                byte[] exchangePlan,
                int parallelism,
                int subtask)
                throws Exception {
            super(
                    operator,
                    new NativeExchangeFrameKeySelector(MAX_PARALLELISM),
                    Types.INT,
                    MAX_PARALLELISM,
                    parallelism,
                    subtask);
            this.exchangePlan = exchangePlan.clone();
            setOutputCreator(ignored -> new CapturingOutput(captured));
        }

        private List<String> take() {
            List<String> result = List.copyOf(captured);
            captured.clear();
            return result;
        }
    }

    private static final class CapturingOutput implements Output<StreamRecord<ArrowRowDataBatch>> {
        private final List<String> captured;

        private CapturingOutput(List<String> captured) {
            this.captured = captured;
        }

        @Override
        public void collect(StreamRecord<ArrowRowDataBatch> record) {
            ArrowRowDataBatch batch = record.getValue();
            for (int index = 0; index < batch.size(); index++) {
                RowData row = batch.rowView(index);
                captured.add(batch.rowKind(index).shortString()
                        + ":"
                        + row.getLong(0)
                        + ":"
                        + row.getLong(1)
                        + ":"
                        + row.getTimestamp(2, 3).getMillisecond()
                        + ":"
                        + row.getTimestamp(3, 3).getMillisecond());
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
        public void emitWatermark(org.apache.flink.runtime.event.WatermarkEvent watermarkEvent) {}
    }
}
