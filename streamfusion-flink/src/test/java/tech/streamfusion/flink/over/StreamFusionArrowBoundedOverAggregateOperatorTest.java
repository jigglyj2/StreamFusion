/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.over;

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
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.TestingNativeMemoryManager;
import tech.streamfusion.flink.arrow.ArrowExchangeCDataBridge;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchSerializer;
import tech.streamfusion.flink.exchange.ArrowExchangeBatch;
import tech.streamfusion.flink.exchange.NativeExchangeFrame;
import tech.streamfusion.flink.exchange.NativeExchangeFrameKeySelector;
import tech.streamfusion.flink.exchange.NativeExchangePlanSerializer;
import tech.streamfusion.flink.proto.FlinkLogicalTypeProto;
import tech.streamfusion.flink.state.StreamFusionStateBackend;
import tech.streamfusion.proto.plan.v1.AggregateCall;
import tech.streamfusion.proto.plan.v1.AggregateFunction;
import tech.streamfusion.proto.plan.v1.EmptyType;
import tech.streamfusion.proto.plan.v1.Field;
import tech.streamfusion.proto.plan.v1.Input;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;
import tech.streamfusion.proto.plan.v1.OverAggregate;
import tech.streamfusion.proto.plan.v1.OverTimeAttribute;
import tech.streamfusion.proto.plan.v1.Schema;

class StreamFusionArrowBoundedOverAggregateOperatorTest {
    private static final int MAX_PARALLELISM = 128;
    private static final RowType INPUT_TYPE = RowType.of(
            false,
            new LogicalType[] {new BigIntType(false), new BigIntType(false), new BigIntType(false)},
            new String[] {"account", "sequence", "amount"});
    private static final RowType OUTPUT_TYPE = RowType.of(
            false,
            new LogicalType[] {new BigIntType(false), new BigIntType(false), new BigIntType(false), new BigIntType(true)
            },
            new String[] {"account", "sequence", "amount", "running_sum"});
    private static final byte[] EXCHANGE_PLAN =
            NativeExchangePlanSerializer.hash(INPUT_TYPE, new int[] {0}, MAX_PARALLELISM, 1, true);

    @Test
    void emitsSortedRangePeersOnMemoryAndRocksDb() throws Exception {
        assertThat(operator().getOperatorAttributes().isInternalSorterSupported())
                .isTrue();
        assertThat(operator().getOperatorAttributes().isOutputOnlyAfterEndOfStream())
                .isTrue();
        try (RootAllocator inputs = new RootAllocator(64L << 20)) {
            for (boolean rocks : new boolean[] {false, true}) {
                try (Harness harness = harness(null, rocks)) {
                    process(harness, inputs, row(7, 2, 20), row(7, 1, 10), row(7, 3, 5));
                    assertThat(harness.take()).isEmpty();
                    harness.endInput();
                    assertThat(harness.take()).containsExactly("+I:7:1:10:10", "+I:7:2:20:30", "+I:7:3:5:25");
                }
            }
        }
    }

    @Test
    void appliesRetractionsBeforeTerminalOutput() throws Exception {
        try (RootAllocator inputs = new RootAllocator(64L << 20);
                Harness harness = harness(null, false)) {
            GenericRowData deleted = row(7, 1, 10);
            deleted.setRowKind(RowKind.DELETE);
            process(harness, inputs, row(7, 1, 10), row(7, 2, 20), deleted);
            harness.endInput();
            assertThat(harness.take()).containsExactly("+I:7:2:20:20");
        }
    }

    @Test
    void restoresAlignedUnalignedAndCanonicalStateAcrossSupportedBackends() throws Exception {
        try (RootAllocator inputs = new RootAllocator(64L << 20)) {
            for (boolean sourceRocks : new boolean[] {false, true}) {
                for (boolean targetRocks : new boolean[] {false, true}) {
                    for (SnapshotKind kind : SnapshotKind.values()) {
                        if (kind != SnapshotKind.CANONICAL && sourceRocks != targetRocks) {
                            continue;
                        }
                        OperatorSubtaskState state;
                        try (Harness source = harness(null, sourceRocks)) {
                            process(source, inputs, row(7, 2, 20), row(7, 1, 10));
                            state = snapshot(source, kind);
                        }
                        try (Harness target = harness(state, targetRocks)) {
                            process(target, inputs, row(7, 3, 5));
                            target.endInput();
                            assertThat(target.take()).containsExactly("+I:7:1:10:10", "+I:7:2:20:30", "+I:7:3:5:25");
                        }
                    }
                }
            }
        }
    }

    @Test
    void rocksDbCheckpointsReuseUnchangedSsts() throws Exception {
        try (RootAllocator inputs = new RootAllocator(64L << 20);
                Harness source = harness(null, true)) {
            process(source, inputs, row(7, 2, 20), row(7, 1, 10));
            OperatorSubtaskState first = source.snapshot(20, 20);
            IncrementalRemoteKeyedStateHandle firstHandle = incremental(first);
            source.notifyOfCompletedCheckpoint(20);
            OperatorSubtaskState second = source.snapshot(21, 21);
            IncrementalRemoteKeyedStateHandle secondHandle = incremental(second);
            assertThat(secondHandle.getSharedState()).hasSameSizeAs(firstHandle.getSharedState());
            assertThat(secondHandle.getCheckpointedSize()).isLessThan(firstHandle.getCheckpointedSize());
        }
    }

    private static IncrementalRemoteKeyedStateHandle incremental(OperatorSubtaskState state) {
        assertThat(state.getRawKeyedState()).isEmpty();
        assertThat(state.getManagedKeyedState()).hasSize(1);
        return (IncrementalRemoteKeyedStateHandle)
                state.getManagedKeyedState().iterator().next();
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
        Harness harness = new Harness(operator());
        harness.setStateBackend(new StreamFusionStateBackend(
                rocks ? new EmbeddedRocksDBStateBackend(true) : new HashMapStateBackend()));
        harness.setup(ArrowRowDataBatchSerializer.INSTANCE);
        if (state != null) {
            harness.initializeState(state);
        }
        harness.open();
        return harness;
    }

    private static StreamFusionArrowBoundedOverAggregateOperator operator() {
        return new StreamFusionArrowBoundedOverAggregateOperator(INPUT_TYPE, OUTPUT_TYPE, plan(), EXCHANGE_PLAN);
    }

    private static void process(Harness harness, RootAllocator allocator, GenericRowData... rows) throws Exception {
        RowKind[] kinds = new RowKind[rows.length];
        for (int index = 0; index < rows.length; index++) {
            kinds[index] = rows[index].getRowKind();
        }
        try (ArrowRowDataBatch batch = ArrowRowDataBatch.transpose(List.of(rows), INPUT_TYPE, allocator)
                        .withEnvelope(kinds, new boolean[rows.length], new long[rows.length]);
                ArrowExchangeBatch.EnvelopeBatch envelope = ArrowExchangeBatch.withEnvelope(batch, INPUT_TYPE)) {
            for (NativeExchangeFrame frame : ArrowExchangeCDataBridge.route(
                    EXCHANGE_PLAN, envelope.batch(), allocator, TestingNativeMemoryManager.create())) {
                harness.processElement(new StreamRecord<>(frame));
            }
        }
    }

    private static GenericRowData row(long account, long sequence, long amount) {
        return GenericRowData.of(account, sequence, amount);
    }

    private static byte[] plan() {
        tech.streamfusion.proto.plan.v1.LogicalType bigint = tech.streamfusion.proto.plan.v1.LogicalType.newBuilder()
                .setBigint(EmptyType.getDefaultInstance())
                .build();
        OverAggregate aggregate = OverAggregate.newBuilder()
                .setInput(Operator.newBuilder().setInput(Input.newBuilder()))
                .addPartitionKeyIndices(0)
                .setOrderKeyIndex(1)
                .setRowsFrame(false)
                .setPrecedingOffset(1)
                .setTimeAttribute(OverTimeAttribute.OVER_TIME_ATTRIBUTE_NON_TIME)
                .addAggregateCalls(AggregateCall.newBuilder()
                        .setFunction(AggregateFunction.AGGREGATE_FUNCTION_SUM)
                        .setInputIndex(2)
                        .setInputType(bigint)
                        .setOutputType(bigint)
                        .setRetractable(true))
                .setInputSchema(schema(INPUT_TYPE))
                .setOutputSchema(schema(OUTPUT_TYPE))
                .setInputChangelog(true)
                .setSortAscending(true)
                .setSortNullsLast(false)
                .setBoundedFinalOutput(true)
                .build();
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setOverAggregate(aggregate))
                .build()
                .toByteArray();
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

        private Harness(StreamFusionArrowBoundedOverAggregateOperator operator) throws Exception {
            super(operator, new NativeExchangeFrameKeySelector(MAX_PARALLELISM), Types.INT, MAX_PARALLELISM, 1, 0);
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
                        + row.getLong(2)
                        + ":"
                        + row.getLong(3));
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
