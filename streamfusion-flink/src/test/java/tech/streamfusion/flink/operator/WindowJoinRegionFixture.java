/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.operator;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.CollectorOutput;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchSerializer;
import tech.streamfusion.flink.memory.FlinkManagedMemory;
import tech.streamfusion.flink.memory.StreamFusionTaskMemory;
import tech.streamfusion.flink.proto.FlinkLogicalTypeProto;
import tech.streamfusion.flink.state.StreamFusionStateBackend;
import tech.streamfusion.proto.plan.v1.*;

/** Production shared runtime; only the collecting test sink copies borrowed Arrow output. */
final class WindowJoinRegionFixture extends AbstractStreamOperatorTestHarness<ArrowRowDataBatch> {
    static final int GROUPS = 16;
    static final RowType INPUT = RowType.of(new BigIntType(), new BigIntType(false), new VarCharType());
    static final RowType OUTPUT = RowType.of(
            new BigIntType(),
            new BigIntType(false),
            new VarCharType(),
            new BigIntType(),
            new BigIntType(false),
            new VarCharType());
    final DataOutputSerializer rows = new DataOutputSerializer(128);
    final List<Long> timestamps = new ArrayList<>();
    final List<org.apache.flink.streaming.runtime.streamrecord.StreamElement> controls = new ArrayList<>();
    private final FlinkManagedMemory memory;
    long acceptedRows;
    long emittedRows;

    WindowJoinRegionFixture(boolean rocks, OperatorSubtaskState restored) throws Exception {
        super(
                new StreamFusionNativeRegionOperatorFactory(List.of(INPUT, INPUT), OUTPUT, plan(), List.of(3L)),
                new MockEnvironmentBuilder()
                        .setTaskName("window-region")
                        .setManagedMemorySize(256L << 20)
                        .setBufferSize(1024)
                        .setInputSplitProvider(new MockInputSplitProvider())
                        .setMaxParallelism(GROUPS)
                        .setParallelism(1)
                        .setSubtaskIndex(0)
                        .build());
        config.setStateKeySerializer(IntSerializer.INSTANCE);
        config.setStatePartitioner(0, (org.apache.flink.api.java.functions.KeySelector<Object, Integer>) batch -> 0);
        config.setStatePartitioner(1, (org.apache.flink.api.java.functions.KeySelector<Object, Integer>) batch -> 0);
        config.serializeAllConfigs();
        setStateBackend(new StreamFusionStateBackend(
                rocks ? new EmbeddedRocksDBStateBackend(true) : new HashMapStateBackend()));
        setOutputCreator(ignored -> new CollectorOutput<ArrowRowDataBatch>(controls) {
            @Override
            public void collect(StreamRecord<ArrowRowDataBatch> record) {
                var batch = record.getValue();
                emittedRows += batch.size();
                var serializer = new RowDataSerializer(OUTPUT);
                for (int row = 0; row < batch.size(); row++) {
                    RowData value = serializer.copy(batch.rowView(row));
                    value.setRowKind(batch.rowKind(row));
                    try {
                        serializer.serialize(value, rows);
                    } catch (java.io.IOException failure) {
                        throw new java.io.UncheckedIOException(failure);
                    }
                    timestamps.add(batch.hasTimestamp(row) ? batch.timestamp(row) : null);
                }
            }
        });
        setup(ArrowRowDataBatchSerializer.INSTANCE);
        if (restored != null) initializeState(restored);
        open();
        var field = StreamFusionArrowNativeRegionOperator.class.getDeclaredField("memory");
        field.setAccessible(true);
        memory = (FlinkManagedMemory) ((StreamFusionTaskMemory) field.get(operator)).nativeMemoryManager();
    }

    StreamFusionArrowNativeRegionOperator region() {
        return (StreamFusionArrowNativeRegionOperator) operator;
    }

    void processElement(int side, StreamRecord<ArrowRowDataBatch> record) throws Exception {
        acceptedRows += record.getValue().size();
        var input =
                ((StreamFusionArrowNativeRegionOperator) operator).getInputs().get(side);
        input.setKeyContextElement(record);
        input.processElement(record);
    }

    void processWatermark(int side, Watermark watermark) throws Exception {
        ((StreamFusionArrowNativeRegionOperator) operator).getInputs().get(side).processWatermark(watermark);
    }

    OperatorSubtaskState checkpoint(int mode) throws Exception {
        if (mode == 0)
            return snapshotWithLocalState(
                            1,
                            1,
                            org.apache.flink.runtime.checkpoint.SavepointType.savepoint(
                                    org.apache.flink.core.execution.SavepointFormatType.CANONICAL))
                    .getJobManagerOwnedState();
        var location = org.apache.flink.runtime.state.CheckpointStorageLocationReference.getDefault();
        var options = mode == 1
                ? org.apache.flink.runtime.checkpoint.CheckpointOptions.alignedNoTimeout(
                        org.apache.flink.runtime.checkpoint.CheckpointType.CHECKPOINT, location)
                : org.apache.flink.runtime.checkpoint.CheckpointOptions.unaligned(
                        org.apache.flink.runtime.checkpoint.CheckpointType.CHECKPOINT, location);
        var futures = ((StreamFusionArrowNativeRegionOperator) operator)
                .snapshotState(
                        1, 1, options, new org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory(64 << 20));
        return org.apache.flink.streaming.api.operators.OperatorSnapshotFinalizer.create(futures)
                .getJobManagerOwnedState();
    }

    @Override
    public void close() throws Exception {
        try {
            super.close();
            assertThat(memory.reserved()).isZero();
        } finally {
            getEnvironment().close();
        }
    }

    static byte[] plan() {
        var schema = Schema.newBuilder();
        for (var field : INPUT.getFields())
            schema.addFields(Field.newBuilder()
                    .setName(field.getName())
                    .setType(FlinkLogicalTypeProto.serialize(field.getType())));
        var join = Operator.newBuilder()
                .setPlanNodeId(3)
                .setClearRecordTimestamps(true)
                .setWindowJoin(WindowJoin.newBuilder()
                        .setLeftInput(edge(0))
                        .setRightInput(edge(1))
                        .setLeftSchema(schema)
                        .setRightSchema(schema)
                        .setLeftWindowEndIndex(1)
                        .setRightWindowEndIndex(1)
                        .addLeftKeyIndices(0)
                        .addRightKeyIndices(0)
                        .addFilterNulls(true)
                        .setJoinType(RegularJoinType.REGULAR_JOIN_TYPE_INNER)
                        .setShiftTimeZone("UTC"));
        var calc = Calc.newBuilder().setInput(join).setPreserveInputEnvelope(true);
        for (int index = 0; index < OUTPUT.getFieldCount(); index++)
            calc.addProjections(Expression.newBuilder()
                    .setInputReference(InputReference.newBuilder().setIndex(index)));
        return NativePlan.newBuilder()
                .setProtocolVersion(3)
                .setRoot(Operator.newBuilder().setPlanNodeId(4).setCalc(calc))
                .build()
                .toByteArray();
    }

    private static Operator edge(int port) {
        return Operator.newBuilder()
                .setPlanNodeId(port + 1)
                .setInput(Input.newBuilder().setInputIndex(port))
                .build();
    }
}
