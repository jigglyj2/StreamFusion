/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.metrics.groups.InternalOperatorMetricGroup;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.CollectorOutput;
import org.apache.flink.streaming.util.KeyedMultiInputStreamOperatorTestHarness;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchSerializer;
import tech.streamfusion.flink.memory.FlinkManagedMemory;
import tech.streamfusion.flink.memory.StreamFusionTaskMemory;
import tech.streamfusion.flink.operator.StreamFusionArrowNativeRegionOperator;
import tech.streamfusion.flink.operator.StreamFusionNativeRegionOperatorFactory;
import tech.streamfusion.flink.state.StreamFusionStateBackend;

/** Common keyed Arrow runtime fixture; SQL families supply only a plan, schemas and state IDs. */
final class KeyedNativeMetricHarness extends KeyedMultiInputStreamOperatorTestHarness<Integer, ArrowRowDataBatch> {
    final DataOutputSerializer output;
    final List<DataOutputSerializer> outputs = new ArrayList<>();
    final List<StreamElement> controls = new ArrayList<>();
    final FlinkManagedMemory memory;
    int maxOutputBatchRows;
    private final List<RowType> outputTypes;

    KeyedNativeMetricHarness(boolean rocks, byte[] plan, RowType type, List<Long> states) throws Exception {
        this(rocks, plan, List.of(type), type, states);
    }

    KeyedNativeMetricHarness(boolean rocks, byte[] plan, List<RowType> inputTypes, RowType type, List<Long> states)
            throws Exception {
        this(
                rocks,
                new StreamFusionNativeRegionOperatorFactory(inputTypes, type, plan, states),
                inputTypes.size(),
                type,
                null,
                1,
                0);
    }

    KeyedNativeMetricHarness(
            boolean rocks,
            StreamFusionNativeRegionOperatorFactory factory,
            int inputCount,
            RowType type,
            org.apache.flink.runtime.checkpoint.OperatorSubtaskState restore,
            int parallelism,
            int subtask)
            throws Exception {
        this(rocks, factory, inputCount, List.of(type), restore, parallelism, subtask);
    }

    KeyedNativeMetricHarness(
            boolean rocks,
            StreamFusionNativeRegionOperatorFactory factory,
            int inputCount,
            List<RowType> types,
            org.apache.flink.runtime.checkpoint.OperatorSubtaskState restore,
            int parallelism,
            int subtask)
            throws Exception {
        super(factory, 16, parallelism, subtask);
        outputTypes = List.copyOf(types);
        for (var ignored : outputTypes) outputs.add(new DataOutputSerializer(128));
        output = outputs.get(0);
        var previous = getEnvironment().getMemoryManager();
        var field = getEnvironment().getClass().getDeclaredField("memManager");
        field.setAccessible(true);
        field.set(
                getEnvironment(),
                org.apache.flink.runtime.memory.MemoryManagerBuilder.newBuilder()
                        .setMemorySize(64L << 20)
                        .build());
        previous.shutdown();
        config.setStateKeySerializer(IntSerializer.INSTANCE);
        // Match production routing: Flink must select the frame's native key group after
        // rescaling. A constant lifecycle anchor can hash outside the assigned subtask range.
        var frameKeys = new tech.streamfusion.flink.exchange.NativeExchangeFrameKeySelector(16);
        for (int port = 0; port < inputCount; port++)
            setKeySelector(
                    port,
                    value -> value instanceof tech.streamfusion.flink.exchange.NativeExchangeFrame
                            ? frameKeys.getKey((tech.streamfusion.flink.exchange.NativeExchangeFrame) value)
                            : 0);
        setStateBackend(new StreamFusionStateBackend(
                rocks ? new EmbeddedRocksDBStateBackend(true) : new HashMapStateBackend()));
        setOutputCreator(ignored -> new CollectorOutput<ArrowRowDataBatch>(controls) {
            @Override
            public void collect(StreamRecord<ArrowRowDataBatch> record) {
                capture(0, record.getValue());
            }

            @Override
            public <X> void collect(org.apache.flink.util.OutputTag<X> tag, StreamRecord<X> record) {
                for (int port = 1; port < outputTypes.size(); port++)
                    if (factory.outputTag(port).equals(tag)) {
                        capture(port, (ArrowRowDataBatch) record.getValue());
                        return;
                    }
                throw new AssertionError("Unknown shared output " + tag);
            }

            private void capture(int port, ArrowRowDataBatch batch) {
                maxOutputBatchRows = Math.max(maxOutputBatchRows, batch.size());
                for (int row = 0; row < batch.size(); row++) {
                    var value = batch.rowView(row);
                    value.setRowKind(batch.rowKind(row));
                    try {
                        StageEventBytes.row(
                                outputTypes.get(port),
                                value,
                                batch.hasTimestamp(row),
                                batch.timestamp(row),
                                outputs.get(port));
                    } catch (java.io.IOException error) {
                        throw new java.io.UncheckedIOException(error);
                    }
                }
            }
        });
        setup(ArrowRowDataBatchSerializer.INSTANCE);
        if (restore != null) initializeState(restore);
        open();
        var memoryField = StreamFusionArrowNativeRegionOperator.class.getDeclaredField("memory");
        memoryField.setAccessible(true);
        memory = (FlinkManagedMemory) ((StreamFusionTaskMemory) memoryField.get(region())).nativeMemoryManager();
    }

    StreamFusionArrowNativeRegionOperator region() {
        return (StreamFusionArrowNativeRegionOperator) operator;
    }

    InternalOperatorMetricGroup stage(long id) throws Exception {
        return (InternalOperatorMetricGroup) SharedAggregateMetricSurfaceTest.stageGroup(region(), id);
    }

    void drainControls() throws Exception {
        for (int port = 0; port < outputs.size(); port++)
            for (var event : controls) StageEventBytes.encode(outputTypes.get(port), event, outputs.get(port));
        controls.clear();
    }

    @Override
    public void close() throws Exception {
        super.close();
        assertThat(memory.reserved()).isZero();
    }
}
