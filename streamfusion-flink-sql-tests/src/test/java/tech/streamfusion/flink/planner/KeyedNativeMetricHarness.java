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
    final DataOutputSerializer output = new DataOutputSerializer(128);
    final List<StreamElement> controls = new ArrayList<>();
    final FlinkManagedMemory memory;
    private final RowType outputType;

    KeyedNativeMetricHarness(boolean rocks, byte[] plan, RowType type, List<Long> states) throws Exception {
        super(new StreamFusionNativeRegionOperatorFactory(List.of(type), type, plan, states), 16, 1, 0);
        outputType = type;
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
        setKeySelector(0, ignored -> 0); // One subtask owns every native key group.
        setStateBackend(new StreamFusionStateBackend(
                rocks ? new EmbeddedRocksDBStateBackend(true) : new HashMapStateBackend()));
        setOutputCreator(ignored -> new CollectorOutput<ArrowRowDataBatch>(controls) {
            @Override
            public void collect(StreamRecord<ArrowRowDataBatch> record) {
                var batch = record.getValue();
                for (int row = 0; row < batch.size(); row++) {
                    var value = batch.rowView(row);
                    value.setRowKind(batch.rowKind(row));
                    try {
                        StageEventBytes.row(outputType, value, batch.hasTimestamp(row), batch.timestamp(row), output);
                    } catch (java.io.IOException error) {
                        throw new java.io.UncheckedIOException(error);
                    }
                }
            }
        });
        setup(ArrowRowDataBatchSerializer.INSTANCE);
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
        for (var event : controls) StageEventBytes.encode(outputType, event, output);
        controls.clear();
    }

    @Override
    public void close() throws Exception {
        super.close();
        assertThat(memory.reserved()).isZero();
    }
}
