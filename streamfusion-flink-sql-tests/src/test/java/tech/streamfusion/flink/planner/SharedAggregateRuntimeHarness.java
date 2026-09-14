/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.CollectorOutput;
import org.apache.flink.streaming.util.KeyedMultiInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchSerializer;
import tech.streamfusion.flink.memory.FlinkManagedMemory;
import tech.streamfusion.flink.memory.StreamFusionTaskMemory;
import tech.streamfusion.flink.operator.StreamFusionArrowNativeRegionOperator;
import tech.streamfusion.flink.operator.StreamFusionNativeRegionOperatorFactory;
import tech.streamfusion.flink.state.StreamFusionStateBackend;

/** Shared production-region harness; copies borrowed rows only at the test sink. */
final class SharedAggregateRuntimeHarness extends KeyedMultiInputStreamOperatorTestHarness<Integer, ArrowRowDataBatch> {
    final DataOutputSerializer captured = new DataOutputSerializer(128);
    final List<Long> times = new ArrayList<>();
    final List<org.apache.flink.streaming.runtime.streamrecord.StreamElement> controls = new ArrayList<>();
    final List<String> eventOrder = new ArrayList<>();
    final FlinkManagedMemory nativeMemory;
    java.util.function.IntConsumer afterOutput;
    boolean failed;
    final java.util.Map<String, List<String>> changelog = new java.util.HashMap<>();

    SharedAggregateRuntimeHarness(boolean rocks, OperatorSubtaskState restore) throws Exception {
        this(rocks, restore, 1, 0, false);
    }

    SharedAggregateRuntimeHarness(
            boolean rocks, OperatorSubtaskState restore, int parallelism, int subtask, boolean framed)
            throws Exception {
        this(rocks, restore, parallelism, subtask, framed, SharedAggregateRegionParityTest.plan());
    }

    SharedAggregateRuntimeHarness(boolean rocks, OperatorSubtaskState restore, byte[] plan) throws Exception {
        this(rocks, restore, 1, 0, false, plan);
    }

    SharedAggregateRuntimeHarness(boolean rocks, OperatorSubtaskState restore, byte[] plan, RowType inputType)
            throws Exception {
        this(rocks, restore, 1, 0, false, plan, 0, inputType);
    }

    private SharedAggregateRuntimeHarness(
            boolean rocks, OperatorSubtaskState restore, int parallelism, int subtask, boolean framed, byte[] plan)
            throws Exception {
        this(rocks, restore, parallelism, subtask, framed, plan, 0);
    }

    SharedAggregateRuntimeHarness(boolean rocks, byte[] plan, long managedMemoryBytes) throws Exception {
        this(rocks, null, 1, 0, false, plan, managedMemoryBytes);
    }

    private SharedAggregateRuntimeHarness(
            boolean rocks,
            OperatorSubtaskState restore,
            int parallelism,
            int subtask,
            boolean framed,
            byte[] plan,
            long managedMemoryBytes)
            throws Exception {
        this(rocks, restore, parallelism, subtask, framed, plan, managedMemoryBytes, SharedAggregateFlinkOracle.INPUT);
    }

    private SharedAggregateRuntimeHarness(
            boolean rocks,
            OperatorSubtaskState restore,
            int parallelism,
            int subtask,
            boolean framed,
            byte[] plan,
            long managedMemoryBytes,
            RowType inputType)
            throws Exception {
        this(
                rocks,
                restore,
                parallelism,
                subtask,
                framed,
                plan,
                managedMemoryBytes,
                inputType,
                new org.apache.flink.configuration.Configuration());
    }

    SharedAggregateRuntimeHarness(boolean rocks, double write, double high) throws Exception {
        this(
                rocks,
                null,
                1,
                0,
                false,
                SharedAggregateRegionParityTest.plan(),
                0,
                SharedAggregateFlinkOracle.INPUT,
                memoryOptions(write, high));
    }

    static SharedAggregateRuntimeHarness configured(
            boolean rocks, OperatorSubtaskState restore, org.apache.flink.configuration.Configuration options)
            throws Exception {
        return configured(rocks, restore, options, 0);
    }

    static SharedAggregateRuntimeHarness configured(
            boolean rocks,
            OperatorSubtaskState restore,
            org.apache.flink.configuration.Configuration options,
            long managedMemoryBytes)
            throws Exception {
        return new SharedAggregateRuntimeHarness(
                rocks,
                restore,
                1,
                0,
                false,
                SharedAggregateRegionParityTest.plan(),
                managedMemoryBytes,
                SharedAggregateFlinkOracle.INPUT,
                options);
    }

    private static org.apache.flink.configuration.Configuration memoryOptions(double write, double high) {
        var options = new org.apache.flink.configuration.Configuration();
        options.set(org.apache.flink.state.rocksdb.RocksDBOptions.WRITE_BUFFER_RATIO, write);
        options.set(org.apache.flink.state.rocksdb.RocksDBOptions.HIGH_PRIORITY_POOL_RATIO, high);
        return options;
    }

    static SharedAggregateRuntimeHarness configuredBackend(
            org.apache.flink.runtime.state.StateBackend backend, OperatorSubtaskState restore) throws Exception {
        return new SharedAggregateRuntimeHarness(
                true,
                restore,
                1,
                0,
                false,
                SharedAggregateRegionParityTest.plan(),
                16L << 20,
                SharedAggregateFlinkOracle.INPUT,
                new org.apache.flink.configuration.Configuration(),
                backend);
    }

    private SharedAggregateRuntimeHarness(
            boolean rocks,
            OperatorSubtaskState restore,
            int parallelism,
            int subtask,
            boolean framed,
            byte[] plan,
            long managedMemoryBytes,
            RowType inputType,
            org.apache.flink.configuration.Configuration backendOptions)
            throws Exception {
        this(rocks, restore, parallelism, subtask, framed, plan, managedMemoryBytes, inputType, backendOptions, null);
    }

    private SharedAggregateRuntimeHarness(
            boolean rocks,
            OperatorSubtaskState restore,
            int parallelism,
            int subtask,
            boolean framed,
            byte[] plan,
            long managedMemoryBytes,
            RowType inputType,
            org.apache.flink.configuration.Configuration backendOptions,
            org.apache.flink.runtime.state.StateBackend providedBackend)
            throws Exception {
        this(
                rocks,
                restore,
                parallelism,
                subtask,
                framed,
                plan,
                managedMemoryBytes,
                inputType,
                backendOptions,
                providedBackend,
                null);
    }

    static SharedAggregateRuntimeHarness localBackup(
            org.apache.flink.runtime.state.StateBackend backend,
            OperatorSubtaskState restore,
            org.apache.flink.runtime.state.LocalRecoveryConfig local)
            throws Exception {
        return new SharedAggregateRuntimeHarness(
                true,
                restore,
                1,
                0,
                false,
                SharedAggregateRegionParityTest.plan(),
                16L << 20,
                SharedAggregateFlinkOracle.INPUT,
                new org.apache.flink.configuration.Configuration(),
                backend,
                local);
    }

    private SharedAggregateRuntimeHarness(
            boolean rocks,
            OperatorSubtaskState restore,
            int parallelism,
            int subtask,
            boolean framed,
            byte[] plan,
            long managedMemoryBytes,
            RowType inputType,
            org.apache.flink.configuration.Configuration backendOptions,
            org.apache.flink.runtime.state.StateBackend providedBackend,
            org.apache.flink.runtime.state.LocalRecoveryConfig local)
            throws Exception {
        this(
                rocks,
                restore,
                parallelism,
                subtask,
                framed,
                plan,
                managedMemoryBytes,
                inputType,
                backendOptions,
                providedBackend,
                local,
                null);
    }

    static SharedAggregateRuntimeHarness localRecovery(
            org.apache.flink.runtime.state.StateBackend backend,
            OperatorSubtaskState restore,
            OperatorSubtaskState backup,
            org.apache.flink.runtime.state.LocalRecoveryConfig local)
            throws Exception {
        return new SharedAggregateRuntimeHarness(
                true,
                restore,
                1,
                0,
                false,
                SharedAggregateRegionParityTest.plan(),
                16L << 20,
                SharedAggregateFlinkOracle.INPUT,
                new org.apache.flink.configuration.Configuration(),
                backend,
                local,
                backup);
    }

    private SharedAggregateRuntimeHarness(
            boolean rocks,
            OperatorSubtaskState restore,
            int parallelism,
            int subtask,
            boolean framed,
            byte[] plan,
            long managedMemoryBytes,
            RowType inputType,
            org.apache.flink.configuration.Configuration backendOptions,
            org.apache.flink.runtime.state.StateBackend providedBackend,
            org.apache.flink.runtime.state.LocalRecoveryConfig local,
            OperatorSubtaskState localRestore)
            throws Exception {
        super(
                new StreamFusionNativeRegionOperatorFactory(
                        List.of(inputType),
                        SharedAggregateFlinkOracle.OUTPUT,
                        plan,
                        List.of(3L),
                        List.of(
                                framed
                                        ? exchangePlan(parallelism)
                                        : tech.streamfusion.flink.exchange.NativeExchangePlanSerializer.singleton(
                                                inputType))),
                16,
                parallelism,
                subtask);
        SharedAggregateHarnessMemory.configure(getEnvironment(), managedMemoryBytes);
        SharedAggregateHarnessMemory.configureLocalRecovery(taskStateManager, local);
        config.setStateKeySerializer(IntSerializer.INSTANCE);
        var keys = new tech.streamfusion.flink.exchange.NativeExchangeFrameKeySelector(16);
        setKeySelector(
                0,
                (Object batch) -> batch instanceof tech.streamfusion.flink.exchange.NativeExchangeFrame
                        ? keys.getKey((tech.streamfusion.flink.exchange.NativeExchangeFrame) batch)
                        : 0);
        setStateBackend(
                providedBackend != null
                        ? providedBackend
                        : new StreamFusionStateBackend(
                                rocks
                                        ? new EmbeddedRocksDBStateBackend(true)
                                                .configure(
                                                        backendOptions,
                                                        getClass().getClassLoader())
                                        : new HashMapStateBackend(),
                                backendOptions));
        setOutputCreator(ignored -> new CollectorOutput<ArrowRowDataBatch>(controls) {
            @Override
            public void collect(StreamRecord<ArrowRowDataBatch> record) {
                eventOrder.add("rows:" + record.getValue().size());
                var serializer = new RowDataSerializer(SharedAggregateFlinkOracle.OUTPUT);
                var batch = record.getValue();
                for (int i = 0; i < batch.size(); i++) {
                    var row = batch.rowView(i);
                    row.setRowKind(batch.rowKind(i));
                    try {
                        serializer.serialize(row, captured);
                        record(changelog, row, batch.hasTimestamp(i) ? batch.timestamp(i) : null);
                    } catch (java.io.IOException failure) {
                        throw new java.io.UncheckedIOException(failure);
                    }
                    times.add(batch.hasTimestamp(i) ? batch.timestamp(i) : null);
                }
                if (afterOutput != null) afterOutput.accept(batch.size());
            }

            @Override
            public void emitWatermark(org.apache.flink.streaming.api.watermark.Watermark watermark) {
                eventOrder.add("watermark:" + watermark.getTimestamp());
                super.emitWatermark(watermark);
            }
        });
        setup(ArrowRowDataBatchSerializer.INSTANCE);
        if (restore != null) initializeState(restore, localRestore);
        open();
        // Assert the production factory/constructor registered this exact Flink subtask. A
        // Java RocksDB delegate here would hide a second cache behind the native state path.
        assertThat(region().getKeyedStateBackend())
                .isInstanceOf(tech.streamfusion.flink.state.StreamFusionKeyedStateBackend.class)
                .extracting("delegate")
                .isInstanceOf(org.apache.flink.runtime.state.heap.HeapKeyedStateBackend.class);
        var field = StreamFusionArrowNativeRegionOperator.class.getDeclaredField("memory");
        field.setAccessible(true);
        nativeMemory = (FlinkManagedMemory) ((StreamFusionTaskMemory) field.get(region())).nativeMemoryManager();
    }

    StreamFusionArrowNativeRegionOperator region() {
        return (StreamFusionArrowNativeRegionOperator) operator;
    }

    static byte[] exchangePlan(int parallelism) {
        return tech.streamfusion.flink.exchange.NativeExchangePlanSerializer.hash(
                SharedAggregateFlinkOracle.INPUT, new int[] {0}, 16, parallelism, true);
    }

    static void record(java.util.Map<String, List<String>> target, RowData row, Long timestamp)
            throws java.io.IOException {
        var bytes = new DataOutputSerializer(128);
        new RowDataSerializer(SharedAggregateFlinkOracle.OUTPUT).serialize(row, bytes);
        target.computeIfAbsent(row.isNullAt(0) ? null : row.getString(0).toString(), ignored -> new ArrayList<>())
                .add(java.util.Base64.getEncoder().encodeToString(bytes.getCopyOfBuffer()) + ":" + timestamp);
    }

    @Override
    public void close() throws Exception {
        if (failed) {
            // Task failure closes resources without invoking finish on the poisoned execution.
            processingTimeService.shutdownService();
            try {
                operator.close();
            } finally {
                getEnvironment().close();
                var cleanup = org.apache.flink.streaming.util.MockStreamTask.class.getDeclaredMethod("cleanUpInternal");
                cleanup.setAccessible(true);
                cleanup.invoke(mockTask);
            }
        } else super.close();
        assertThat(nativeMemory.reserved()).isZero();
    }
}
