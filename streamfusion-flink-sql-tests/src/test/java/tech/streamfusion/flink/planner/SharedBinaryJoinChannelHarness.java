/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static tech.streamfusion.flink.planner.SharedBinaryJoinMetricFixture.*;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.checkpoint.channel.SequentialChannelStateReaderImpl;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.io.network.partition.consumer.StreamFusionRecoveredTestChannel;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.runtime.io.recovery.RecordFilterContext;
import org.apache.flink.streaming.runtime.tasks.MultipleInputStreamTask;
import org.apache.flink.streaming.runtime.tasks.StreamMockEnvironment;
import org.apache.flink.streaming.runtime.tasks.StreamTaskMailboxTestHarness;
import org.apache.flink.streaming.runtime.tasks.StreamTaskMailboxTestHarnessBuilder;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.mockito.Mockito;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchSerializer;
import tech.streamfusion.flink.exchange.NativeExchangeFrameKeySelector;
import tech.streamfusion.flink.exchange.NativeExchangeFrameTypeInfo;
import tech.streamfusion.flink.exchange.NativeExchangePlanSerializer;
import tech.streamfusion.flink.operator.StreamFusionNativeRegionOperatorFactory;
import tech.streamfusion.flink.state.StreamFusionStateBackend;

/** Real Flink task/network gates, with the production Arrow-to-RowData sink adapter. */
final class SharedBinaryJoinChannelHarness extends StreamTaskMailboxTestHarnessBuilder<RowData> {
    static final OperatorID REGION = new OperatorID(31, 37);

    private SharedBinaryJoinChannelHarness() {
        super(MultipleInputStreamTask::new, InternalTypeInfo.of(OUTPUT));
        memorySize = 64L << 20;
        bufferSize = 64 << 10;
        modifyGateBuilder(gate -> gate.setCheckpointingDuringRecoveryEnabled(false)
                .setSegmentProvider(StreamFusionRecoveredTestChannel.memorySegments(bufferSize)));
    }

    static byte[] exchange() {
        // StreamMockEnvironment owns exactly one key group; rescaling has a separate harness.
        return NativeExchangePlanSerializer.hash(INPUT, new int[] {0}, 1, 1, true);
    }

    static StreamTaskMailboxTestHarness<RowData> create(boolean rocks, boolean unaligned, TaskStateSnapshot restore)
            throws Exception {
        var fixture = new SharedBinaryJoinMetricFixture(false);
        byte[] exchange = exchange();
        var factory = new StreamFusionNativeRegionOperatorFactory(
                List.of(INPUT, INPUT), OUTPUT, fixture.plan(), List.of(id(0)), List.of(exchange, exchange));
        var builder = new SharedBinaryJoinChannelHarness();
        builder.addInput(NativeExchangeFrameTypeInfo.INSTANCE, 1, new NativeExchangeFrameKeySelector(1));
        builder.addInput(NativeExchangeFrameTypeInfo.INSTANCE, 1, new NativeExchangeFrameKeySelector(1));
        builder.setKeyType(Types.INT);
        builder.addJobConfig(CheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(1));
        builder.addJobConfig(CheckpointingOptions.ENABLE_UNALIGNED, unaligned);
        builder.addJobConfig(CheckpointingOptions.ALIGNED_CHECKPOINT_TIMEOUT, Duration.ZERO);
        builder.modifyStreamConfig(config -> {
            config.setStateBackend(new StreamFusionStateBackend(
                    rocks ? new EmbeddedRocksDBStateBackend(true) : new HashMapStateBackend()));
            config.setManagedMemoryFractionOperatorOfUseCase(ManagedMemoryUseCase.OPERATOR, 1.0);
            config.setManagedMemoryFractionOperatorOfUseCase(ManagedMemoryUseCase.STATE_BACKEND, 1.0);
        });
        var constructor = Class.forName("tech.streamfusion.flink.arrow.ArrowBatchToRowDataOperator")
                .getDeclaredConstructor();
        constructor.setAccessible(true);
        @SuppressWarnings("unchecked")
        var sinkBoundary = (StreamOperator<RowData>) constructor.newInstance();
        builder.setupOperatorChain(REGION, factory)
                .name("streamfusion-native-region")
                .chain(ArrowRowDataBatchSerializer.INSTANCE, new RowDataSerializer(OUTPUT))
                .setOperatorID(new OperatorID(41, 43))
                .setOperatorFactory(SimpleOperatorFactory.of(sinkBoundary))
                .build()
                .finish();
        if (restore != null) builder.setTaskStateSnapshot(1, restore);
        var harness = builder.buildUnrestored();
        var readFinished = new CompletableFuture<Void>();
        if (restore != null) {
            // Flink's TestTaskStateManager returns a no-op reader. Replace only that seam;
            // actual checkpoint registration, handles and all other state behavior stay intact.
            var manager = Mockito.spy(harness.getTaskStateManager());
            Mockito.doReturn(new SequentialChannelStateReaderImpl(restore) {
                        @Override
                        public void readInputData(InputGate[] gates, RecordFilterContext context)
                                throws IOException, InterruptedException {
                            try {
                                super.readInputData(gates, context);
                                readFinished.complete(null);
                            } catch (Exception | Error failure) {
                                readFinished.completeExceptionally(failure);
                                throw failure;
                            }
                        }
                    })
                    .when(manager)
                    .getSequentialChannelStateReader();
            var environmentField = harness.getStreamMockEnvironment().getClass().getDeclaredField("taskStateManager");
            environmentField.setAccessible(true);
            environmentField.set(harness.getStreamMockEnvironment(), manager);
            var harnessField = StreamTaskMailboxTestHarness.class.getDeclaredField("taskStateManager");
            harnessField.setAccessible(true);
            harnessField.set(harness, manager);
        }
        try {
            harness.getStreamTask().restore();
            // StreamMockEnvironment otherwise drops asynchronous channel-reader failures.
            if (restore != null) readFinished.get(15, TimeUnit.SECONDS);
            return harness;
        } catch (Exception | Error failure) {
            try {
                harness.close();
            } catch (Exception cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    @Override
    protected void initializeInputs(StreamMockEnvironment environment) {
        super.initializeInputs(environment);
        if (taskStateSnapshots != null)
            for (var gate : inputGates) StreamFusionRecoveredTestChannel.install(gate.getInputGate());
    }
}
