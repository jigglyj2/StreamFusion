/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.operator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.streaming.api.graph.NonChainedOutput;
import org.apache.flink.streaming.api.transformations.SideOutputTransformation;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.exchange.NativeExchangeFrameTypeInfo;
import tech.streamfusion.flink.exchange.NativeExchangePlanSerializer;
import tech.streamfusion.flink.exchange.StreamFusionExchangeTranslator;

class NativeRegionFrameOutputTest {
    @Test
    void topologyFramesBothTreeAndSharedExitsWithoutAJavaWriter() throws Exception {
        var type = SharedNativeRegionRuntimeTest.TYPE;
        var source = NativeRegionInputTest.arrowSource();
        var tree = StreamFusionNativeRegionTranslator.translateInputs(
                List.of(source), List.of(type), type, StreamFusionNativeRegionTranslator.inputPlan(0));
        var frames = StreamFusionExchangeTranslator.frameForMultiInput(
                tree, type, NativeExchangePlanSerializer.singleton(type));
        assertThat(frames).isInstanceOf(SideOutputTransformation.class);
        assertThat(frames.getOutputType()).isSameAs(NativeExchangeFrameTypeInfo.INSTANCE);
        assertThat(frames.getInputs()).containsExactly(tree);
        var outputs = NativeSharedRegionTranslation.translate(
                List.of(source),
                List.of(type),
                List.of(type, SharedNativeRegionRuntimeTest.TEXT),
                SharedNativeRegionRuntimeTest.plan().toByteArray(),
                List.of(),
                org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.getExecutionEnvironment(),
                null);
        var side = StreamFusionExchangeTranslator.frameForMultiInput(
                outputs.get(1),
                SharedNativeRegionRuntimeTest.TEXT,
                NativeExchangePlanSerializer.singleton(SharedNativeRegionRuntimeTest.TEXT));
        assertThat(side).isInstanceOf(SideOutputTransformation.class);
        assertThat(side.getInputs()).containsExactly(outputs.get(0));
        assertThat(side.getTransitivePredecessors()).hasSize(3);
    }

    @Test
    void nestedKeysRouteDirectlyFromNativeOutputAndKeepCanonicalConsumerKeys() throws Exception {
        var type = org.apache.flink.table.types.logical.RowType.of(
                new org.apache.flink.table.types.logical.ArrayType(new org.apache.flink.table.types.logical.IntType()));
        var plan = StreamFusionNativeRegionTranslator.inputPlan(0);
        var exchange = NativeExchangePlanSerializer.hash(type, new int[] {0}, 128, 4, true, true);
        var tree = StreamFusionNativeRegionTranslator.translateInputs(
                List.of(NativeRegionInputTest.arrowSource()), List.of(type), type, plan);
        var frames = StreamFusionExchangeTranslator.frameForMultiInput(tree, type, exchange);
        assertThat(frames).isInstanceOf(SideOutputTransformation.class);
        assertThat(frames.getInputs()).containsExactly(tree);
        var factory = new StreamFusionNativeRegionOperatorFactory(List.of(type), type, plan);
        int output = factory.bindFrameOutput(0, type, exchange);
        var memory = tech.streamfusion.flink.TestingNativeMemoryManager.create();
        try (var allocator = new RootAllocator(64L << 20);
                var harness = new NativeRegionTestHarness(factory, List.of(type));
                var input = ArrowRowDataBatch.transpose(
                        List.of(
                                GenericRowData.of(
                                        new org.apache.flink.table.data.GenericArrayData(new Integer[] {1, null, 3})),
                                GenericRowData.of(new org.apache.flink.table.data.GenericArrayData(new Integer[] {}))),
                        type,
                        allocator)) {
            harness.open();
            harness.accept(0, input);
            assertThat(harness.rows).isEmpty();
            var selector = org.apache.flink.table.planner.plan.utils.KeySelectorUtil.getRowDataSelector(
                    getClass().getClassLoader(),
                    new int[] {0},
                    org.apache.flink.table.runtime.typeutils.InternalTypeInfo.of(type));
            int count = 0;
            for (var frame : harness.frameOutputs.get(NativeRegionExchangeOutputs.tag(output))) {
                try (var decoded = tech.streamfusion.flink.arrow.ArrowExchangeInputCDataBridge.decode(
                        exchange, frame, type, allocator, memory)) {
                    for (int row = 0; row < decoded.size(); row++) {
                        var key = (org.apache.flink.table.data.binary.BinaryRowData)
                                selector.getKey(decoded.rowView(row));
                        assertThat(decoded.routingKeys().get(row))
                                .containsExactly(org.apache.flink.table.data.binary.BinarySegmentUtils.copyToBytes(
                                        key.getSegments(), key.getOffset(), key.getSizeInBytes()));
                        count++;
                    }
                }
            }
            assertThat(count).isEqualTo(2);
            assertThat(harness.metrics()
                            .getIOMetricGroup()
                            .getNumRecordsOutCounter()
                            .getCount())
                    .isEqualTo(2);
        }
        assertThat(memory.available()).isEqualTo(memory.limit());
    }

    @Test
    void framesAndMixedArrowConsumersCountLogicalRecordsOncePerNativeExit() throws Exception {
        var type = SharedNativeRegionRuntimeTest.TYPE;
        for (boolean arrow : new boolean[] {false, true}) {
            var factory = SharedNativeRegionRuntimeTest.factory();
            int first = factory.bindFrameOutput(
                    0, type, NativeExchangePlanSerializer.hash(type, new int[] {0}, 128, 4, true));
            int second = factory.bindFrameOutput(
                    1,
                    SharedNativeRegionRuntimeTest.TEXT,
                    NativeExchangePlanSerializer.singleton(SharedNativeRegionRuntimeTest.TEXT));
            try (var allocator = new RootAllocator(64L << 20);
                    var harness =
                            new NativeRegionTestHarness(factory, List.of(type, SharedNativeRegionRuntimeTest.TEXT));
                    var input = ArrowRowDataBatch.transpose(
                            List.of(
                                    GenericRowData.of(1, StringData.fromString("a")),
                                    GenericRowData.of(2, StringData.fromString("b"))),
                            type,
                            allocator)) {
                if (arrow) {
                    harness.getStreamConfig()
                            .setOperatorNonChainedOutputs(List.of(new NonChainedOutput(
                                    true,
                                    1,
                                    1,
                                    128,
                                    0,
                                    false,
                                    new IntermediateDataSetID(),
                                    null,
                                    new ForwardPartitioner<>(),
                                    ResultPartitionType.PIPELINED_BOUNDED)));
                    harness.getStreamConfig().serializeAllConfigs();
                }
                harness.open();
                harness.accept(0, input);
                assertThat(harness.rows).hasSize(arrow ? 2 : 0);
                assertThat(harness.outputRows.get(1)).isEmpty();
                for (int id : List.of(first, second))
                    assertThat(harness.frameOutputs.get(NativeRegionExchangeOutputs.tag(id)).stream()
                                    .mapToInt(frame -> frame.logicalRowCount())
                                    .sum())
                            .isEqualTo(2);
                assertThat(harness.metrics()
                                .getIOMetricGroup()
                                .getNumRecordsOutCounter()
                                .getCount())
                        .isEqualTo(4);
                for (long id : List.of(11L, 12L, 13L))
                    assertThat(harness.stageMetrics(id)
                                    .getIOMetricGroup()
                                    .getNumRecordsOutCounter()
                                    .getCount())
                            .isEqualTo(2);
                harness.processWatermark(0, new org.apache.flink.streaming.api.watermark.Watermark(123));
                assertThat(harness.controls)
                        .containsExactly(new org.apache.flink.streaming.api.watermark.Watermark(123));
                harness.prepareSnapshotPreBarrier(1);
                harness.snapshot(1, 123);
                harness.end(0);
            }
        }
    }

    @Test
    void failedFrameConsumerCancelsPendingExitsAndCountsLogicalRecords() throws Exception {
        var type = SharedNativeRegionRuntimeTest.TYPE;
        var factory = SharedNativeRegionRuntimeTest.factory();
        factory.bindFrameOutput(0, type, NativeExchangePlanSerializer.singleton(type));
        tech.streamfusion.flink.memory.FlinkManagedMemory memory;
        try (var allocator = new RootAllocator(64L << 20);
                var harness = new NativeRegionTestHarness(factory, List.of(type, SharedNativeRegionRuntimeTest.TEXT));
                var input = ArrowRowDataBatch.transpose(
                        List.of(
                                GenericRowData.of(1, StringData.fromString("a")),
                                GenericRowData.of(2, StringData.fromString("b"))),
                        type,
                        allocator)) {
            harness.open();
            memory = SharedNativeRegionRuntimeTest.memory(harness);
            harness.sinkFailure = new IllegalStateException("frame consumer failed");
            assertThatThrownBy(() -> harness.accept(0, input)).hasMessageContaining("frame consumer failed");
            assertThat(harness.metrics()
                            .getIOMetricGroup()
                            .getNumRecordsOutCounter()
                            .getCount())
                    .isEqualTo(2);
            assertThat(harness.outputRows.get(1)).isEmpty();
            assertThatThrownBy(() -> harness.accept(0, input)).hasMessageContaining("active or failed invocation");
        }
        assertThat(memory.reserved()).isZero();
    }
}
