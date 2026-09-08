/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static tech.streamfusion.flink.planner.SharedLocalWindowFixture.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.util.InstantiationUtil;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.exchange.NativeExchangePlanSerializer;
import tech.streamfusion.flink.memory.FlinkOperatorMemoryShare;
import tech.streamfusion.flink.operator.StreamFusionNativeRegionOperatorFactory;
import tech.streamfusion.flink.window.NativeLocalWindowResources;
import tech.streamfusion.proto.plan.v1.NativePlan;

/** Actual shared operator startup binds local capacities separately from native allocation allowance. */
class SharedLocalWindowTaskResourcesTest {
    @Test
    void rejectsMissingExtraAndWrongOwnerSharesBeforeTaskStartup() throws Exception {
        assertThatThrownBy(() -> new StreamFusionNativeRegionOperatorFactory(List.of(INPUT), PARTIAL, plan()))
                .hasMessageContaining("memory shares must match");
        assertThatThrownBy(() -> new NativeLocalWindowResources(Map.of(4L, share())).validate(plan()))
                .hasMessageContaining("memory shares must match");
        assertThatThrownBy(() -> new NativeLocalWindowResources(Map.of(3L, share(), 4L, share())).validate(plan()))
                .hasMessageContaining("memory shares must match");
    }

    @Test
    void zeroOriginalCapacityFailsWithoutReservingNativeMemory() throws Exception {
        try (var environment = new org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder()
                .setManagedMemorySize(64L << 20)
                .build()) {
            var runtime = new org.apache.flink.streaming.api.graph.StreamConfig(
                    new org.apache.flink.configuration.Configuration());
            runtime.setStateBackendUsesManagedMemory(false);
            runtime.setManagedMemoryFractionOperatorOfUseCase(ManagedMemoryUseCase.OPERATOR, 1.0);
            var resources = new NativeLocalWindowResources(Map.of(
                    3L, new FlinkOperatorMemoryShare(1, Integer.MAX_VALUE, Set.of(ManagedMemoryUseCase.OPERATOR))));
            assertThatThrownBy(() -> resources.resolve(environment, runtime))
                    .hasMessageContaining("no original local-window buffer memory");
            assertThat(environment.getMemoryManager().verifyEmpty()).isTrue();
        }
    }

    @Test
    void taskBoundCapacitiesPreservePressureAndControlChangelogsWithAndWithoutKeyedState() throws Exception {
        for (boolean rocks : List.of(false, true))
            for (boolean global : List.of(false, true)) {
                byte[] bytes = plan();
                if (global) {
                    var window =
                            NativePlan.parseFrom(SharedSlicingWindowFixture.plan())
                                    .getRoot()
                                    .getCalc()
                                    .getInput()
                                    .toBuilder()
                                    .setPlanNodeId(5);
                    window.getWindowAggregateBuilder()
                            .setInput(NativePlan.parseFrom(bytes).getRoot());
                    bytes = NativePlan.newBuilder()
                            .setProtocolVersion(2)
                            .setRoot(SharedSlicingWindowFixture.calc(6, window.build(), 4))
                            .build()
                            .toByteArray();
                }
                var outputType = global ? SharedSlicingWindowFixture.OUTPUT : PARTIAL;
                var factory = new StreamFusionNativeRegionOperatorFactory(
                        List.of(INPUT),
                        outputType,
                        bytes,
                        global ? List.of(5L) : List.of(),
                        List.of(NativeExchangePlanSerializer.singleton(INPUT)),
                        new NativeLocalWindowResources(Map.of(3L, share())));
                // Flink serializes the factory before task startup. The original metadata must survive it.
                factory = InstantiationUtil.clone(factory);
                try (var local = LocalWindowFlinkOracle.create(3L << 20);
                        var keyed = global ? GlobalWindowFlinkOracle.create(rocks, null) : null;
                        var target = new KeyedNativeMetricHarness(rocks, factory, 1, outputType, null, 1, 0);
                        var allocator = new RootAllocator(64L << 20)) {
                    assertThat(target.memory.limit()).isGreaterThan(3L << 20);
                    var expected = new DataOutputSerializer(128);
                    var partialSerializer = new RowDataSerializer(SharedSlicingWindowFixture.FLINK_INPUT);
                    var random = new Random(17);
                    for (int phase = 0; phase < 5; phase++) {
                        int count = phase == 0 ? 180000 : 93;
                        int batchSize = phase == 0 ? 4096 : phase % 2 == 0 ? 7 : 31;
                        for (int start = 0; start < count; start += batchSize) {
                            var rows = new ArrayList<RowData>();
                            for (int row = start; row < Math.min(count, start + batchSize); row++) {
                                var value = GenericRowData.of(
                                        1L,
                                        TimestampData.fromEpochMillis(
                                                phase == 0 ? 1000 : (random.nextInt(17) - 8) * 1000L));
                                rows.add(value);
                                local.processElement(new StreamRecord<>(value, 123));
                            }
                            try (var batch = ArrowRowDataBatch.transpose(rows, INPUT, allocator)) {
                                target.processElement(0, new StreamRecord<>(batch));
                            }
                            drain(local.getOutput(), keyed, expected, partialSerializer);
                            compare(target, keyed, expected, outputType);
                        }
                        if (phase == 0 || phase == 2) {
                            local.prepareSnapshotPreBarrier(phase + 1);
                            drain(local.getOutput(), keyed, expected, partialSerializer);
                            if (keyed != null) keyed.prepareSnapshotPreBarrier(phase + 1);
                            target.region().prepareSnapshotPreBarrier(phase + 1);
                        } else {
                            long watermark = phase == 4 ? Long.MAX_VALUE : phase * 2000L - 1;
                            local.processWatermark(new Watermark(watermark));
                            drain(local.getOutput(), keyed, expected, partialSerializer);
                            target.processWatermark(0, new Watermark(watermark));
                        }
                        compare(target, keyed, expected, outputType);
                    }
                    // Credit is returned when the common operator closes, including buffered rows.
                }
            }
    }

    private static FlinkOperatorMemoryShare share() {
        // Harness slot: 64 MiB. Original local share: 3 MiB. Native runtime: much larger.
        return new FlinkOperatorMemoryShare(3, 64, Set.of(ManagedMemoryUseCase.OPERATOR));
    }

    private static void drain(
            java.util.Queue<Object> events,
            org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> global,
            DataOutputSerializer expected,
            RowDataSerializer partialSerializer)
            throws Exception {
        for (var event : events) {
            if (event instanceof StreamRecord<?>) {
                var row = (RowData) ((StreamRecord<?>) event).getValue();
                if (global != null) global.processElement(new StreamRecord<>(partialSerializer.toBinaryRow(row)));
                else
                    StageEventBytes.row(
                            PARTIAL,
                            GenericRowData.of(
                                    row.getLong(0),
                                    SharedSlicingWindowFixture.count(row.getLong(1)),
                                    row.getLong(2) - 2000,
                                    row.getLong(2)),
                            false,
                            0,
                            expected);
            } else if (global != null && event instanceof Watermark) global.processWatermark((Watermark) event);
            else if (global == null) StageEventBytes.encode(PARTIAL, (StreamElement) event, expected);
        }
        events.clear();
    }

    private static void compare(
            KeyedNativeMetricHarness target,
            org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> global,
            DataOutputSerializer expected,
            org.apache.flink.table.types.logical.RowType output)
            throws Exception {
        if (global != null) {
            for (var event : global.getOutput()) StageEventBytes.encode(output, (StreamElement) event, expected);
            global.getOutput().clear();
        }
        target.drainControls();
        assertThat(target.output.getCopyOfBuffer()).isEqualTo(expected.getCopyOfBuffer());
        target.output.clear();
        expected.clear();
    }
}
