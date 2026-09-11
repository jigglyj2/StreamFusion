/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.operator;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.transformations.SideOutputTransformation;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.data.*;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.binary.BinarySegmentUtils;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.TestingNativeMemoryManager;
import tech.streamfusion.flink.TestingPhysicalRowType;
import tech.streamfusion.flink.arrow.ArrowExchangeInputCDataBridge;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.exchange.NativeExchangePlanSerializer;
import tech.streamfusion.flink.exchange.StreamFusionExchangeTranslator;

class NativeRegionAliasFrameTest {
    @Test
    void logicalAliasesFrameAtTheNativeExitWithoutAJavaKeyWriter() throws Exception {
        var type = RowType.of(
                new IntType(false),
                new YearMonthIntervalType(YearMonthIntervalType.YearMonthResolution.YEAR_TO_MONTH),
                new DayTimeIntervalType(DayTimeIntervalType.DayTimeResolution.DAY_TO_SECOND),
                new MultisetType(new VarCharType(false, 100)),
                DistinctType.newBuilder(
                                ObjectIdentifier.of("streamfusion", "test", "numbers"), new ArrayType(new IntType()))
                        .build(),
                StructuredType.newBuilder(ObjectIdentifier.of("streamfusion", "test", "record"))
                        .attributes(List.of(
                                new StructuredType.StructuredAttribute("id", new BigIntType()),
                                new StructuredType.StructuredAttribute("name", new VarCharType())))
                        .comparison(StructuredType.StructuredComparison.EQUALS)
                        .build());
        int[] keys = {1, 2, 3, 4, 5};
        var plan = StreamFusionNativeRegionTranslator.inputPlan(0);
        var exchange = NativeExchangePlanSerializer.hash(type, keys, 128, 4, true, true);
        var tree = StreamFusionNativeRegionTranslator.translateInputs(
                List.of(NativeRegionInputTest.arrowSource()), List.of(type), type, plan);
        var frames = StreamFusionExchangeTranslator.frameForMultiInput(tree, type, exchange);
        assertThat(frames).isInstanceOf(SideOutputTransformation.class);
        assertThat(frames.getInputs()).containsExactly(tree);
        var factory = new StreamFusionNativeRegionOperatorFactory(List.of(type), type, plan);
        int port = factory.bindFrameOutput(0, type, exchange);
        var serializer = new RowDataSerializer(TestingPhysicalRowType.physicalRowType(type));
        var selector = KeySelectorUtil.getRowDataSelector(getClass().getClassLoader(), keys, InternalTypeInfo.of(type));
        var inputRows = new ArrayList<RowData>();
        var expected = new ArrayList<byte[]>();
        var expectedKeys = new ArrayList<byte[]>();
        var groups = new ArrayList<Integer>();
        var kinds = new RowKind[8];
        for (int row = 0; row < 8; row++) {
            var entries = new LinkedHashMap<StringData, Integer>();
            entries.put(StringData.fromString("é"), row + 1);
            entries.put(StringData.fromString("a"), 2);
            var value = row % 3 == 0
                    ? GenericRowData.of(row, null, null, null, null, null)
                    : GenericRowData.of(
                            row,
                            row - 25,
                            -90061007L + row,
                            new GenericMapData(entries),
                            new GenericArrayData(new Integer[] {row, null, -2}),
                            GenericRowData.of((long) row, StringData.fromString("é-" + row)));
            kinds[row] = RowKind.values()[row % 4];
            value.setRowKind(kinds[row]);
            var binary = serializer.toBinaryRow(value).copy();
            inputRows.add(binary);
            var encoded = new DataOutputSerializer(128);
            serializer.serialize(binary, encoded);
            expected.add(encoded.getCopyOfBuffer());
            var key = (BinaryRowData) selector.getKey(binary);
            expectedKeys.add(BinarySegmentUtils.copyToBytes(key.getSegments(), key.getOffset(), key.getSizeInBytes()));
            groups.add(KeyGroupRangeAssignment.assignToKeyGroup(key, 128));
        }
        var memory = TestingNativeMemoryManager.create();
        try (var allocator = new RootAllocator(64L << 20);
                var harness = new NativeRegionTestHarness(factory, List.of(type));
                var input = ArrowRowDataBatch.transpose(inputRows, type, allocator)
                        .withEnvelope(kinds, new boolean[8], new long[8])) {
            harness.open();
            harness.accept(0, input);
            assertThat(harness.rows).isEmpty();
            int count = 0;
            for (var frame : harness.frameOutputs.get(NativeRegionExchangeOutputs.tag(port))) {
                try (var decoded = ArrowExchangeInputCDataBridge.decode(exchange, frame, type, allocator, memory)) {
                    for (int row = 0; row < decoded.size(); row++) {
                        var value = decoded.rowView(row);
                        int index = value.getInt(0);
                        var encoded = new DataOutputSerializer(128);
                        serializer.serialize(value, encoded);
                        assertThat(encoded.getCopyOfBuffer()).containsExactly(expected.get(index));
                        assertThat(decoded.routingKeys().get(row)).containsExactly(expectedKeys.get(index));
                        assertThat(frame.keyGroup()).isEqualTo(groups.get(index));
                        count++;
                    }
                }
            }
            assertThat(count).isEqualTo(8);
            assertThat(harness.metrics()
                            .getIOMetricGroup()
                            .getNumRecordsOutCounter()
                            .getCount())
                    .isEqualTo(8);
        }
        assertThat(memory.available()).isEqualTo(memory.limit());
    }
}
