/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.exchange;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.binary.BinarySegmentUtils;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import tech.streamfusion.flink.TestingNativeMemoryManager;
import tech.streamfusion.flink.arrow.ArrowExchangeCDataBridge;
import tech.streamfusion.flink.arrow.ArrowExchangeInputCDataBridge;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.nativebridge.NativeExchangeRouter;
import tech.streamfusion.proto.plan.v1.NativeExchangePlan;

/** Compares native composite routing keys and complete changelogs with Flink's serializers. */
final class ExchangeKeyParityFixture {
    private ExchangeKeyParityFixture() {}

    static void verify(RowType type, List<RowData> rows, String description) throws Exception {
        var selector = KeySelectorUtil.getRowDataSelector(
                ExchangeKeyParityFixture.class.getClassLoader(), new int[] {1, 0}, InternalTypeInfo.of(type));
        var serializer = new RowDataSerializer(tech.streamfusion.flink.TestingPhysicalRowType.physicalRowType(type));
        var expected = new ArrayList<byte[]>();
        var groups = new ArrayList<Integer>();
        var changelog = new ArrayList<byte[]>();
        var kinds = new RowKind[rows.size()];
        var hasTimestamps = new boolean[rows.size()];
        var timestamps = new long[rows.size()];
        for (int row = 0; row < rows.size(); row++) {
            kinds[row] = RowKind.values()[row % 4];
            rows.get(row).setRowKind(kinds[row]);
            // Only the Flink oracle needs physical materialization. Feed the original generic
            // values to Arrow so noncanonical NaNs and nested offsets still exercise native encoding.
            var input = serializer.toBinaryRow(rows.get(row)).copy();
            hasTimestamps[row] = row % 2 == 0;
            timestamps[row] = -1000L + row;
            var encoded = new DataOutputSerializer(128);
            serializer.serialize(input, encoded);
            changelog.add(encoded.getCopyOfBuffer());
            var binary = (BinaryRowData) selector.getKey(input);
            expected.add(
                    BinarySegmentUtils.copyToBytes(binary.getSegments(), binary.getOffset(), binary.getSizeInBytes()));
            groups.add(KeyGroupRangeAssignment.assignToKeyGroup(binary, 128));
        }
        byte[] plan = NativeExchangePlanSerializer.hash(type, new int[] {1, 0}, 128, 4, true, true);
        var decodedPlan = NativeExchangePlan.parseFrom(plan);
        assertThat(decodedPlan.getProtocolVersion()).isEqualTo(2);
        assertThat(decodedPlan.getMetadataColumns().hasRoutingKeyIndex()).isFalse();
        assertThat(NativeExchangePlanSerializer.requiresPreencodedKeys(type, new int[] {1, 0}))
                .isFalse();
        var memory = TestingNativeMemoryManager.create();
        int count = 0;
        try (var allocator = new RootAllocator();
                var original = ArrowRowDataBatch.transpose(rows, type, allocator)
                        .withEnvelope(kinds, hasTimestamps, timestamps);
                var slice = original.slice(1, rows.size() - 2);
                var envelope = ArrowExchangeBatch.withEnvelope(slice, type);
                var router = new NativeExchangeRouter(plan, memory)) {
            for (var frame : ArrowExchangeCDataBridge.route(router, envelope.batch())) {
                try (var output = ArrowExchangeInputCDataBridge.decode(plan, frame, type, allocator, memory)) {
                    var keys = output.routingKeys();
                    assertThat(keys != null).isEqualTo(decodedPlan.getTransportRoutingKey());
                    for (int row = 0; row < output.size(); row++) {
                        var value = output.rowView(row);
                        int index = value.getInt(0);
                        var encoded = new DataOutputSerializer(128);
                        serializer.serialize(value, encoded);
                        assertThat(encoded.getCopyOfBuffer())
                                .as("%s changelog row %s", description, index)
                                .containsExactly(changelog.get(index));
                        assertThat(output.hasTimestamp(row)).isEqualTo(hasTimestamps[index]);
                        if (hasTimestamps[index])
                            assertThat(output.timestamp(row)).isEqualTo(timestamps[index]);
                        if (keys != null)
                            assertThat(keys.get(row))
                                    .as("%s key row %s", description, index)
                                    .containsExactly(expected.get(index));
                        assertThat(frame.keyGroup()).isEqualTo(groups.get(index));
                        for (int parallelism : new int[] {1, 4, 11, 128})
                            assertThat(frame.keyGroup() * parallelism / 128)
                                    .isEqualTo(KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(
                                            128, parallelism, groups.get(index)));
                        count++;
                    }
                }
            }
        }
        assertThat(count).isEqualTo(rows.size() - 2);
        assertThat(memory.available()).isEqualTo(memory.limit());
    }
}
