/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;
import static tech.streamfusion.flink.planner.SharedExpandMetricFixture.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.arrow.ArrowNativePlanBridge;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.NativeCalcResult;
import tech.streamfusion.nativebridge.NativeExecutionContext;
import tech.streamfusion.proto.plan.v1.Input;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;
import tech.streamfusion.proto.plan.v1.Union;

/** UNION forwards records from both a generated SQL collector and a timestamp-preserving edge. */
class SharedRecordPolicyBranchParityTest {
    @Test
    void unionOfClearingAndPreservingBranchesMatchesFlinkRecordEnvelopes() throws Exception {
        var fixture = new SharedExpandMetricFixture();
        var first = NativePlan.parseFrom(fixture.plan())
                .getRoot()
                .getCalc()
                .getInput()
                .getExpand()
                .getInput();
        var plan = NativePlan.newBuilder()
                .setProtocolVersion(3)
                .setRoot(Operator.newBuilder()
                        .setPlanNodeId(99)
                        .setUnion(Union.newBuilder()
                                .addInputs(first)
                                .addInputs(Operator.newBuilder()
                                        .setInput(Input.newBuilder().setInputIndex(1)))))
                .build()
                .toByteArray();
        for (int size : new int[] {0, 1, 7, 257}) {
            var rows = new ArrayList<GenericRowData>();
            var kinds = new RowKind[size];
            var present = new boolean[size];
            var times = new long[size];
            for (int row = 0; row < size; row++) {
                var value = GenericRowData.of(row % 7 == 0 ? null : row % 5 - 1);
                value.setRowKind(RowKind.values()[row % 4]);
                rows.add(value);
                kinds[row] = value.getRowKind();
                present[row] = row % 3 != 0;
                times[row] = new long[] {Long.MIN_VALUE, -9, 0, Long.MAX_VALUE}[row % 4];
            }
            var expected = new DataOutputSerializer(128);
            try (var oracle = fixture.oracle(0)) {
                for (int row = 0; row < size; row++) {
                    oracle.accept(
                            present[row]
                                    ? new StreamRecord<>(rows.get(row), times[row])
                                    : new StreamRecord<>(rows.get(row)));
                    for (var result : oracle.drain()) encode(result, expected);
                }
            }
            // Flink UNION ALL has no record-changing operator on this second branch.
            for (int row = 0; row < size; row++) encodeRow(rows.get(row), present[row], times[row], expected);
            var actual = new DataOutputSerializer(128);
            var memory = new SharedAggregateRegionParityTest.Memory();
            long available = memory.available();
            try (var allocator = new RootAllocator(64L << 20);
                    var context = new NativeExecutionContext(plan, memory);
                    var left =
                            ArrowRowDataBatch.transpose(rows, TYPE, allocator).withEnvelope(kinds, present, times);
                    var right =
                            ArrowRowDataBatch.transpose(rows, TYPE, allocator).withEnvelope(kinds, present, times)) {
                assertThat(context.requiresInputEnvelope()).isTrue();
                var inputs = List.of(left, right);
                var bridge = new ArrowNativePlanBridge(context, TYPE, allocator);
                try (var stream = bridge.executeStream(inputs)) {
                    NativeCalcResult next;
                    while ((next = stream.nextWithSelection()) != null) {
                        try (var result = next) {
                            var output = result.selectEnvelopeFrom(inputs);
                            for (int row = 0; row < output.size(); row++) {
                                var value = output.rowView(row);
                                value.setRowKind(output.rowKind(row));
                                encodeRow(value, output.hasTimestamp(row), output.timestamp(row), actual);
                            }
                        }
                    }
                }
            }
            assertThat(actual.getCopyOfBuffer()).as("batch size %s", size).containsExactly(expected.getCopyOfBuffer());
            assertThat(memory.available()).isEqualTo(available);
        }
    }
}
