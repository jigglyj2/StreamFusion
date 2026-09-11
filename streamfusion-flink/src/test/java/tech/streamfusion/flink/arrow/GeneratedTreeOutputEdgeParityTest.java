/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.arrow;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static tech.streamfusion.flink.arrow.NativeRegionArrowFixtures.*;

import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.TestingNativeMemoryManager;
import tech.streamfusion.nativebridge.NativeExecutionContext;
import tech.streamfusion.proto.plan.v1.*;

class GeneratedTreeOutputEdgeParityTest {
    private static byte[] tree() throws Exception {
        var root = NativeRegionPlan.parseFrom(plan()).getStages(0).getOperator();
        return NativePlan.newBuilder()
                .setProtocolVersion(3)
                .setRoot(root.toBuilder()
                        .setCalc(root.getCalc().toBuilder()
                                .setInput(Operator.newBuilder()
                                        .setInput(Input.newBuilder().setInputIndex(0)))))
                .build()
                .toByteArray();
    }

    @Test
    void treePortPreservesFlinkBytesEnvelopesZeroCopyAndRepeatedInvocations() throws Exception {
        for (int seed : List.of(3, 19, 71)) {
            var memory = TestingNativeMemoryManager.create();
            try (var allocator = new RootAllocator(64L << 20);
                    var source = input(allocator, seed);
                    var empty = ArrowRowDataBatch.empty(TYPE, allocator);
                    var context = new NativeExecutionContext(tree(), memory)) {
                assertThat(context.hasRegionOutputs()).isFalse();
                var edge = new ArrowNativeRegionBridge(context, List.of(TYPE), allocator);
                var serializer = new RowDataSerializer(TYPE);
                long total = 0;
                for (var input : List.of(empty, source, empty, source)) {
                    int rows = 0;
                    try (var stream = edge.executeStream(List.of(input))) {
                        ArrowNativeRegionOutput.Batch next;
                        while ((next = stream.next()) != null) {
                            try (var output = next) {
                                assertThat(output.port()).isZero();
                                var batch = output.batch();
                                for (int row = 0; row < batch.size(); row++) {
                                    assertThat(serializer
                                                    .toBinaryRow(batch.rowView(row))
                                                    .copy())
                                            .isEqualTo(serializer
                                                    .toBinaryRow(input.rowView(rows + row))
                                                    .copy());
                                    assertThat(batch.rowKind(row)).isEqualTo(input.rowKind(rows + row));
                                    assertThat(batch.hasTimestamp(row)).isEqualTo(input.hasTimestamp(rows + row));
                                    if (batch.hasTimestamp(row))
                                        assertThat(batch.timestamp(row)).isEqualTo(input.timestamp(rows + row));
                                }
                                if (batch.size() > 0)
                                    assertThat(batch.root()
                                                    .getVector(0)
                                                    .getDataBuffer()
                                                    .memoryAddress())
                                            .isEqualTo(input.root()
                                                    .getVector(0)
                                                    .getDataBuffer()
                                                    .memoryAddress());
                                rows += batch.size();
                            }
                        }
                        assertThat(stream.next()).isNull();
                    }
                    assertThat(rows).isEqualTo(input.size());
                    total += rows;
                    assertThat(context.metricSnapshot()).containsExactly(11, total, total, 1, 0, total);
                }
                try (var stream = edge.executeStream(List.of(source))) {
                    source.close();
                    context.close();
                    try (var output = stream.next()) {
                        stream.close();
                        assertThat(output.batch().size()).isEqualTo(37);
                        assertThat(output.batch().rowView(1).getInt(0)).isEqualTo(seed + 1);
                        assertThatThrownBy(stream::next).hasMessageContaining("closed");
                    }
                }
            }
            assertThat(memory.available()).isEqualTo(memory.limit());
        }
    }
}
