/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.arrow;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.NullType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.proto.FlinkLogicalTypeProto;
import tech.streamfusion.nativebridge.NativeExecutionContext;
import tech.streamfusion.nativebridge.NativeMemoryManager;
import tech.streamfusion.proto.plan.v1.BooleanLiteral;
import tech.streamfusion.proto.plan.v1.Expression;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.NullLiteral;

class NativeOutputLeaseAccountingTest extends ArrowCDataBridgeTestSupport {
    @Test
    void nullOutputSurvivesStreamAndContextCloseAndReleasesEveryExportOwner() throws Exception {
        var plan = NativePlan.parseFrom(projectionPlan(0, 1)).toBuilder();
        plan.getRootBuilder()
                .getCalcBuilder()
                .setProjections(
                        0,
                        Expression.newBuilder()
                                .setNullLiteral(NullLiteral.newBuilder()
                                        .setType(FlinkLogicalTypeProto.serialize(new NullType()))));
        var memory = new AfterGatherMemory();
        var inputType = RowType.of(new VarCharType(false, VarCharType.MAX_LENGTH));
        var outputType = RowType.of(new NullType());
        try (var allocator = new RootAllocator(64L << 20);
                var context = new NativeExecutionContext(plan.build().toByteArray(), memory);
                var input = ArrowRowDataBatch.transpose(
                        Collections.nCopies(257, GenericRowData.of(StringData.fromString("input"))),
                        inputType,
                        allocator)) {
            var execution = new ArrowCDataBridge.ReusableExecution(context, outputType, allocator);
            try (var stream = execution.executeStream(input)) {
                try (var output = stream.nextWithSelection()) {
                    assertThat(output.batch().size()).isEqualTo(257);
                    assertThat(stream.nextWithSelection()).isNull();
                    stream.close();
                    context.close();
                    // Ordinals were copied to Java and closed; NULL has no native payload.
                    // All producer descriptors must now be gone, even while its Java view lives.
                    assertThat(memory.reserved).isZero();
                    for (int row = 0; row < 257; row++)
                        assertThat(output.batch().rowView(row).isNullAt(0)).isTrue();
                }
            }
            assertThat(memory.reserved).isZero();
            assertThat(memory.denied).isZero();
        }
    }

    @Test
    void allPassFilterAndCStreamEdgeDoNotReserveProducerOwnedPayloadAgain() throws Exception {
        var plan = NativePlan.parseFrom(projectionPlan(0, 1)).toBuilder();
        plan.getRootBuilder()
                .getCalcBuilder()
                .setCondition(Expression.newBuilder()
                        .setBooleanLiteral(BooleanLiteral.newBuilder().setValue(true)));
        var memory = new AfterGatherMemory();
        var type = RowType.of(new VarCharType(false, VarCharType.MAX_LENGTH));
        String value = "é" + "x".repeat(510);
        try (var allocator = new RootAllocator(64L << 20);
                var context = new NativeExecutionContext(plan.build().toByteArray(), memory);
                var input = ArrowRowDataBatch.transpose(
                        Collections.nCopies(4096, GenericRowData.of(StringData.fromString(value))), type, allocator)) {
            var execution = new ArrowCDataBridge.ReusableExecution(context, type, allocator);
            var stream = execution.executeStream(input);
            try (var output = stream.nextWithSelection()) {
                assertThat(memory.admittedGather).isFalse();
                assertThat(memory.denied).isZero();
                assertThat(output.batch().size()).isEqualTo(4096);
                assertThat(stream.nextWithSelection()).isNull();
                stream.close();
                context.close();
                // The output lease may retain its ordinal workspace, but never the >2 MiB
                // producer-owned payload. All remaining credit is released with this output.
                assertThat(memory.reserved).isLessThanOrEqualTo(4096L * Integer.BYTES);
                for (int row : new int[] {0, 2048, 4095}) {
                    assertThat(output.batch().rowView(row).getString(0).toString())
                            .isEqualTo(value);
                }
            } finally {
                stream.close();
            }
            assertThat(memory.reserved).isZero();
        }
    }

    private static final class AfterGatherMemory implements NativeMemoryManager {
        private long reserved;
        private boolean admittedGather;
        private int denied;

        public synchronized boolean tryReserve(long bytes) {
            // Producer-owned input is >2 MiB. An all-pass filter and its export must not
            // reserve another payload-sized owner, even while Java retains the output.
            if (bytes > (1L << 20)) {
                admittedGather = true;
                denied++;
                return false;
            }
            if (bytes > limit() - reserved) {
                denied++;
                return false;
            }
            reserved += bytes;
            return true;
        }

        public synchronized void release(long bytes) {
            assertThat(bytes).isBetween(0L, reserved);
            reserved -= bytes;
        }

        public synchronized long available() {
            return limit() - reserved;
        }

        public long limit() {
            return 64L << 20;
        }
    }
}
