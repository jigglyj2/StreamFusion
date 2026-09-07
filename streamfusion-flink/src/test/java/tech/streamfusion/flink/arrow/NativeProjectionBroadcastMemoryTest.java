/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.arrow;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Collections;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;
import tech.streamfusion.nativebridge.NativeExecutionContext;
import tech.streamfusion.nativebridge.NativeMemoryManager;
import tech.streamfusion.proto.plan.v1.Expression;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.StringLiteral;

class NativeProjectionBroadcastMemoryTest extends ArrowCDataBridgeTestSupport {
    @Test
    void sharedPlanRejectsAnOversizedBroadcastBeforeMaterializingIt() throws Exception {
        String value = "é".repeat(32 * 1024);
        var plan = NativePlan.parseFrom(projectionPlan(0, 1)).toBuilder();
        plan.getRootBuilder()
                .getCalcBuilder()
                .setProjections(
                        0,
                        Expression.newBuilder()
                                .setStringLiteral(StringLiteral.newBuilder().setValue(value))
                                .build());
        var memory = new Budget();
        var inputType = RowType.of(new IntType(false));
        var outputType = RowType.of(new VarCharType(false, VarCharType.MAX_LENGTH));
        try (var allocator = new RootAllocator(64L << 20);
                var context = new NativeExecutionContext(plan.build().toByteArray(), memory);
                var warm = ArrowRowDataBatch.transpose(
                        Collections.nCopies(2, GenericRowData.of(1)), inputType, allocator);
                var large = ArrowRowDataBatch.transpose(
                        Collections.nCopies(4096, GenericRowData.of(1)), inputType, allocator)) {
            var execution = new ArrowCDataBridge.ReusableExecution(context, outputType, allocator);
            try (var stream = execution.executeStream(warm)) {
                try (var output = stream.nextWithSelection()) {
                    assertThat(output.batch().rowView(0).getString(0).toString())
                            .isEqualTo(value);
                }
                assertThat(stream.nextWithSelection()).isNull();
            }
            try (var stream = execution.executeStream(large)) {
                assertThatThrownBy(stream::nextWithSelection)
                        .hasRootCauseInstanceOf(java.io.IOException.class)
                        .hasStackTraceContaining("native projection scalar broadcast requested")
                        .hasStackTraceContaining("Flink assigned 67108864 bytes");
            }
        }
        assertThat(memory.reserved).isZero();
    }

    private static final class Budget implements NativeMemoryManager {
        private long reserved;

        public synchronized boolean tryReserve(long bytes) {
            if (bytes > limit() - reserved) {
                return false;
            }
            reserved += bytes;
            return true;
        }

        public synchronized void release(long bytes) {
            assertThat(bytes).isBetween(0L, reserved);
            reserved -= bytes;
        }

        public long limit() {
            return 64L << 20;
        }

        public synchronized long available() {
            return limit() - reserved;
        }
    }
}
