/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.operator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.runtime.tasks.StreamOperatorWrapper;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.proto.plan.v1.*;

class NativeRegionEndInputTest {
    @Test
    void flinkWrapperEndsOnlyTheNamedPortForSingleAndMultipleInputRegions() throws Exception {
        var type = RowType.of(new IntType());
        for (int ports : List.of(1, 2)) {
            var union = Union.newBuilder();
            for (int port = 0; port < ports; port++)
                union.addInputs(
                        Operator.newBuilder().setInput(Input.newBuilder().setInputIndex(port)));
            var plan = NativePlan.newBuilder()
                    .setProtocolVersion(3)
                    .setRoot(Operator.newBuilder().setPlanNodeId(11).setUnion(union))
                    .build();
            try (var allocator = new RootAllocator(64L << 20);
                    var batch = ArrowRowDataBatch.transpose(List.of(GenericRowData.of(7)), type, allocator);
                    var harness =
                            new NativeRegionTestHarness(plan.toByteArray(), Collections.nCopies(ports, type), type)) {
                harness.open();
                var constructor = StreamOperatorWrapper.class.getDeclaredConstructor(
                        StreamOperator.class, Optional.class, MailboxExecutor.class, boolean.class);
                constructor.setAccessible(true);
                var wrapper = (StreamOperatorWrapper<?, ?>) constructor.newInstance(
                        harness.region(),
                        Optional.empty(),
                        java.lang.reflect.Proxy.newProxyInstance(
                                MailboxExecutor.class.getClassLoader(),
                                new Class<?>[] {MailboxExecutor.class},
                                (proxy, method, arguments) -> {
                                    throw new AssertionError(
                                            "End-input unexpectedly scheduled mailbox work: " + method);
                                }),
                        ports > 1);
                wrapper.endOperatorInput(1);
                assertThatThrownBy(() -> harness.accept(0, batch)).hasMessageContaining("ended");
                if (ports == 2) {
                    harness.accept(1, batch);
                    assertThat(harness.rows).hasSize(1);
                    wrapper.endOperatorInput(2);
                    assertThatThrownBy(() -> harness.accept(1, batch)).hasMessageContaining("ended");
                }
            }
        }
    }
}
