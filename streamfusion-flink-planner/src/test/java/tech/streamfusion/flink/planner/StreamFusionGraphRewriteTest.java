/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecDropUpdateBefore;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

class StreamFusionGraphRewriteTest {
    @Test
    void laterRootFailureLeavesEarlierRootUntouched() {
        var source = node("source");
        var sink = node("sink");
        var replacement = node("native source");
        ExecEdge original = edge(source, sink);
        sink.setInputEdges(List.of(original));
        StreamFusionGraphRewrite rewrite = new StreamFusionGraphRewrite();

        rewrite.convert(sink, node -> {
            rewrite.replaceInputEdge(node, 0, edge(replacement, sink));
            return node;
        });
        assertThatThrownBy(() -> rewrite.convert(node("second root"), node -> {
                    throw new IllegalStateException("second root cannot be replaced");
                }))
                .isInstanceOf(IllegalStateException.class);

        assertThat(sink.getInputEdges()).containsExactly(original);
    }

    @Test
    void commitsOnlyAfterEveryRootIsConstructedAndPreservesSharedNodes() {
        var shared = node("shared");
        var replacement = node("replacement");
        var sink = node("sink");
        sink.setInputEdges(List.of(edge(shared, sink)));
        StreamFusionGraphRewrite rewrite = new StreamFusionGraphRewrite();
        AtomicInteger conversions = new AtomicInteger();
        for (int root = 0; root < 2; root++) {
            assertThat(rewrite.convert(shared, node -> {
                        conversions.incrementAndGet();
                        return replacement;
                    }))
                    .isSameAs(replacement);
        }
        rewrite.replaceInputEdge(sink, 0, edge(replacement, sink));
        assertThat(sink.getInputEdges().get(0).getSource()).isSameAs(shared);
        rewrite.commit();
        assertThat(sink.getInputEdges().get(0).getSource()).isSameAs(replacement);
        assertThat(conversions).hasValue(1);
    }

    private static StreamExecDropUpdateBefore node(String name) {
        var node = new StreamExecDropUpdateBefore(
                new Configuration(), InputProperty.DEFAULT, RowType.of(new IntType()), name);
        node.setInputEdges(List.of());
        return node;
    }

    private static ExecEdge edge(StreamExecDropUpdateBefore source, StreamExecDropUpdateBefore target) {
        return ExecEdge.builder().source(source).target(target).build();
    }
}
