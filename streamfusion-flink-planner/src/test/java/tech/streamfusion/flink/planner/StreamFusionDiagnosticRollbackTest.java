/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecSink;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecUnion;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;

@Isolated("Temporarily changes the planner diagnostic system property")
class StreamFusionDiagnosticRollbackTest {
    @TempDir
    Path directory;

    @Test
    void failedAuditWriteRestoresEveryOriginalSinkEdgeAndAllowsRetry() {
        var first = sink("first");
        var second = sink("second");
        var firstEdges = List.copyOf(first.getInputEdges());
        var secondEdges = List.copyOf(second.getInputEdges());
        var graph = new ExecNodeGraph(List.of(first, second));
        String property = "tech.streamfusion.flink.acceleration-audit-file";
        String original = System.getProperty(property);
        try {
            // A directory cannot be opened as an append-only audit file on any platform.
            System.setProperty(property, directory.toString());
            assertThat(new StreamFusionExecGraphProcessor().process(graph, null))
                    .isSameAs(graph);
            assertThat(StreamFusionPlanningDiagnostics.explain())
                    .contains("Accelerated: no", "replacement-preflight", directory.toString());
            assertThat(first.getInputEdges()).containsExactlyElementsOf(firstEdges);
            assertThat(second.getInputEdges()).containsExactlyElementsOf(secondEdges);

            System.setProperty(property, directory.resolve("audit.txt").toString());
            var selected = new StreamFusionExecGraphProcessor().process(graph, null);
            assertThat(selected).isNotSameAs(graph);
            assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
            for (var sink : List.of(first, second)) {
                assertThat(sink.getInputEdges().get(0).getSource()).isInstanceOf(StreamFusionExecSinkBoundary.class);
            }
        } finally {
            if (original == null) System.clearProperty(property);
            else System.setProperty(property, original);
        }
    }

    private static StreamExecSink sink(String name) {
        var config = new Configuration();
        var type = RowType.of(new IntType(false));
        var left = new StreamExecTableSourceScan(config, null, type, name + " left");
        var right = new StreamExecTableSourceScan(config, null, type, name + " right");
        left.setInputEdges(List.of());
        right.setInputEdges(List.of());
        var union = new StreamExecUnion(config, List.of(InputProperty.DEFAULT, InputProperty.DEFAULT), type, name);
        union.setInputEdges(List.of(
                ExecEdge.builder().source(left).target(union).build(),
                ExecEdge.builder().source(right).target(union).build()));
        var sink = new StreamExecSink(
                config,
                null,
                ChangelogMode.insertOnly(),
                InputProperty.DEFAULT,
                type,
                false,
                null,
                null,
                null,
                name + " sink");
        sink.setInputEdges(List.of(ExecEdge.builder().source(union).target(sink).build()));
        return sink;
    }
}
