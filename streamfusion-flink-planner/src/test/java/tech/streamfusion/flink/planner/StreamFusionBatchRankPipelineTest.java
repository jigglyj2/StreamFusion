/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecExchange;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecRank;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecSort;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.exec.spec.SortSpec;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

class StreamFusionBatchRankPipelineTest {
    @Test
    void collapsesFlinksTwoPhaseRankButRetainsItsHashExchange() {
        Configuration configuration = new Configuration();
        RowType inputType = RowType.of(false, new LogicalType[] {new IntType(false), new IntType(false)}, new String[] {
            "partition_value", "order_value"
        });
        RowType outputType = RowType.of(
                false,
                new LogicalType[] {new IntType(false), new IntType(false), new BigIntType(false)},
                new String[] {"partition_value", "order_value", "rank_value"});
        SortSpec sortSpec = SortSpec.builder()
                .addField(0, true, false)
                .addField(1, false, true)
                .build();

        BatchExecTableSourceScan source = new BatchExecTableSourceScan(configuration, null, inputType, "source");
        source.setInputEdges(List.of());
        BatchExecSort localSort = unary(
                new BatchExecSort(configuration, sortSpec, InputProperty.DEFAULT, inputType, "local sort"), source);
        BatchExecRank localRank = unary(
                new BatchExecRank(
                        configuration,
                        new int[] {0},
                        new int[] {1},
                        1,
                        10,
                        false,
                        InputProperty.DEFAULT,
                        inputType,
                        "local rank"),
                localSort);
        InputProperty hashInput = InputProperty.builder()
                .requiredDistribution(InputProperty.hashDistribution(new int[] {0}))
                .build();
        BatchExecExchange hashExchange =
                unary(new BatchExecExchange(configuration, hashInput, inputType, "hash exchange"), localRank);
        BatchExecSort globalSort = unary(
                new BatchExecSort(configuration, sortSpec, InputProperty.DEFAULT, inputType, "global sort"),
                hashExchange);
        BatchExecExchange outerExchange = unary(
                new BatchExecExchange(configuration, InputProperty.DEFAULT, inputType, "local exchange"), globalSort);
        BatchExecRank globalRank = unary(
                new BatchExecRank(
                        configuration,
                        new int[] {0},
                        new int[] {1},
                        2,
                        10,
                        true,
                        InputProperty.DEFAULT,
                        outputType,
                        "global rank"),
                outerExchange);

        ExecNode<?> converted = new StreamFusionExecGraphProcessor().convert(globalRank);

        assertThat(converted).isInstanceOf(StreamFusionBatchExecRank.class);
        ExecNode<?> retainedExchange = converted.getInputEdges().get(0).getSource();
        assertThat(retainedExchange).isInstanceOf(StreamFusionBatchExecExchange.class);
        assertThat(retainedExchange
                        .getInputProperties()
                        .get(0)
                        .getRequiredDistribution()
                        .getType())
                .isEqualTo(InputProperty.DistributionType.HASH);
        assertThat(retainedExchange.getInputEdges().get(0).getSource()).isSameAs(source);
        assertThat(descendants(converted))
                .noneMatch(node -> node instanceof BatchExecRank || node instanceof BatchExecSort);
    }

    private static <T extends ExecNode<?>> T unary(T target, ExecNode<?> source) {
        target.setInputEdges(
                List.of(ExecEdge.builder().source(source).target(target).build()));
        return target;
    }

    private static java.util.stream.Stream<ExecNode<?>> descendants(ExecNode<?> node) {
        return java.util.stream.Stream.concat(
                java.util.stream.Stream.of(node),
                node.getInputEdges().stream().flatMap(edge -> descendants(edge.getSource())));
    }
}
