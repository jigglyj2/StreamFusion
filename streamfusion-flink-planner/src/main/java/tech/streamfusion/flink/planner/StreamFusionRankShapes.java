/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import static tech.streamfusion.flink.planner.FlinkExecNodeAccess.*;
import static tech.streamfusion.flink.planner.StreamFusionAggregateShapes.*;
import static tech.streamfusion.flink.planner.StreamFusionExecGraphProcessor.bypassBatchExchange;
import static tech.streamfusion.flink.planner.StreamFusionGroupAggregateSupport.*;
import static tech.streamfusion.flink.planner.StreamFusionJoinSupport.*;
import static tech.streamfusion.flink.planner.StreamFusionOverSupport.*;
import static tech.streamfusion.flink.planner.StreamFusionProcessingTimeShapes.*;
import static tech.streamfusion.flink.planner.StreamFusionRankSupport.*;
import static tech.streamfusion.flink.planner.StreamFusionRuntimeClasses.*;
import static tech.streamfusion.flink.planner.StreamFusionStatelessSupport.*;
import static tech.streamfusion.flink.planner.StreamFusionWindowAggregateSupport.*;

import java.util.Arrays;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecExchange;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecRank;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecSort;

/** StreamFusion RankShapes for native physical planning. */
final class StreamFusionRankShapes {
    static BatchExecSort boundedRankInputSort(BatchExecRank rank) {
        if (rank.getInputEdges().size() != 1) {
            return null;
        }
        ExecNode<?> source = bypassBatchExchange(rank.getInputEdges().get(0)).getSource();
        return source instanceof BatchExecSort ? (BatchExecSort) source : null;
    }

    static BoundedRankPipeline boundedRankPipeline(BatchExecRank rank) {
        BatchExecSort globalSort = boundedRankInputSort(rank);
        if (globalSort == null || globalSort.getInputEdges().size() != 1) {
            return null;
        }
        ExecNode<?> globalSortInput = globalSort.getInputEdges().get(0).getSource();
        if (globalSortInput instanceof BatchExecExchange
                && globalSortInput.getInputEdges().size() == 1) {
            BatchExecExchange redistribution = (BatchExecExchange) globalSortInput;
            if (!isKeyedBoundedRankExchange(redistribution)) {
                return null;
            }
            ExecNode<?> redistributionInput =
                    bypassBatchExchange(redistribution.getInputEdges().get(0)).getSource();
            if (redistributionInput instanceof BatchExecRank) {
                BatchExecRank localRank = (BatchExecRank) redistributionInput;
                BatchExecSort localSort = boundedRankInputSort(localRank);
                if (localSort == null
                        || localSort.getInputEdges().size() != 1
                        || boundedRankStart(localRank) != 1
                        || boundedRankEnd(localRank) < boundedRankEnd(rank)
                        || boundedRankOutputNumber(localRank)
                        || !Arrays.equals(boundedRankPartitionFields(localRank), boundedRankPartitionFields(rank))
                        || !Arrays.equals(boundedRankSortFields(localRank), boundedRankSortFields(rank))) {
                    return null;
                }
                return new BoundedRankPipeline(
                        rank,
                        redistribution,
                        localSort,
                        localSort.getInputEdges().get(0));
            }
        }

        if (rank.getInputEdges().get(0).getSource() instanceof BatchExecExchange) {
            BatchExecExchange exchange =
                    (BatchExecExchange) rank.getInputEdges().get(0).getSource();
            if (exchange.getInputEdges().size() != 1 || !isKeyedBoundedRankExchange(exchange)) {
                return null;
            }
            return new BoundedRankPipeline(
                    rank, exchange, globalSort, globalSort.getInputEdges().get(0));
        }
        return null;
    }

    static boolean isKeyedBoundedRankExchange(BatchExecExchange exchange) {
        InputProperty.DistributionType type =
                exchange.getInputProperties().get(0).getRequiredDistribution().getType();
        return type == InputProperty.DistributionType.HASH || type == InputProperty.DistributionType.SINGLETON;
    }

    static final class BoundedRankPipeline {
        final BatchExecRank globalRank;
        final BatchExecExchange exchange;
        final BatchExecSort inputSort;
        final ExecEdge inputEdge;

        BoundedRankPipeline(
                BatchExecRank globalRank, BatchExecExchange exchange, BatchExecSort inputSort, ExecEdge inputEdge) {
            this.globalRank = globalRank;
            this.exchange = exchange;
            this.inputSort = inputSort;
            this.inputEdge = inputEdge;
        }
    }
}
