/*
 * Copyright 2026 StreamFusion Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package tech.streamfusion.flink.planner;

import static tech.streamfusion.flink.planner.FlinkExecNodeAccess.*;
import static tech.streamfusion.flink.planner.StreamFusionAggregateShapes.*;
import static tech.streamfusion.flink.planner.StreamFusionExecGraphProcessor.copyEdge;
import static tech.streamfusion.flink.planner.StreamFusionGroupAggregateSupport.*;
import static tech.streamfusion.flink.planner.StreamFusionJoinSupport.*;
import static tech.streamfusion.flink.planner.StreamFusionOverSupport.*;
import static tech.streamfusion.flink.planner.StreamFusionProcessingTimeShapes.*;
import static tech.streamfusion.flink.planner.StreamFusionRankShapes.*;
import static tech.streamfusion.flink.planner.StreamFusionRankSupport.*;
import static tech.streamfusion.flink.planner.StreamFusionRuntimeClasses.*;
import static tech.streamfusion.flink.planner.StreamFusionStatelessSupport.*;
import static tech.streamfusion.flink.planner.StreamFusionWindowAggregateSupport.*;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecHashWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecSortWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecGlobalWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecGroupWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecLocalWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecWindowAggregate;
import org.apache.flink.table.types.logical.RowType;

/** Constructs native physical nodes for this operator family. */
final class StreamFusionWindowAggregateConversions {
    private StreamFusionWindowAggregateConversions() {}

    static ExecNode<?> convert(ExecNode<?> node, StreamFusionExecGraphProcessor context) {
        if (node instanceof BatchExecHashWindowAggregate || node instanceof BatchExecSortWindowAggregate) {
            BatchWindowAggregatePair pair = batchWindowAggregatePair(node);
            if (pair == null) {
                ExecEdge inputEdge = batchOnePhaseWindowInputEdge(node);
                if (inputEdge == null) {
                    throw new IllegalStateException("Selected malformed bounded window aggregate");
                }
                ExecNodeBase<?> aggregate = (ExecNodeBase<?>) node;
                StreamFusionBatchExecWindowAggregate replacement = new StreamFusionBatchExecWindowAggregate(
                        aggregate.getPersistedConfig(),
                        batchWindowGrouping(aggregate),
                        batchWindowAggregateCalls(aggregate),
                        batchWindow(aggregate),
                        batchWindowProperties(aggregate),
                        aggregate.getInputProperties().get(0),
                        (RowType) aggregate.getOutputType());
                replacement.setInputEdges(
                        List.of(copyEdge(inputEdge, context.convert(inputEdge.getSource()), replacement)));
                return replacement;
            }
            return context.convertBatchWindowAggregatePair(pair);
        }
        if (node instanceof StreamExecGroupWindowAggregate) {
            StreamExecGroupWindowAggregate aggregate = (StreamExecGroupWindowAggregate) node;
            LegacyGroupWindowAggregate legacy = legacyGroupWindowAggregate(aggregate);
            StreamFusionExecGroupWindowAggregate replacement = new StreamFusionExecGroupWindowAggregate(
                    context.tableConfig() == null ? aggregate.getPersistedConfig() : context.tableConfig(),
                    legacy.grouping,
                    legacy.aggregateCalls,
                    legacy.window,
                    legacy.properties,
                    legacy.needRetraction,
                    aggregate.getInputProperties().get(0),
                    (RowType) aggregate.getOutputType(),
                    "StreamFusionGroupWindowAggregate");
            replacement.setInputEdges(aggregate.getInputEdges().stream()
                    .map(edge -> copyEdge(edge, context.convert(edge.getSource()), replacement))
                    .collect(Collectors.toList()));
            return replacement;
        }
        if (node instanceof StreamExecGlobalWindowAggregate) {
            TwoPhaseWindowAggregate twoPhase = twoPhaseWindowAggregate((StreamExecGlobalWindowAggregate) node);
            if (twoPhase == null) {
                throw new IllegalStateException("Selected malformed two-phase window aggregate");
            }
            StreamExecGlobalWindowAggregate global = twoPhase.global;
            StreamExecLocalWindowAggregate local = twoPhase.local;
            int[] grouping = localWindowGrouping(local);
            boolean needRetraction = localWindowNeedRetraction(local);
            RowType originalInputType = (RowType) twoPhase.inputEdge.getOutputType();
            RowType internalType = nativeWindowAccumulatorType(originalInputType, grouping);
            ExecNode<?> nativeLocal = context.convertDerived(local, () -> {
                var replacement = new StreamFusionExecLocalWindowAggregate(
                        local.getPersistedConfig(),
                        grouping,
                        localWindowAggregateCalls(local),
                        localWindowing(local),
                        needRetraction,
                        local.getInputProperties().get(0),
                        internalType);
                replacement.setInputEdges(List.of(
                        copyEdge(twoPhase.inputEdge, context.convert(twoPhase.inputEdge.getSource()), replacement)));
                return replacement;
            });
            ExecNode<?> exchange = context.convertDerived(twoPhase.exchange, () -> {
                var replacement = new StreamFusionExecExchange(
                        twoPhase.exchange.getPersistedConfig(),
                        twoPhase.exchange.getInputProperties().get(0),
                        internalType,
                        "StreamFusionExchange");
                replacement.setInputEdges(
                        List.of(copyEdge(twoPhase.exchange.getInputEdges().get(0), nativeLocal, replacement)));
                return replacement;
            });

            StreamFusionExecGlobalWindowAggregate replacement = new StreamFusionExecGlobalWindowAggregate(
                    global.getPersistedConfig(),
                    originalInputType,
                    grouping.length,
                    localWindowAggregateCalls(local),
                    localWindowing(local),
                    globalWindowProperties(global),
                    needRetraction,
                    global.getInputProperties().get(0),
                    (RowType) global.getOutputType());
            replacement.setInputEdges(List.of(copyEdge(global.getInputEdges().get(0), exchange, replacement)));
            return replacement;
        }
        if (node instanceof StreamExecWindowAggregate) {
            StreamExecWindowAggregate aggregate = (StreamExecWindowAggregate) node;
            ProcessingTimeWindowAggregate folded = processingTimeWindowAggregate(aggregate);
            if (folded != null) {
                StreamFusionExecWindowAggregate replacement = new StreamFusionExecWindowAggregate(
                        aggregate.getPersistedConfig(),
                        folded.grouping,
                        folded.aggregateCalls,
                        folded.windowing,
                        windowProperties(aggregate),
                        windowNeedRetraction(aggregate),
                        folded.inputProperty,
                        (RowType) aggregate.getOutputType(),
                        "StreamFusionWindowAggregate");
                replacement.setInputEdges(List.of(
                        copyEdge(folded.inputEdge, context.convert(folded.inputEdge.getSource()), replacement)));
                return replacement;
            }
            StreamFusionExecWindowAggregate replacement = new StreamFusionExecWindowAggregate(
                    aggregate.getPersistedConfig(),
                    windowGrouping(aggregate),
                    windowAggregateCalls(aggregate),
                    windowing(aggregate),
                    windowProperties(aggregate),
                    windowNeedRetraction(aggregate),
                    aggregate.getInputProperties().get(0),
                    (RowType) aggregate.getOutputType(),
                    "StreamFusionWindowAggregate");
            replacement.setInputEdges(aggregate.getInputEdges().stream()
                    .map(edge -> copyEdge(edge, context.convert(edge.getSource()), replacement))
                    .collect(Collectors.toList()));
            return replacement;
        }
        return null;
    }
}
