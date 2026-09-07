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
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecHashAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecSortAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecGlobalGroupAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecGroupAggregate;
import org.apache.flink.table.types.logical.RowType;

/** Constructs native physical nodes for this operator family. */
final class StreamFusionGroupAggregateConversions {
    private StreamFusionGroupAggregateConversions() {}

    static ExecNode<?> convert(ExecNode<?> node, StreamFusionExecGraphProcessor context) {
        if (node instanceof BatchExecHashAggregate || node instanceof BatchExecSortAggregate) {
            BatchGroupAggregatePair pair = batchGroupAggregatePair(node);
            if (pair != null) {
                return context.convertBatchGroupAggregatePair(pair);
            }
        }
        if (node instanceof BatchExecHashAggregate) {
            BatchExecHashAggregate aggregate = (BatchExecHashAggregate) node;
            StreamFusionBatchExecHashAggregate replacement = new StreamFusionBatchExecHashAggregate(
                    aggregate.getPersistedConfig(),
                    grouping(aggregate),
                    aggregateCalls(aggregate),
                    aggregate.getInputProperties().get(0),
                    (RowType) aggregate.getOutputType(),
                    "StreamFusionBatchHashAggregate");
            replacement.setInputEdges(aggregate.getInputEdges().stream()
                    .map(edge -> copyEdge(edge, context.convert(edge.getSource()), replacement))
                    .collect(Collectors.toList()));
            return replacement;
        }
        if (node instanceof StreamExecGlobalGroupAggregate) {
            IncrementalGroupAggregate incremental = incrementalGroupAggregate((StreamExecGlobalGroupAggregate) node);
            if (incremental != null) {
                RowType localInternalType = nativeGroupAccumulatorType(
                        (RowType) incremental.inputEdge.getOutputType(), localGroupGrouping(incremental.local));
                StreamFusionExecLocalGroupAggregate local = new StreamFusionExecLocalGroupAggregate(
                        incremental.local.getPersistedConfig(),
                        localGroupGrouping(incremental.local),
                        localGroupAggregateCalls(incremental.local),
                        localGroupCallNeedRetractions(incremental.local),
                        localGroupNeedRetraction(incremental.local),
                        incremental.local.getInputProperties().get(0),
                        localInternalType);
                context.registerReplacement(incremental.local, local);
                local.setInputEdges(List.of(
                        copyEdge(incremental.inputEdge, context.convert(incremental.inputEdge.getSource()), local)));

                StreamFusionExecExchange partialExchange = new StreamFusionExecExchange(
                        incremental.partialExchange.getPersistedConfig(),
                        incremental.partialExchange.getInputProperties().get(0),
                        localInternalType,
                        "StreamFusionExchange");
                partialExchange.setInputEdges(List.of(
                        copyEdge(incremental.partialExchange.getInputEdges().get(0), local, partialExchange)));

                int[] finalGrouping = incrementalFinalGrouping(incremental.incremental);
                RowType incrementalInternalType = nativeGroupingAccumulatorType(localInternalType, finalGrouping);
                StreamFusionExecIncrementalGroupAggregate nativeIncremental =
                        new StreamFusionExecIncrementalGroupAggregate(
                                incremental.incremental.getPersistedConfig(),
                                incrementalPartialOriginalInputType(incremental.incremental),
                                localGroupGrouping(incremental.local).length,
                                finalGrouping,
                                incrementalOriginalCalls(incremental.incremental),
                                incrementalCallNeedRetractions(incremental.incremental),
                                globalGroupOriginalInputType(incremental.global),
                                globalGroupAggregateCalls(incremental.global),
                                globalGroupCallNeedRetractions(incremental.global),
                                context.miniBatchSize(incremental.incremental),
                                incremental.incremental.getInputProperties().get(0),
                                incrementalInternalType);
                nativeIncremental.setInputEdges(List.of(
                        copyEdge(incremental.incremental.getInputEdges().get(0), partialExchange, nativeIncremental)));

                StreamFusionExecExchange finalExchange = new StreamFusionExecExchange(
                        incremental.finalExchange.getPersistedConfig(),
                        incremental.finalExchange.getInputProperties().get(0),
                        incrementalInternalType,
                        "StreamFusionExchange");
                finalExchange.setInputEdges(List.of(
                        copyEdge(incremental.finalExchange.getInputEdges().get(0), nativeIncremental, finalExchange)));

                StreamFusionExecGlobalGroupAggregate replacement = new StreamFusionExecGlobalGroupAggregate(
                        incremental.global.getPersistedConfig(),
                        globalGroupOriginalInputType(incremental.global),
                        finalGrouping.length,
                        globalGroupAggregateCalls(incremental.global),
                        globalGroupCallNeedRetractions(incremental.global),
                        globalGroupGenerateUpdateBefore(incremental.global),
                        globalGroupNeedRetraction(incremental.global),
                        globalGroupStateMetadata(incremental.global),
                        incremental.global.getInputProperties().get(0),
                        (RowType) incremental.global.getOutputType());
                replacement.setInputEdges(
                        List.of(copyEdge(incremental.global.getInputEdges().get(0), finalExchange, replacement)));
                return replacement;
            }
            TwoPhaseGroupAggregate twoPhase = twoPhaseGroupAggregate((StreamExecGlobalGroupAggregate) node);
            if (twoPhase == null) {
                throw new IllegalStateException("Selected malformed two-phase group aggregate");
            }
            RowType internalType = nativeGroupAccumulatorType(
                    (RowType) twoPhase.inputEdge.getOutputType(), localGroupGrouping(twoPhase.local));
            StreamFusionExecLocalGroupAggregate local = new StreamFusionExecLocalGroupAggregate(
                    twoPhase.local.getPersistedConfig(),
                    localGroupGrouping(twoPhase.local),
                    localGroupAggregateCalls(twoPhase.local),
                    localGroupCallNeedRetractions(twoPhase.local),
                    localGroupNeedRetraction(twoPhase.local),
                    twoPhase.local.getInputProperties().get(0),
                    internalType);
            context.registerReplacement(twoPhase.local, local);
            local.setInputEdges(
                    List.of(copyEdge(twoPhase.inputEdge, context.convert(twoPhase.inputEdge.getSource()), local)));

            StreamFusionExecExchange exchange = new StreamFusionExecExchange(
                    twoPhase.exchange.getPersistedConfig(),
                    twoPhase.exchange.getInputProperties().get(0),
                    internalType,
                    "StreamFusionExchange");
            exchange.setInputEdges(
                    List.of(copyEdge(twoPhase.exchange.getInputEdges().get(0), local, exchange)));

            StreamFusionExecGlobalGroupAggregate global = new StreamFusionExecGlobalGroupAggregate(
                    twoPhase.global.getPersistedConfig(),
                    globalGroupOriginalInputType(twoPhase.global),
                    localGroupGrouping(twoPhase.local).length,
                    globalGroupAggregateCalls(twoPhase.global),
                    globalGroupCallNeedRetractions(twoPhase.global),
                    globalGroupGenerateUpdateBefore(twoPhase.global),
                    globalGroupNeedRetraction(twoPhase.global),
                    globalGroupStateMetadata(twoPhase.global),
                    twoPhase.global.getInputProperties().get(0),
                    (RowType) twoPhase.global.getOutputType());
            global.setInputEdges(
                    List.of(copyEdge(twoPhase.global.getInputEdges().get(0), exchange, global)));
            return global;
        }
        if (node instanceof StreamExecGroupAggregate) {
            StreamExecGroupAggregate aggregate = (StreamExecGroupAggregate) node;
            StreamFusionExecGroupAggregate replacement = new StreamFusionExecGroupAggregate(
                    aggregate.getPersistedConfig(),
                    grouping(aggregate),
                    aggregateCalls(aggregate),
                    aggregateCallNeedRetractions(aggregate),
                    aggregateBooleanField(aggregate, "generateUpdateBefore"),
                    aggregateBooleanField(aggregate, "needRetraction"),
                    aggregateStateMetadata(aggregate),
                    aggregate.getInputProperties().get(0),
                    (RowType) aggregate.getOutputType(),
                    "StreamFusionGroupAggregate");
            replacement.setInputEdges(aggregate.getInputEdges().stream()
                    .map(edge -> copyEdge(edge, context.convert(edge.getSource()), replacement))
                    .collect(Collectors.toList()));
            return replacement;
        }
        return null;
    }
}
