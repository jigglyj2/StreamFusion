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
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecDropUpdateBefore;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecMatch;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecMiniBatchAssigner;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecWatermarkAssigner;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.planner.StreamFusionMatchRecognizePlanner.FixedMatchRecognize;
import tech.streamfusion.flink.planner.StreamFusionMatchRecognizePlanner.ProcessingTimeMatchRecognize;

/** Constructs native physical nodes for this operator family. */
final class StreamFusionControlConversions {
    private StreamFusionControlConversions() {}

    static ExecNode<?> convert(ExecNode<?> node, StreamFusionExecGraphProcessor context) {
        if (node instanceof StreamExecMatch) {
            StreamExecMatch match = (StreamExecMatch) node;
            ProcessingTimeMatchRecognize folded = StreamFusionMatchRecognizePlanner.processingTimeMatchRecognize(match);
            if (folded == null || folded.match.rejectionReason != null) {
                throw new IllegalStateException("Selected unsupported MATCH_RECOGNIZE physical shape");
            }
            StreamFusionExecCalc inputProjection = new StreamFusionExecCalc(
                    folded.inputCalc.getPersistedConfig(),
                    folded.inputProjection,
                    folded.inputCondition,
                    folded.inputCalc.getInputProperties().get(0),
                    folded.inputType,
                    "StreamFusionCalc");
            inputProjection.setInputEdges(List.of(
                    copyEdge(folded.inputEdge, context.convert(folded.inputEdge.getSource()), inputProjection)));
            StreamFusionExecExchange exchange = new StreamFusionExecExchange(
                    folded.exchange.getPersistedConfig(),
                    folded.exchange.getInputProperties().get(0),
                    folded.inputType,
                    "StreamFusionExchange");
            exchange.setInputEdges(
                    List.of(copyEdge(folded.exchange.getInputEdges().get(0), inputProjection, exchange)));
            FixedMatchRecognize fixed = folded.match;
            StreamFusionExecMatchRecognize replacement = new StreamFusionExecMatchRecognize(
                    match.getPersistedConfig(),
                    fixed.partitionKeys,
                    fixed.variableNames,
                    fixed.conditions,
                    fixed.measureVariables,
                    fixed.measureFields,
                    fixed.skipPastLastRow,
                    match.getInputProperties().get(0),
                    (RowType) match.getOutputType(),
                    "StreamFusionMatchRecognize");
            replacement.setInputEdges(List.of(copyEdge(match.getInputEdges().get(0), exchange, replacement)));
            return replacement;
        }
        if (node instanceof StreamExecDropUpdateBefore) {
            StreamExecDropUpdateBefore drop = (StreamExecDropUpdateBefore) node;
            StreamFusionExecDropUpdateBefore replacement = new StreamFusionExecDropUpdateBefore(
                    drop.getPersistedConfig(),
                    drop.getInputProperties().get(0),
                    (RowType) drop.getOutputType(),
                    "StreamFusionDropUpdateBefore");
            replacement.setInputEdges(drop.getInputEdges().stream()
                    .map(edge -> copyEdge(edge, context.convert(edge.getSource()), replacement))
                    .collect(Collectors.toList()));
            return replacement;
        }
        if (node instanceof StreamExecMiniBatchAssigner) {
            StreamExecMiniBatchAssigner assigner = (StreamExecMiniBatchAssigner) node;
            StreamFusionExecMiniBatchAssigner replacement = new StreamFusionExecMiniBatchAssigner(
                    assigner.getPersistedConfig(),
                    miniBatchInterval(assigner),
                    assigner.getInputProperties().get(0),
                    (RowType) assigner.getOutputType(),
                    "StreamFusionMiniBatchAssigner");
            replacement.setInputEdges(assigner.getInputEdges().stream()
                    .map(edge -> copyEdge(edge, context.convert(edge.getSource()), replacement))
                    .collect(Collectors.toList()));
            return replacement;
        }
        if (node instanceof StreamExecWatermarkAssigner) {
            StreamExecWatermarkAssigner watermark = (StreamExecWatermarkAssigner) node;
            StreamFusionExecWatermarkAssigner replacement = new StreamFusionExecWatermarkAssigner(
                    watermark.getPersistedConfig(),
                    watermarkExpression(watermark),
                    watermarkRowtimeFieldIndex(watermark),
                    watermark.getInputProperties().get(0),
                    (RowType) watermark.getOutputType(),
                    "StreamFusionWatermarkAssigner");
            replacement.setInputEdges(watermark.getInputEdges().stream()
                    .map(edge -> copyEdge(edge, context.convert(edge.getSource()), replacement))
                    .collect(Collectors.toList()));
            return replacement;
        }
        return null;
    }
}
