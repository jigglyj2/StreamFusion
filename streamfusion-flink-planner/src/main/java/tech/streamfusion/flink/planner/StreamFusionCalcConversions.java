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
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecCalc;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecCalc;
import org.apache.flink.table.types.logical.RowType;

/** Constructs native physical nodes for this operator family. */
final class StreamFusionCalcConversions {
    private StreamFusionCalcConversions() {}

    static ExecNode<?> convert(ExecNode<?> node, StreamFusionExecGraphProcessor context) {
        if (node instanceof StreamExecCalc) {
            StreamExecCalc calc = (StreamExecCalc) node;
            ProcessingTimeDeduplicate foldedDeduplicate = processingTimeDeduplicate(calc);
            if (foldedDeduplicate != null) {
                StreamFusionExecCalc inputProjection = new StreamFusionExecCalc(
                        foldedDeduplicate.inputCalc.getPersistedConfig(),
                        foldedDeduplicate.inputProjection,
                        condition(foldedDeduplicate.inputCalc),
                        foldedDeduplicate.inputCalc.getInputProperties().get(0),
                        foldedDeduplicate.inputType,
                        "StreamFusionCalc");
                inputProjection.setInputEdges(List.of(copyEdge(
                        foldedDeduplicate.inputEdge,
                        context.convert(foldedDeduplicate.inputEdge.getSource()),
                        inputProjection)));

                StreamFusionExecExchange exchange = new StreamFusionExecExchange(
                        foldedDeduplicate.exchange.getPersistedConfig(),
                        foldedDeduplicate.exchange.getInputProperties().get(0),
                        foldedDeduplicate.inputType,
                        "StreamFusionExchange");
                exchange.setInputEdges(List.of(
                        copyEdge(foldedDeduplicate.exchange.getInputEdges().get(0), inputProjection, exchange)));

                StreamFusionExecDeduplicate deduplicate = new StreamFusionExecDeduplicate(
                        foldedDeduplicate.deduplicate.getPersistedConfig(),
                        foldedDeduplicate.uniqueKeys,
                        false,
                        booleanField(foldedDeduplicate.deduplicate, "keepLastRow"),
                        booleanField(foldedDeduplicate.deduplicate, "outputInsertOnly"),
                        booleanField(foldedDeduplicate.deduplicate, "generateUpdateBefore"),
                        stateMetadata(foldedDeduplicate.deduplicate),
                        foldedDeduplicate.deduplicate.getInputProperties().get(0),
                        foldedDeduplicate.deduplicateOutputType,
                        "StreamFusionDeduplicate");
                deduplicate.setInputEdges(List.of(
                        copyEdge(foldedDeduplicate.deduplicate.getInputEdges().get(0), exchange, deduplicate)));

                StreamFusionExecCalc replacement = new StreamFusionExecCalc(
                        calc.getPersistedConfig(),
                        foldedDeduplicate.outputProjection,
                        foldedDeduplicate.outputCondition,
                        calc.getInputProperties().get(0),
                        (RowType) calc.getOutputType(),
                        "StreamFusionCalc");
                replacement.setInputEdges(List.of(ExecEdge.builder()
                        .source(deduplicate)
                        .target(replacement)
                        .shuffle(ExecEdge.FORWARD_SHUFFLE)
                        .build()));
                return replacement;
            }
            ProcessingTimeOverAggregate folded = processingTimeOverAggregate(calc);
            if (folded != null) {
                StreamFusionExecCalc inputProjection = new StreamFusionExecCalc(
                        folded.inputCalc.getPersistedConfig(),
                        folded.inputProjection,
                        condition(folded.inputCalc),
                        folded.inputCalc.getInputProperties().get(0),
                        folded.inputType,
                        "StreamFusionCalc");
                inputProjection.setInputEdges(List.of(
                        copyEdge(folded.inputEdge, context.convert(folded.inputEdge.getSource()), inputProjection)));
                StreamFusionExecOverAggregate over = new StreamFusionExecOverAggregate(
                        folded.aggregate.getPersistedConfig(),
                        folded.overSpec,
                        folded.inputCalc.getInputProperties().get(0),
                        folded.overOutputType,
                        "StreamFusionOverAggregate",
                        true);
                over.setInputEdges(List.of(ExecEdge.builder()
                        .source(inputProjection)
                        .target(over)
                        .shuffle(ExecEdge.FORWARD_SHUFFLE)
                        .build()));
                StreamFusionExecCalc replacement = new StreamFusionExecCalc(
                        calc.getPersistedConfig(),
                        folded.outputProjection,
                        folded.outputCondition,
                        calc.getInputProperties().get(0),
                        (RowType) calc.getOutputType(),
                        "StreamFusionCalc");
                replacement.setInputEdges(List.of(ExecEdge.builder()
                        .source(over)
                        .target(replacement)
                        .shuffle(ExecEdge.FORWARD_SHUFFLE)
                        .build()));
                return replacement;
            }
            StreamFusionExecCalc replacement = new StreamFusionExecCalc(
                    calc.getPersistedConfig(),
                    projection(calc),
                    condition(calc),
                    calc.getInputProperties().get(0),
                    (RowType) calc.getOutputType(),
                    "StreamFusionCalc");
            replacement.setInputEdges(calc.getInputEdges().stream()
                    .map(edge -> copyEdge(edge, context.convert(edge.getSource()), replacement))
                    .collect(Collectors.toList()));
            return replacement;
        }
        if (node instanceof BatchExecCalc) {
            BatchExecCalc calc = (BatchExecCalc) node;
            StreamFusionBatchExecCalc replacement = new StreamFusionBatchExecCalc(
                    calc.getPersistedConfig(),
                    projection(calc),
                    condition(calc),
                    calc.getInputProperties().get(0),
                    (RowType) calc.getOutputType(),
                    "StreamFusionBatchCalc");
            replacement.setInputEdges(calc.getInputEdges().stream()
                    .map(edge -> copyEdge(edge, context.convert(edge.getSource()), replacement))
                    .collect(Collectors.toList()));
            return replacement;
        }
        return null;
    }
}
