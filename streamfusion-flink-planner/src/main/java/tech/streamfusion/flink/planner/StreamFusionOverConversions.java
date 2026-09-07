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
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecOverAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecSort;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecOverAggregate;
import org.apache.flink.table.types.logical.RowType;

/** Constructs native physical nodes for this operator family. */
final class StreamFusionOverConversions {
    private StreamFusionOverConversions() {}

    static ExecNode<?> convert(ExecNode<?> node, StreamFusionExecGraphProcessor context) {
        if (node instanceof BatchExecOverAggregate) {
            BatchExecOverAggregate aggregate = (BatchExecOverAggregate) node;
            BatchExecSort inputSort = boundedOverInputSort(aggregate);
            if (inputSort == null || inputSort.getInputEdges().size() != 1) {
                throw new IllegalStateException("Selected malformed bounded OVER pipeline");
            }
            ExecEdge inputEdge = inputSort.getInputEdges().get(0);
            StreamFusionBatchExecOverAggregate replacement = new StreamFusionBatchExecOverAggregate(
                    aggregate.getPersistedConfig(),
                    overSpec(aggregate),
                    inputSort.getInputProperties().get(0),
                    (RowType) aggregate.getOutputType(),
                    "StreamFusionBatchOverAggregate");
            replacement.setInputEdges(
                    List.of(copyEdge(inputEdge, context.convert(inputEdge.getSource()), replacement)));
            return replacement;
        }
        if (node instanceof StreamExecOverAggregate) {
            StreamExecOverAggregate aggregate = (StreamExecOverAggregate) node;
            StreamFusionExecOverAggregate replacement = new StreamFusionExecOverAggregate(
                    aggregate.getPersistedConfig(),
                    overSpec(aggregate),
                    aggregate.getInputProperties().get(0),
                    (RowType) aggregate.getOutputType(),
                    "StreamFusionOverAggregate",
                    false);
            replacement.setInputEdges(aggregate.getInputEdges().stream()
                    .map(edge -> copyEdge(edge, context.convert(edge.getSource()), replacement))
                    .collect(Collectors.toList()));
            return replacement;
        }
        return null;
    }
}
