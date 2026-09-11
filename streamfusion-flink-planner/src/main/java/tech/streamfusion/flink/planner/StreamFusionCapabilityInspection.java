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
import static tech.streamfusion.flink.planner.StreamFusionExecGraphProcessor.bypassBatchExchange;
import static tech.streamfusion.flink.planner.StreamFusionExecGraphProcessor.isSinkBoundary;
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
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecAdaptiveJoin;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecCalc;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecCorrelate;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecExchange;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecExpand;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecHashAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecHashJoin;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecHashWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecLimit;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecNestedLoopJoin;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecOverAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecRank;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecSort;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecSortAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecSortLimit;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecSortMergeJoin;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecSortWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecUnion;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecValues;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecWindowTableFunction;
import org.apache.flink.table.planner.plan.nodes.exec.processor.ProcessorContext;
import org.apache.flink.table.planner.plan.nodes.exec.spec.JoinSpec;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecCalc;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecChangelogNormalize;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecCorrelate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecDeduplicate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecDropUpdateBefore;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecExchange;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecExpand;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecGlobalGroupAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecGlobalWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecGroupAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecGroupWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecIntervalJoin;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecJoin;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecLocalGroupAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecLocalWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecMatch;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecMiniBatchAssigner;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecMultiJoin;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecOverAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecRank;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecSort;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecTemporalJoin;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecTemporalSort;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecUnion;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecValues;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecWatermarkAssigner;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecWindowDeduplicate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecWindowJoin;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecWindowRank;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecWindowTableFunction;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.planner.StreamFusionMatchRecognizePlanner.FixedMatchRecognize;
import tech.streamfusion.flink.planner.StreamFusionMatchRecognizePlanner.ProcessingTimeMatchRecognize;

/** Semantic capability inspection, kept separate from transactional graph replacement. */
final class StreamFusionCapabilityInspection {
    private final StreamFusionRejectionTraversal traversal;

    StreamFusionCapabilityInspection(ProcessorContext context, List<String> rejections) {
        traversal = new StreamFusionRejectionTraversal(
                rejections, (node, path) -> inspectRejections(node, context, path, rejections));
    }

    void collect(ExecNode<?> node, String path) {
        traversal.visit(node, path);
    }

    private void collectRejections(ExecNode<?> node, ProcessorContext context, String path, List<String> rejections) {
        traversal.visit(node, path);
    }

    private void inspectRejections(ExecNode<?> node, ProcessorContext context, String path, List<String> rejections) {
        String nodePath = path + "/" + node.getClass().getSimpleName();
        if (node instanceof StreamExecValues) {
            String reason = unsupportedReason((StreamExecValues) node, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
        } else if (node instanceof BatchExecValues) {
            String reason = unsupportedReason((BatchExecValues) node, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
        } else if (node.getInputEdges().isEmpty()) {
            return;
        } else if (node instanceof StreamExecCalc) {
            StreamExecCalc calc = (StreamExecCalc) node;
            ProcessingTimeDeduplicate foldedDeduplicate = processingTimeDeduplicate(calc);
            ProcessingTimeOverAggregate folded = foldedDeduplicate == null ? processingTimeOverAggregate(calc) : null;
            String reason = foldedDeduplicate != null
                    ? unsupportedReason(foldedDeduplicate, context)
                    : folded == null ? unsupportedReason(calc, context) : unsupportedReason(folded, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
            if (foldedDeduplicate != null) {
                collectRejections(
                        foldedDeduplicate.inputEdge.getSource(), context, nodePath + "/native-input", rejections);
                return;
            }
            if (folded != null) {
                collectRejections(folded.inputEdge.getSource(), context, nodePath + "/native-input", rejections);
                return;
            }
        } else if (node instanceof BatchExecCalc) {
            String reason = unsupportedReason((BatchExecCalc) node, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
        } else if (node instanceof BatchExecHashJoin) {
            String reason = unsupportedReason((BatchExecHashJoin) node, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
        } else if (node instanceof BatchExecAdaptiveJoin) {
            String reason = unsupportedReason((BatchExecAdaptiveJoin) node, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
        } else if (node instanceof BatchExecSortMergeJoin) {
            String reason = unsupportedReason((BatchExecSortMergeJoin) node, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
        } else if (node instanceof BatchExecNestedLoopJoin) {
            BatchExecNestedLoopJoin join = (BatchExecNestedLoopJoin) node;
            String reason = unsupportedReason(join, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
            for (int index = 0; index < join.getInputEdges().size(); index++) {
                collectRejections(
                        bypassBatchExchange(join.getInputEdges().get(index)).getSource(),
                        context,
                        nodePath + "/native-input[" + index + "]",
                        rejections);
            }
            return;
        } else if (node instanceof BatchExecHashWindowAggregate || node instanceof BatchExecSortWindowAggregate) {
            BatchWindowAggregatePair pair = batchWindowAggregatePair(node);
            if (pair == null) {
                ExecEdge inputEdge = batchOnePhaseWindowInputEdge(node);
                if (inputEdge == null) {
                    rejections.add(nodePath
                            + "\nbounded window aggregate: expected a one-phase final aggregate or "
                            + "LocalWindowAggregate -> Exchange -> final merge WindowAggregate");
                    return;
                }
                String reason = unsupportedBatchOnePhaseWindowReason((ExecNodeBase<?>) node, inputEdge, context);
                if (reason != null) {
                    rejections.add(nodePath + "\n" + reason);
                }
                collectRejections(inputEdge.getSource(), context, nodePath + "/native-input", rejections);
                return;
            }
            String reason = unsupportedReason(pair, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
            collectRejections(pair.inputEdge.getSource(), context, nodePath + "/native-input", rejections);
            return;
        } else if (node instanceof BatchExecHashAggregate) {
            BatchGroupAggregatePair pair = batchGroupAggregatePair(node);
            String reason = pair == null
                    ? unsupportedReason((BatchExecHashAggregate) node, context)
                    : unsupportedReason(pair, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
            if (pair != null) {
                collectRejections(pair.inputEdge.getSource(), context, nodePath + "/native-input", rejections);
                return;
            }
        } else if (node instanceof BatchExecSortAggregate) {
            BatchGroupAggregatePair pair = batchGroupAggregatePair(node);
            if (pair == null) {
                rejections.add(
                        nodePath + "\nbounded sort aggregate: expected a final merge paired with a local aggregate");
                return;
            }
            String reason = unsupportedReason(pair, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
            collectRejections(pair.inputEdge.getSource(), context, nodePath + "/native-input", rejections);
            return;
        } else if (node instanceof BatchExecOverAggregate) {
            BatchExecOverAggregate aggregate = (BatchExecOverAggregate) node;
            BatchExecSort inputSort = boundedOverInputSort(aggregate);
            if (inputSort == null) {
                rejections.add(nodePath + "\nbounded OVER: expected Flink's required BatchExecSort input");
                return;
            }
            String reason = unsupportedReason(aggregate, context);
            if (reason == null) {
                reason = unsupportedReason(inputSort, context);
            }
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
            ExecEdge nativeInput = inputSort.getInputEdges().get(0);
            collectRejections(nativeInput.getSource(), context, nodePath + "/native-input", rejections);
            return;
        } else if (node instanceof BatchExecRank) {
            BatchExecRank rank = (BatchExecRank) node;
            BoundedRankPipeline pipeline = boundedRankPipeline(rank);
            BatchExecSort inputSort = pipeline == null ? boundedRankInputSort(rank) : pipeline.inputSort;
            String reason = unsupportedReason(rank, context);
            if (reason == null && inputSort != null) {
                reason = unsupportedReason(inputSort, context);
            }
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
            if (pipeline != null) {
                collectRejections(pipeline.inputEdge.getSource(), context, nodePath + "/native-input", rejections);
                return;
            }
            if (inputSort != null) {
                collectRejections(
                        inputSort.getInputEdges().get(0).getSource(), context, nodePath + "/native-input", rejections);
                return;
            }
        } else if (node instanceof BatchExecLimit) {
            String reason = unsupportedReason((BatchExecLimit) node, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
        } else if (node instanceof BatchExecSortLimit) {
            String reason = unsupportedReason((BatchExecSortLimit) node, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
        } else if (node instanceof BatchExecSort) {
            String reason = unsupportedReason((BatchExecSort) node, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
        } else if (node instanceof StreamExecCorrelate) {
            String reason = unsupportedReason((StreamExecCorrelate) node, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
        } else if (node instanceof BatchExecCorrelate) {
            String reason = unsupportedReason((BatchExecCorrelate) node, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
        } else if (node instanceof StreamExecChangelogNormalize) {
            String reason = unsupportedReason((StreamExecChangelogNormalize) node, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
        } else if (node instanceof StreamExecDeduplicate) {
            String reason = unsupportedReason((StreamExecDeduplicate) node, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
        } else if (node instanceof StreamExecGlobalGroupAggregate) {
            IncrementalGroupAggregate incremental = incrementalGroupAggregate((StreamExecGlobalGroupAggregate) node);
            if (incremental != null) {
                String reason = unsupportedReason(incremental, context);
                if (reason != null) {
                    rejections.add(nodePath + "\n" + reason);
                }
                collectRejections(incremental.inputEdge.getSource(), context, nodePath + "/native-input", rejections);
                return;
            }
            if (hasIncrementalGroupAggregateChain((StreamExecGlobalGroupAggregate) node)) {
                rejections.add(nodePath
                        + "\nincremental group aggregate: expanded or computed DISTINCT split input "
                        + "requires dedicated native incremental stages");
                return;
            }
            TwoPhaseGroupAggregate twoPhase = twoPhaseGroupAggregate((StreamExecGlobalGroupAggregate) node);
            if (twoPhase == null) {
                rejections.add(nodePath
                        + "\nglobal group aggregate: expected LocalGroupAggregate -> Exchange -> "
                        + "GlobalGroupAggregate");
                return;
            }
            String reason = unsupportedReason(twoPhase, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
            collectRejections(twoPhase.inputEdge.getSource(), context, nodePath + "/native-input", rejections);
            return;
        } else if (node instanceof StreamExecLocalGroupAggregate) {
            rejections.add(
                    nodePath + "\nlocal group aggregate: native acceleration requires its paired global aggregate");
        } else if (node instanceof StreamExecGroupAggregate) {
            String reason = unsupportedReason((StreamExecGroupAggregate) node, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
        } else if (node instanceof StreamExecGroupWindowAggregate) {
            LegacyGroupWindowAggregate legacy = legacyGroupWindowAggregate((StreamExecGroupWindowAggregate) node);
            String reason = unsupportedReason(legacy, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
        } else if (node instanceof StreamExecOverAggregate) {
            String reason = unsupportedReason((StreamExecOverAggregate) node, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
        } else if (node instanceof StreamExecGlobalWindowAggregate) {
            TwoPhaseWindowAggregate twoPhase = twoPhaseWindowAggregate((StreamExecGlobalWindowAggregate) node);
            if (twoPhase == null) {
                rejections.add(nodePath
                        + "\nglobal window aggregate: expected LocalWindowAggregate -> Exchange -> "
                        + "GlobalWindowAggregate so the native stages can preserve Flink's partial-merge contract");
                return;
            }
            String reason = unsupportedReason(twoPhase, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
            collectRejections(twoPhase.inputEdge.getSource(), context, nodePath + "/native-input", rejections);
            return;
        } else if (node instanceof StreamExecLocalWindowAggregate) {
            rejections.add(
                    nodePath + "\nlocal window aggregate: native acceleration requires its paired global aggregate");
        } else if (node instanceof StreamExecWindowAggregate) {
            // Admit the verified single-stage family and inspect every original input.
            // Clock attributes and their exchange edge remain part of the selected graph.
            String reason = unsupportedReason((StreamExecWindowAggregate) node, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
        } else if (node instanceof StreamExecWindowDeduplicate) {
            String reason = unsupportedReason((StreamExecWindowDeduplicate) node, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
        } else if (node instanceof StreamExecRank) {
            String reason = unsupportedReason((StreamExecRank) node, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
        } else if (node instanceof StreamExecWindowRank) {
            String reason = unsupportedReason((StreamExecWindowRank) node, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
        } else if (node instanceof StreamExecWindowJoin) {
            String reason = unsupportedReason((StreamExecWindowJoin) node, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
        } else if (node instanceof org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecLookupJoin) {
            String reason = StreamFusionLookupJoinSupport.unsupportedReason(
                    (org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecLookupJoin) node);
            if (reason != null) rejections.add(nodePath + "\n" + reason);
        } else if (node instanceof StreamExecTemporalJoin) {
            String reason = unsupportedReason((StreamExecTemporalJoin) node, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
        } else if (node instanceof StreamExecIntervalJoin) {
            String reason = unsupportedReason((StreamExecIntervalJoin) node, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
        } else if (node instanceof StreamExecMultiJoin) {
            StreamExecMultiJoin multiJoin = (StreamExecMultiJoin) node;
            JoinSpec binaryJoin = binaryMultiJoinSpec(multiJoin);
            String reason = binaryJoin == null
                    ? unsupportedReason(multiJoin, context)
                    : unsupportedReason(binaryJoin, multiJoin, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
        } else if (node instanceof StreamExecJoin) {
            String reason = unsupportedReason((StreamExecJoin) node, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
        } else if (node instanceof StreamExecMatch) {
            ProcessingTimeMatchRecognize folded =
                    StreamFusionMatchRecognizePlanner.processingTimeMatchRecognize((StreamExecMatch) node);
            FixedMatchRecognize match = folded == null
                    ? FixedMatchRecognize.rejected(
                            (StreamExecMatch) node,
                            "processing time: expected Calc(PROCTIME) -> Exchange -> Match physical shape")
                    : folded.match;
            String reason = match.rejectionReason != null
                    ? match.rejectionReason
                    : StreamFusionMatchRecognizePlanner.unsupportedReason(match, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
            if (folded != null && reason == null) {
                String calcReason = unsupportedCalcReason(
                        (RowType) folded.inputEdge.getOutputType(),
                        folded.inputType,
                        folded.inputProjection,
                        folded.inputCondition,
                        context);
                if (calcReason != null) {
                    rejections.add(nodePath + "/native-input-calc\n" + calcReason);
                }
                collectRejections(folded.inputEdge.getSource(), context, nodePath + "/native-input", rejections);
                return;
            }
        } else if (node instanceof StreamExecDropUpdateBefore) {
            // RowKind is Flink changelog metadata, so this node is always eligible.
        } else if (node instanceof StreamExecMiniBatchAssigner) {
            // Native stateful operators already consume Arrow mini-batches. The latency-marker
            // assigner is folded into the native stateful node during conversion.
        } else if (node instanceof StreamExecTemporalSort) {
            String reason = unsupportedReason((StreamExecTemporalSort) node, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
        } else if (node instanceof StreamExecSort) {
            String reason = unsupportedReason((StreamExecSort) node, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
        } else if (node instanceof StreamExecWatermarkAssigner) {
            // The distinct node retains Flink's generated expression and watermark state machine.
            // Flink's row representation tolerates precision changes; Arrow may change time units.
            if (!node.getInputEdges().get(0).getOutputType().equals(node.getOutputType())) {
                rejections.add(nodePath + "\nArrow watermark assignment requires unchanged input field types; "
                        + "timestamp precision conversion is not supported");
            }
        } else if (node instanceof StreamExecExpand) {
            String reason = unsupportedReason((StreamExecExpand) node, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
        } else if (node instanceof BatchExecExpand) {
            String reason = unsupportedReason((BatchExecExpand) node, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
        } else if (node instanceof StreamExecExchange) {
            StreamExecExchange exchange = (StreamExecExchange) node;
            String reason = StreamFusionExchangeSupport.unsupportedReason(
                    (RowType) exchange.getOutputType(),
                    exchange.getInputProperties().get(0).getRequiredDistribution());
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
        } else if (node instanceof BatchExecExchange) {
            BatchExecExchange exchange = (BatchExecExchange) node;
            String reason = StreamFusionExchangeSupport.unsupportedReason(
                    (RowType) exchange.getOutputType(),
                    exchange.getInputProperties().get(0).getRequiredDistribution());
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
        } else if (node instanceof StreamExecUnion) {
            String reason = unsupportedReason((StreamExecUnion) node, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
        } else if (node instanceof BatchExecUnion) {
            String reason = unsupportedReason((BatchExecUnion) node, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
        } else if (node instanceof StreamExecWindowTableFunction) {
            String reason = unsupportedReason((StreamExecWindowTableFunction) node, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
        } else if (node instanceof BatchExecWindowTableFunction) {
            String reason = unsupportedReason((BatchExecWindowTableFunction) node, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
        } else if (!(node instanceof StreamExecUnion) && !isSinkBoundary(node)) {
            rejections.add(nodePath + "\noperator has no StreamFusion physical implementation");
        }
        for (int index = 0; index < node.getInputEdges().size(); index++) {
            collectRejections(
                    node.getInputEdges().get(index).getSource(),
                    context,
                    nodePath + "/input[" + index + "]",
                    rejections);
        }
    }
}
