/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import static tech.streamfusion.flink.planner.FlinkExecNodeAccess.*;
import static tech.streamfusion.flink.planner.StreamFusionAggregateShapes.*;
import static tech.streamfusion.flink.planner.StreamFusionGroupAggregateSupport.*;
import static tech.streamfusion.flink.planner.StreamFusionJoinSupport.*;
import static tech.streamfusion.flink.planner.StreamFusionOverSupport.*;
import static tech.streamfusion.flink.planner.StreamFusionRankShapes.*;
import static tech.streamfusion.flink.planner.StreamFusionRankSupport.*;
import static tech.streamfusion.flink.planner.StreamFusionRuntimeClasses.*;
import static tech.streamfusion.flink.planner.StreamFusionStatelessSupport.*;
import static tech.streamfusion.flink.planner.StreamFusionWindowAggregateSupport.*;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.rex.RexWindowBounds;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecExchange;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecOverAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecSort;
import org.apache.flink.table.planner.plan.nodes.exec.spec.OverSpec;
import org.apache.flink.table.planner.plan.nodes.exec.spec.PartitionSpec;
import org.apache.flink.table.planner.plan.nodes.exec.spec.SortSpec;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecCalc;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecDeduplicate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecExchange;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecOverAggregate;
import org.apache.flink.table.types.logical.RowType;

/** StreamFusion ProcessingTimeShapes for native physical planning. */
final class StreamFusionProcessingTimeShapes {
    static ProcessingTimeOverAggregate processingTimeOverAggregate(StreamExecCalc outputCalc) {
        if (outputCalc.getInputEdges().size() != 1
                || !(outputCalc.getInputEdges().get(0).getSource() instanceof StreamExecOverAggregate)) {
            return null;
        }
        StreamExecOverAggregate aggregate =
                (StreamExecOverAggregate) outputCalc.getInputEdges().get(0).getSource();
        OverSpec originalSpec = overSpec(aggregate);
        if (originalSpec.getGroups().size() != 1 || aggregate.getInputEdges().size() != 1) {
            return null;
        }
        OverSpec.GroupSpec originalGroup = originalSpec.getGroups().get(0);
        int[] orderFields = originalGroup.getSort().getFieldIndices();
        if (orderFields.length != 1) {
            return null;
        }
        int timeIndex = orderFields[0];
        ExecEdge aggregateInput = aggregate.getInputEdges().get(0);
        if (!(aggregateInput.getSource() instanceof StreamExecExchange)) {
            return null;
        }
        StreamExecExchange exchange = (StreamExecExchange) aggregateInput.getSource();
        if (exchange.getInputEdges().size() != 1
                || !(exchange.getInputEdges().get(0).getSource() instanceof StreamExecCalc)) {
            return null;
        }
        StreamExecCalc inputCalc =
                (StreamExecCalc) exchange.getInputEdges().get(0).getSource();
        if (inputCalc.getInputEdges().size() != 1) {
            return null;
        }
        List<RexNode> originalInputProjection = projection(inputCalc);
        if (timeIndex < 0
                || timeIndex >= originalInputProjection.size()
                || !isProctimeCall(originalInputProjection.get(timeIndex))) {
            return null;
        }
        List<RexNode> inputProjection = new ArrayList<>(originalInputProjection.size() - 1);
        for (int index = 0; index < originalInputProjection.size(); index++) {
            if (index == timeIndex) {
                continue;
            }
            RexNode expression = originalInputProjection.get(index);
            inputProjection.add(expression);
        }
        if (inputProjection.isEmpty()) {
            return null;
        }

        RemappedExpressions output = remapExpressions(projection(outputCalc), condition(outputCalc), timeIndex);
        if (output == null) {
            return null;
        }
        int[] partition = originalSpec.getPartition().getFieldIndices().clone();
        for (int index = 0; index < partition.length; index++) {
            if (partition[index] == timeIndex) {
                return null;
            }
            if (partition[index] > timeIndex) {
                partition[index]--;
            }
        }
        List<org.apache.calcite.rel.core.AggregateCall> calls =
                new ArrayList<>(originalGroup.getAggCalls().size());
        for (org.apache.calcite.rel.core.AggregateCall call : originalGroup.getAggCalls()) {
            List<Integer> arguments = new ArrayList<>(call.getArgList().size());
            for (int argument : call.getArgList()) {
                if (argument == timeIndex) {
                    return null;
                }
                arguments.add(argument > timeIndex ? argument - 1 : argument);
            }
            org.apache.calcite.rel.core.AggregateCall remapped = call.withArgList(arguments);
            if (call.filterArg == timeIndex) {
                return null;
            }
            if (call.filterArg > timeIndex) {
                remapped = remapped.withFilter(call.filterArg - 1);
            }
            calls.add(remapped);
        }
        SortSpec sort = SortSpec.builder()
                .addField(
                        0,
                        originalGroup.getSort().getAscendingOrders()[0],
                        originalGroup.getSort().getNullsIsLast()[0])
                .build();
        RexWindowBound lowerBound = remapBound(originalGroup.getLowerBound(), timeIndex);
        RexWindowBound upperBound = remapBound(originalGroup.getUpperBound(), timeIndex);
        if (lowerBound == null || upperBound == null) {
            return null;
        }
        OverSpec remappedSpec = new OverSpec(
                new PartitionSpec(partition),
                List.of(new OverSpec.GroupSpec(sort, originalGroup.isRows(), lowerBound, upperBound, calls)),
                originalSpec.getConstants(),
                originalSpec.getOriginalInputFields() - 1);
        RowType inputType = withoutField((RowType) inputCalc.getOutputType(), timeIndex);
        RowType overOutputType = withoutField((RowType) aggregate.getOutputType(), timeIndex);
        return new ProcessingTimeOverAggregate(
                aggregate,
                inputCalc,
                inputCalc.getInputEdges().get(0),
                inputProjection,
                inputType,
                remappedSpec,
                overOutputType,
                outputCalc,
                output.projection,
                output.condition);
    }

    static ProcessingTimeDeduplicate processingTimeDeduplicate(StreamExecCalc outputCalc) {
        if (outputCalc.getInputEdges().size() != 1
                || !(outputCalc.getInputEdges().get(0).getSource() instanceof StreamExecDeduplicate)) {
            return null;
        }
        StreamExecDeduplicate deduplicate =
                (StreamExecDeduplicate) outputCalc.getInputEdges().get(0).getSource();
        if (booleanField(deduplicate, "isRowtime")
                || deduplicate.getInputEdges().size() != 1) {
            return null;
        }
        ExecEdge deduplicateInput = deduplicate.getInputEdges().get(0);
        if (!(deduplicateInput.getSource() instanceof StreamExecExchange)) {
            return null;
        }
        StreamExecExchange exchange = (StreamExecExchange) deduplicateInput.getSource();
        if (exchange.getInputEdges().size() != 1
                || !(exchange.getInputEdges().get(0).getSource() instanceof StreamExecCalc)) {
            return null;
        }
        StreamExecCalc inputCalc =
                (StreamExecCalc) exchange.getInputEdges().get(0).getSource();
        if (inputCalc.getInputEdges().size() != 1) {
            return null;
        }
        List<RexNode> originalProjection = projection(inputCalc);
        int timeIndex = -1;
        for (int index = 0; index < originalProjection.size(); index++) {
            if (isProctimeCall(originalProjection.get(index))) {
                if (timeIndex >= 0) {
                    return null;
                }
                timeIndex = index;
            }
        }
        if (timeIndex < 0) {
            return null;
        }
        int[] keys = uniqueKeys(deduplicate);
        for (int index = 0; index < keys.length; index++) {
            if (keys[index] == timeIndex) {
                return null;
            }
            if (keys[index] > timeIndex) {
                keys[index]--;
            }
        }
        RemappedExpressions output = remapExpressions(projection(outputCalc), condition(outputCalc), timeIndex);
        if (output == null) {
            return null;
        }
        List<RexNode> inputProjection = new ArrayList<>(originalProjection);
        inputProjection.remove(timeIndex);
        if (inputProjection.isEmpty()) {
            return null;
        }
        return new ProcessingTimeDeduplicate(
                deduplicate,
                exchange,
                inputCalc,
                inputCalc.getInputEdges().get(0),
                inputProjection,
                withoutField((RowType) inputCalc.getOutputType(), timeIndex),
                keys,
                withoutField((RowType) deduplicate.getOutputType(), timeIndex),
                outputCalc,
                output.projection,
                output.condition);
    }

    static RowType withoutField(RowType type, int removed) {
        List<RowType.RowField> fields = new ArrayList<>(type.getFields());
        fields.remove(removed);
        return new RowType(type.isNullable(), fields);
    }

    static RexWindowBound remapBound(RexWindowBound bound, int removed) {
        if (bound.isCurrentRow() || bound.isUnbounded()) {
            return bound;
        }
        RexNode offset = bound.getOffset();
        if (!(offset instanceof RexInputRef)) {
            return null;
        }
        RexInputRef inputRef = (RexInputRef) offset;
        if (inputRef.getIndex() == removed) {
            return null;
        }
        int index = inputRef.getIndex() > removed ? inputRef.getIndex() - 1 : inputRef.getIndex();
        RexInputRef remapped = new RexInputRef(index, inputRef.getType());
        return bound.isPreceding() ? RexWindowBounds.preceding(remapped) : RexWindowBounds.following(remapped);
    }

    static RemappedExpressions remapExpressions(List<RexNode> projection, RexNode condition, int removed) {
        boolean[] observedRemovedField = {false};
        RexShuttle shuttle = new RexShuttle() {
            @Override
            public RexNode visitInputRef(RexInputRef inputRef) {
                if (inputRef.getIndex() == removed) {
                    observedRemovedField[0] = true;
                    return inputRef;
                }
                int index = inputRef.getIndex() > removed ? inputRef.getIndex() - 1 : inputRef.getIndex();
                return index == inputRef.getIndex() ? inputRef : new RexInputRef(index, inputRef.getType());
            }
        };
        List<RexNode> remappedProjection = projection.stream()
                .map(expression -> expression.accept(shuttle))
                .collect(Collectors.toList());
        RexNode remappedCondition = condition == null ? null : condition.accept(shuttle);
        return observedRemovedField[0] ? null : new RemappedExpressions(remappedProjection, remappedCondition);
    }

    static boolean isProctimeCall(RexNode expression) {
        return expression instanceof RexCall
                && ((RexCall) expression).getOperator().getName().equalsIgnoreCase("PROCTIME");
    }

    static final class RemappedExpressions {
        final List<RexNode> projection;
        final RexNode condition;

        RemappedExpressions(List<RexNode> projection, RexNode condition) {
            this.projection = projection;
            this.condition = condition;
        }
    }

    static final class ProcessingTimeOverAggregate {
        final StreamExecOverAggregate aggregate;
        final StreamExecCalc inputCalc;
        final ExecEdge inputEdge;
        final List<RexNode> inputProjection;
        final RowType inputType;
        final OverSpec overSpec;
        final RowType overOutputType;
        final StreamExecCalc outputCalc;
        final List<RexNode> outputProjection;
        final RexNode outputCondition;

        ProcessingTimeOverAggregate(
                StreamExecOverAggregate aggregate,
                StreamExecCalc inputCalc,
                ExecEdge inputEdge,
                List<RexNode> inputProjection,
                RowType inputType,
                OverSpec overSpec,
                RowType overOutputType,
                StreamExecCalc outputCalc,
                List<RexNode> outputProjection,
                RexNode outputCondition) {
            this.aggregate = aggregate;
            this.inputCalc = inputCalc;
            this.inputEdge = inputEdge;
            this.inputProjection = inputProjection;
            this.inputType = inputType;
            this.overSpec = overSpec;
            this.overOutputType = overOutputType;
            this.outputCalc = outputCalc;
            this.outputProjection = outputProjection;
            this.outputCondition = outputCondition;
        }
    }

    static final class ProcessingTimeDeduplicate {
        final StreamExecDeduplicate deduplicate;
        final StreamExecExchange exchange;
        final StreamExecCalc inputCalc;
        final ExecEdge inputEdge;
        final List<RexNode> inputProjection;
        final RowType inputType;
        final int[] uniqueKeys;
        final RowType deduplicateOutputType;
        final StreamExecCalc outputCalc;
        final List<RexNode> outputProjection;
        final RexNode outputCondition;

        ProcessingTimeDeduplicate(
                StreamExecDeduplicate deduplicate,
                StreamExecExchange exchange,
                StreamExecCalc inputCalc,
                ExecEdge inputEdge,
                List<RexNode> inputProjection,
                RowType inputType,
                int[] uniqueKeys,
                RowType deduplicateOutputType,
                StreamExecCalc outputCalc,
                List<RexNode> outputProjection,
                RexNode outputCondition) {
            this.deduplicate = deduplicate;
            this.exchange = exchange;
            this.inputCalc = inputCalc;
            this.inputEdge = inputEdge;
            this.inputProjection = inputProjection;
            this.inputType = inputType;
            this.uniqueKeys = uniqueKeys;
            this.deduplicateOutputType = deduplicateOutputType;
            this.outputCalc = outputCalc;
            this.outputProjection = outputProjection;
            this.outputCondition = outputCondition;
        }
    }

    static BatchExecSort boundedOverInputSort(BatchExecOverAggregate aggregate) {
        if (aggregate.getInputEdges().size() != 1) {
            return null;
        }
        ExecNode<?> input = aggregate.getInputEdges().get(0).getSource();
        while (input instanceof BatchExecExchange && input.getInputEdges().size() == 1) {
            input = input.getInputEdges().get(0).getSource();
        }
        return input instanceof BatchExecSort ? (BatchExecSort) input : null;
    }
}
