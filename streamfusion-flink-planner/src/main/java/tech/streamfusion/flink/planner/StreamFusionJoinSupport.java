/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import static tech.streamfusion.flink.planner.FlinkExecNodeAccess.*;
import static tech.streamfusion.flink.planner.StreamFusionAggregateShapes.*;
import static tech.streamfusion.flink.planner.StreamFusionGroupAggregateSupport.*;
import static tech.streamfusion.flink.planner.StreamFusionOverSupport.*;
import static tech.streamfusion.flink.planner.StreamFusionProcessingTimeShapes.*;
import static tech.streamfusion.flink.planner.StreamFusionRankShapes.*;
import static tech.streamfusion.flink.planner.StreamFusionRankSupport.*;
import static tech.streamfusion.flink.planner.StreamFusionRuntimeClasses.*;
import static tech.streamfusion.flink.planner.StreamFusionStatelessSupport.*;
import static tech.streamfusion.flink.planner.StreamFusionWindowAggregateSupport.*;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.planner.plan.logical.WindowingStrategy;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecAdaptiveJoin;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecHashJoin;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecNestedLoopJoin;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecSortMergeJoin;
import org.apache.flink.table.planner.plan.nodes.exec.processor.ProcessorContext;
import org.apache.flink.table.planner.plan.nodes.exec.spec.IntervalJoinSpec;
import org.apache.flink.table.planner.plan.nodes.exec.spec.JoinSpec;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecIntervalJoin;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecJoin;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecMultiJoin;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecTemporalJoin;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecWindowJoin;
import org.apache.flink.table.runtime.operators.join.stream.keyselector.AttributeBasedJoinKeyExtractor.ConditionAttributeRef;
import org.apache.flink.table.types.logical.RowType;

/** StreamFusion JoinSupport for native physical planning. */
final class StreamFusionJoinSupport {
    static String unsupportedReason(BatchExecHashJoin join, ProcessorContext context) {
        return unsupportedBatchJoinReason(join, batchHashJoinSpec(join), context);
    }

    static String unsupportedReason(BatchExecAdaptiveJoin join, ProcessorContext context) {
        return unsupportedBatchJoinReason(join, batchAdaptiveJoinSpec(join), context);
    }

    static String unsupportedReason(BatchExecSortMergeJoin join, ProcessorContext context) {
        return unsupportedBatchJoinReason(join, batchSortMergeJoinSpec(join), context);
    }

    static String unsupportedReason(BatchExecNestedLoopJoin join, ProcessorContext context) {
        if (batchNestedLoopJoinSingleRow(join)) {
            return "single-row join: native bounded join does not yet enforce scalar-subquery cardinality";
        }
        return unsupportedBatchJoinReason(join, batchNestedLoopJoinSpec(join), context);
    }

    static String unsupportedBatchJoinReason(ExecNode<?> join, JoinSpec joinSpec, ProcessorContext context) {
        ExecEdge left = join.getInputEdges().get(0);
        ExecEdge right = join.getInputEdges().get(1);
        try {
            Class<?> translator = Class.forName(
                    REGULAR_JOIN_TRANSLATOR_CLASS, true, StreamFusionRuntimeClasses.class.getClassLoader());
            Method method = translator.getMethod(
                    "unsupportedBatchReason", RowType.class, RowType.class, RowType.class, JoinSpec.class);
            return (String) method.invoke(
                    null,
                    (RowType) left.getOutputType(),
                    (RowType) right.getOutputType(),
                    (RowType) join.getOutputType(),
                    joinSpec);
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException failure) {
            throw new IllegalStateException("Could not inspect bounded native join support", failure);
        } catch (InvocationTargetException failure) {
            throw new IllegalStateException("Bounded native join support inspection failed", failure.getCause());
        }
    }

    static String unsupportedReason(StreamExecWindowJoin join, ProcessorContext context) {
        ExecEdge left = join.getInputEdges().get(0);
        ExecEdge right = join.getInputEdges().get(1);
        try {
            Class<?> translator = Class.forName(
                    WINDOW_JOIN_TRANSLATOR_CLASS, true, StreamFusionRuntimeClasses.class.getClassLoader());
            Method method = translator.getMethod(
                    "unsupportedReason",
                    RowType.class,
                    RowType.class,
                    RowType.class,
                    JoinSpec.class,
                    WindowingStrategy.class,
                    WindowingStrategy.class,
                    ReadableConfig.class);
            return (String) method.invoke(
                    null,
                    (RowType) left.getOutputType(),
                    (RowType) right.getOutputType(),
                    (RowType) join.getOutputType(),
                    windowJoinSpec(join),
                    windowJoinLeftWindowing(join),
                    windowJoinRightWindowing(join),
                    join.getPersistedConfig());
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalStateException("Could not inspect StreamFusion WindowJoin support", e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException("StreamFusion WindowJoin support inspection failed", e.getCause());
        }
    }

    static String unsupportedReason(StreamExecTemporalJoin join, ProcessorContext context) {
        ExecEdge left = join.getInputEdges().get(0);
        ExecEdge right = join.getInputEdges().get(1);
        try {
            Class<?> translator = Class.forName(
                    TEMPORAL_JOIN_TRANSLATOR_CLASS,
                    true,
                    context.getPlanner().getFlinkContext().getClassLoader());
            Method method = translator.getMethod(
                    "unsupportedReason",
                    RowType.class,
                    RowType.class,
                    RowType.class,
                    JoinSpec.class,
                    boolean.class,
                    int.class,
                    int.class,
                    ReadableConfig.class);
            return (String) method.invoke(
                    null,
                    (RowType) left.getOutputType(),
                    (RowType) right.getOutputType(),
                    (RowType) join.getOutputType(),
                    temporalJoinSpec(join),
                    temporalJoinFunction(join),
                    temporalJoinLeftTimeIndex(join),
                    temporalJoinRightTimeIndex(join),
                    join.getPersistedConfig());
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalStateException("Could not inspect StreamFusion temporal join support", e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException("StreamFusion temporal join support inspection failed", e.getCause());
        }
    }

    static String unsupportedReason(StreamExecMultiJoin join, ProcessorContext context) {
        List<RowType> inputTypes = join.getInputEdges().stream()
                .map(edge -> (RowType) edge.getOutputType())
                .collect(Collectors.toList());
        Map<Integer, List<ConditionAttributeRef>> attributes = multiJoinAttributeMap(join);
        org.apache.flink.table.runtime.operators.join.stream.keyselector.AttributeBasedJoinKeyExtractor extractor =
                new org.apache.flink.table.runtime.operators.join.stream.keyselector.AttributeBasedJoinKeyExtractor(
                        attributes, inputTypes);
        List<int[]> commonKeys = java.util.stream.IntStream.range(0, inputTypes.size())
                .mapToObj(extractor::getCommonJoinKeyIndices)
                .collect(Collectors.toList());
        try {
            Class<?> translator = Class.forName(
                    MULTI_JOIN_TRANSLATOR_CLASS,
                    true,
                    context.getPlanner().getFlinkContext().getClassLoader());
            Method method = translator.getMethod(
                    "unsupportedReason",
                    List.class,
                    RowType.class,
                    List.class,
                    List.class,
                    Map.class,
                    List.class,
                    long[].class,
                    boolean.class,
                    ReadableConfig.class);
            return (String) method.invoke(
                    null,
                    inputTypes,
                    (RowType) join.getOutputType(),
                    commonKeys,
                    multiJoinTypes(join),
                    attributes,
                    multiJoinUniqueKeys(join),
                    multiJoinStateTtl(join),
                    multiJoinEquiOnly(join),
                    join.getPersistedConfig());
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalStateException("Could not inspect StreamFusion multi-join support", e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException("StreamFusion multi-join support inspection failed", e.getCause());
        }
    }

    static String unsupportedReason(JoinSpec joinSpec, StreamExecMultiJoin join, ProcessorContext context) {
        List<ExecEdge> inputs = join.getInputEdges();
        List<List<int[]>> uniqueKeys = multiJoinUniqueKeys(join);
        long[] ttl = multiJoinStateTtl(join);
        return unsupportedReason(
                (RowType) inputs.get(0).getOutputType(),
                (RowType) inputs.get(1).getOutputType(),
                (RowType) join.getOutputType(),
                joinSpec,
                uniqueKeys.get(0),
                uniqueKeys.get(1),
                ttl[0],
                ttl[1],
                join.getPersistedConfig(),
                context);
    }

    static String unsupportedReason(StreamExecJoin join, ProcessorContext context) {
        ExecEdge left = join.getInputEdges().get(0);
        ExecEdge right = join.getInputEdges().get(1);
        List<Long> ttl = regularJoinStateTtl(join);
        return unsupportedReason(
                (RowType) left.getOutputType(),
                (RowType) right.getOutputType(),
                (RowType) join.getOutputType(),
                regularJoinSpec(join),
                regularJoinLeftUpsertKeys(join),
                regularJoinRightUpsertKeys(join),
                ttl.get(0),
                ttl.get(1),
                join.getPersistedConfig(),
                context);
    }

    static String unsupportedReason(
            RowType leftType,
            RowType rightType,
            RowType outputType,
            JoinSpec joinSpec,
            List<int[]> leftUpsertKeys,
            List<int[]> rightUpsertKeys,
            long leftTtl,
            long rightTtl,
            ReadableConfig config,
            ProcessorContext context) {
        try {
            Class<?> translator = Class.forName(
                    REGULAR_JOIN_TRANSLATOR_CLASS, true, StreamFusionRuntimeClasses.class.getClassLoader());
            Method method = translator.getMethod(
                    "unsupportedReason",
                    RowType.class,
                    RowType.class,
                    RowType.class,
                    JoinSpec.class,
                    List.class,
                    List.class,
                    long.class,
                    long.class,
                    ReadableConfig.class);
            return (String) method.invoke(
                    null,
                    leftType,
                    rightType,
                    outputType,
                    joinSpec,
                    leftUpsertKeys,
                    rightUpsertKeys,
                    leftTtl,
                    rightTtl,
                    config);
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalStateException("Could not inspect StreamFusion regular join support", e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException("StreamFusion regular join support inspection failed", e.getCause());
        }
    }

    static String unsupportedReason(StreamExecIntervalJoin join, ProcessorContext context) {
        ExecEdge left = join.getInputEdges().get(0);
        ExecEdge right = join.getInputEdges().get(1);
        try {
            Class<?> translator = Class.forName(
                    INTERVAL_JOIN_TRANSLATOR_CLASS,
                    true,
                    context.getPlanner().getFlinkContext().getClassLoader());
            Method method = translator.getMethod(
                    "unsupportedReason",
                    RowType.class,
                    RowType.class,
                    RowType.class,
                    IntervalJoinSpec.class,
                    ReadableConfig.class);
            return (String) method.invoke(
                    null,
                    (RowType) left.getOutputType(),
                    (RowType) right.getOutputType(),
                    (RowType) join.getOutputType(),
                    intervalJoinSpec(join),
                    join.getPersistedConfig());
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalStateException("Could not inspect StreamFusion interval join support", e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException("StreamFusion interval join support inspection failed", e.getCause());
        }
    }
}
