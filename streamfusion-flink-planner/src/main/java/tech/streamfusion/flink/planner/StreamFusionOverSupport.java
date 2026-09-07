/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import static tech.streamfusion.flink.planner.FlinkExecNodeAccess.*;
import static tech.streamfusion.flink.planner.StreamFusionAggregateShapes.*;
import static tech.streamfusion.flink.planner.StreamFusionGroupAggregateSupport.*;
import static tech.streamfusion.flink.planner.StreamFusionJoinSupport.*;
import static tech.streamfusion.flink.planner.StreamFusionProcessingTimeShapes.*;
import static tech.streamfusion.flink.planner.StreamFusionRankShapes.*;
import static tech.streamfusion.flink.planner.StreamFusionRankSupport.*;
import static tech.streamfusion.flink.planner.StreamFusionRuntimeClasses.*;
import static tech.streamfusion.flink.planner.StreamFusionStatelessSupport.*;
import static tech.streamfusion.flink.planner.StreamFusionWindowAggregateSupport.*;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecOverAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecSort;
import org.apache.flink.table.planner.plan.nodes.exec.processor.ProcessorContext;
import org.apache.flink.table.planner.plan.nodes.exec.spec.OverSpec;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecOverAggregate;
import org.apache.flink.table.types.logical.RowType;

/** StreamFusion OverSupport for native physical planning. */
final class StreamFusionOverSupport {
    static String unsupportedReason(StreamExecOverAggregate aggregate, ProcessorContext context) {
        ExecEdge input = aggregate.getInputEdges().get(0);
        long stateTtl = aggregate
                .getPersistedConfig()
                .get(ExecutionConfigOptions.IDLE_STATE_RETENTION)
                .toMillis();
        try {
            Class<?> translator = Class.forName(
                    OVER_AGGREGATE_TRANSLATOR_CLASS,
                    true,
                    context.getPlanner().getFlinkContext().getClassLoader());
            Method method = translator.getMethod(
                    "unsupportedReason",
                    RowType.class,
                    RowType.class,
                    OverSpec.class,
                    long.class,
                    ReadableConfig.class);
            return (String) method.invoke(
                    null,
                    (RowType) input.getOutputType(),
                    (RowType) aggregate.getOutputType(),
                    overSpec(aggregate),
                    stateTtl,
                    aggregate.getPersistedConfig());
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException failure) {
            throw new IllegalStateException("Could not inspect StreamFusion OVER support", failure);
        } catch (InvocationTargetException failure) {
            throw new IllegalStateException("StreamFusion OVER support inspection failed", failure.getCause());
        }
    }

    static String unsupportedReason(BatchExecOverAggregate aggregate, ProcessorContext context) {
        ExecEdge input = aggregate.getInputEdges().get(0);
        BatchExecSort inputSort = boundedOverInputSort(aggregate);
        if (inputSort != null) {
            input = inputSort.getInputEdges().get(0);
        }
        try {
            Class<?> translator = Class.forName(
                    OVER_AGGREGATE_TRANSLATOR_CLASS,
                    true,
                    context.getPlanner().getFlinkContext().getClassLoader());
            Method method = translator.getMethod(
                    "unsupportedBoundedReason", RowType.class, RowType.class, OverSpec.class, ReadableConfig.class);
            return (String) method.invoke(
                    null,
                    (RowType) input.getOutputType(),
                    (RowType) aggregate.getOutputType(),
                    overSpec(aggregate),
                    aggregate.getPersistedConfig());
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException failure) {
            throw new IllegalStateException("Could not inspect StreamFusion bounded OVER support", failure);
        } catch (InvocationTargetException failure) {
            throw new IllegalStateException("StreamFusion bounded OVER support inspection failed", failure.getCause());
        }
    }

    static String unsupportedReason(ProcessingTimeDeduplicate folded, ProcessorContext context) {
        String reason = unsupportedCalcReason(
                (RowType) folded.inputEdge.getOutputType(),
                folded.inputType,
                folded.inputProjection,
                condition(folded.inputCalc),
                context);
        if (reason != null) {
            return "processing-time input projection: " + reason;
        }
        try {
            Class<?> translator = Class.forName(
                    DEDUPLICATE_TRANSLATOR_CLASS,
                    true,
                    context.getPlanner().getFlinkContext().getClassLoader());
            Method method = translator.getMethod(
                    "unsupportedReason",
                    RowType.class,
                    RowType.class,
                    int[].class,
                    boolean.class,
                    boolean.class,
                    boolean.class,
                    boolean.class,
                    long.class,
                    ReadableConfig.class);
            reason = (String) method.invoke(
                    null,
                    folded.inputType,
                    folded.deduplicateOutputType,
                    folded.uniqueKeys,
                    false,
                    booleanField(folded.deduplicate, "keepLastRow"),
                    booleanField(folded.deduplicate, "outputInsertOnly"),
                    booleanField(folded.deduplicate, "generateUpdateBefore"),
                    stateTtl(folded.deduplicate),
                    folded.deduplicate.getPersistedConfig());
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException failure) {
            throw new IllegalStateException("Could not inspect folded processing-time deduplicate support", failure);
        } catch (InvocationTargetException failure) {
            throw new IllegalStateException(
                    "Folded processing-time deduplicate support inspection failed", failure.getCause());
        }
        if (reason != null) {
            return reason;
        }
        reason = unsupportedCalcReason(
                folded.deduplicateOutputType,
                (RowType) folded.outputCalc.getOutputType(),
                folded.outputProjection,
                folded.outputCondition,
                context);
        return reason == null ? null : "processing-time output projection: " + reason;
    }

    static String unsupportedReason(ProcessingTimeOverAggregate folded, ProcessorContext context) {
        String reason = unsupportedCalcReason(
                (RowType) folded.inputEdge.getOutputType(),
                folded.inputType,
                folded.inputProjection,
                condition(folded.inputCalc),
                context);
        if (reason != null) {
            return "processing-time input projection: " + reason;
        }
        long stateTtl = folded.aggregate
                .getPersistedConfig()
                .get(ExecutionConfigOptions.IDLE_STATE_RETENTION)
                .toMillis();
        try {
            Class<?> translator = Class.forName(
                    OVER_AGGREGATE_TRANSLATOR_CLASS,
                    true,
                    context.getPlanner().getFlinkContext().getClassLoader());
            Method method = translator.getMethod(
                    "unsupportedReason",
                    RowType.class,
                    RowType.class,
                    OverSpec.class,
                    long.class,
                    ReadableConfig.class,
                    boolean.class);
            reason = (String) method.invoke(
                    null,
                    folded.inputType,
                    folded.overOutputType,
                    folded.overSpec,
                    stateTtl,
                    folded.aggregate.getPersistedConfig(),
                    true);
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException failure) {
            throw new IllegalStateException("Could not inspect folded processing-time OVER support", failure);
        } catch (InvocationTargetException failure) {
            throw new IllegalStateException(
                    "Folded processing-time OVER support inspection failed", failure.getCause());
        }
        if (reason != null) {
            return reason;
        }
        reason = unsupportedCalcReason(
                folded.overOutputType,
                (RowType) folded.outputCalc.getOutputType(),
                folded.outputProjection,
                folded.outputCondition,
                context);
        return reason == null ? null : "processing-time output projection: " + reason;
    }
}
