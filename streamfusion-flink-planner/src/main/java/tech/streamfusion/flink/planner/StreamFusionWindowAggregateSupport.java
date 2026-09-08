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
import static tech.streamfusion.flink.planner.StreamFusionProcessingTimeShapes.*;
import static tech.streamfusion.flink.planner.StreamFusionRankShapes.*;
import static tech.streamfusion.flink.planner.StreamFusionRankSupport.*;
import static tech.streamfusion.flink.planner.StreamFusionRuntimeClasses.*;
import static tech.streamfusion.flink.planner.StreamFusionStatelessSupport.*;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.planner.plan.logical.LogicalWindow;
import org.apache.flink.table.planner.plan.logical.WindowingStrategy;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.processor.ProcessorContext;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecWindowAggregate;
import org.apache.flink.table.runtime.groupwindow.NamedWindowProperty;
import org.apache.flink.table.types.logical.RowType;

/** StreamFusion WindowAggregateSupport for native physical planning. */
final class StreamFusionWindowAggregateSupport {
    static String unsupportedReason(BatchWindowAggregatePair pair, ProcessorContext context) {
        int[] localGrouping = batchWindowGrouping(pair.local);
        int[] finalGrouping = batchWindowGrouping(pair.global);
        if (batchWindowAuxiliaryGrouping(pair.local).length != 0
                || batchWindowAuxiliaryGrouping(pair.global).length != 0) {
            return "auxiliary grouping: bounded native two-phase window aggregation does not yet encode auxiliary keys";
        }
        if (localGrouping.length != finalGrouping.length) {
            return "grouping: bounded local and global window aggregate key counts do not match";
        }
        if (batchWindowAggregateCalls(pair.local).length != batchWindowAggregateCalls(pair.global).length) {
            return "aggregate: bounded local and global window aggregate call counts do not match";
        }
        if (!batchWindow(pair.local).toString().equals(batchWindow(pair.global).toString())) {
            return "window: bounded local and global window definitions do not match";
        }
        if (batchWindowBoolean(pair.local, "inputTimeIsDate") || batchWindowBoolean(pair.global, "inputTimeIsDate")) {
            return "window time type: bounded native window aggregation currently requires TIMESTAMP";
        }
        RowType inputType = (RowType) pair.inputEdge.getOutputType();
        if (!batchWindowInputType(pair.local).equals(inputType)) {
            return "aggregate input schema: planned local window input "
                    + batchWindowInputType(pair.local)
                    + " does not match edge input "
                    + inputType;
        }
        try {
            Class<?> translator = Class.forName(
                    GROUP_WINDOW_AGGREGATE_TRANSLATOR_CLASS,
                    true,
                    context.getPlanner().getFlinkContext().getClassLoader());
            Method method = translator.getMethod(
                    "unsupportedBatchReason",
                    RowType.class,
                    RowType.class,
                    int[].class,
                    org.apache.calcite.rel.core.AggregateCall[].class,
                    LogicalWindow.class,
                    NamedWindowProperty[].class,
                    ReadableConfig.class);
            return (String) method.invoke(
                    null,
                    inputType,
                    (RowType) pair.global.getOutputType(),
                    localGrouping,
                    batchWindowAggregateCalls(pair.local),
                    batchWindow(pair.local),
                    batchWindowProperties(pair.global),
                    pair.global.getPersistedConfig());
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException failure) {
            throw new IllegalStateException("Could not inspect bounded native two-phase window aggregation", failure);
        } catch (InvocationTargetException failure) {
            throw new IllegalStateException(
                    "Bounded native two-phase window aggregation support inspection failed", failure.getCause());
        }
    }

    static String unsupportedBatchOnePhaseWindowReason(
            ExecNodeBase<?> aggregate, ExecEdge inputEdge, ProcessorContext context) {
        if (batchWindowAuxiliaryGrouping(aggregate).length != 0) {
            return "auxiliary grouping: bounded native window aggregation does not yet encode auxiliary keys";
        }
        if (batchWindowBoolean(aggregate, "inputTimeIsDate")) {
            return "window time type: bounded native window aggregation currently requires TIMESTAMP";
        }
        RowType inputType = (RowType) inputEdge.getOutputType();
        if (!batchWindowInputType(aggregate).equals(inputType)) {
            return "aggregate input schema: planned bounded window input "
                    + batchWindowInputType(aggregate)
                    + " does not match edge input "
                    + inputType;
        }
        try {
            Class<?> translator = Class.forName(
                    GROUP_WINDOW_AGGREGATE_TRANSLATOR_CLASS,
                    true,
                    context.getPlanner().getFlinkContext().getClassLoader());
            Method method = translator.getMethod(
                    "unsupportedBatchOnePhaseReason",
                    RowType.class,
                    RowType.class,
                    int[].class,
                    org.apache.calcite.rel.core.AggregateCall[].class,
                    LogicalWindow.class,
                    NamedWindowProperty[].class,
                    ReadableConfig.class);
            return (String) method.invoke(
                    null,
                    inputType,
                    (RowType) aggregate.getOutputType(),
                    batchWindowGrouping(aggregate),
                    batchWindowAggregateCalls(aggregate),
                    batchWindow(aggregate),
                    batchWindowProperties(aggregate),
                    aggregate.getPersistedConfig());
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException failure) {
            throw new IllegalStateException("Could not inspect bounded native one-phase window aggregation", failure);
        } catch (InvocationTargetException failure) {
            throw new IllegalStateException(
                    "Bounded native one-phase window aggregation support inspection failed", failure.getCause());
        }
    }

    static String unsupportedReason(StreamExecWindowAggregate aggregate, ProcessorContext context) {
        ExecEdge input = aggregate.getInputEdges().get(0);
        try {
            Class<?> translator = Class.forName(
                    WINDOW_AGGREGATE_TRANSLATOR_CLASS,
                    true,
                    context.getPlanner().getFlinkContext().getClassLoader());
            Method method = translator.getMethod(
                    "unsupportedReason",
                    RowType.class,
                    RowType.class,
                    int[].class,
                    org.apache.calcite.rel.core.AggregateCall[].class,
                    WindowingStrategy.class,
                    NamedWindowProperty[].class,
                    boolean.class,
                    ReadableConfig.class);
            return (String) method.invoke(
                    null,
                    (RowType) input.getOutputType(),
                    (RowType) aggregate.getOutputType(),
                    windowGrouping(aggregate),
                    windowAggregateCalls(aggregate),
                    windowing(aggregate),
                    windowProperties(aggregate),
                    windowNeedRetraction(aggregate),
                    aggregate.getPersistedConfig());
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalStateException("Could not inspect StreamFusion WindowAggregate support", e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException("StreamFusion WindowAggregate support inspection failed", e.getCause());
        }
    }

    static String unsupportedReason(LegacyGroupWindowAggregate aggregate, ProcessorContext context) {
        try {
            Class<?> translator = Class.forName(
                    GROUP_WINDOW_AGGREGATE_TRANSLATOR_CLASS,
                    true,
                    context.getPlanner().getFlinkContext().getClassLoader());
            Method method = translator.getMethod(
                    "unsupportedReason",
                    RowType.class,
                    RowType.class,
                    int[].class,
                    org.apache.calcite.rel.core.AggregateCall[].class,
                    LogicalWindow.class,
                    NamedWindowProperty[].class,
                    boolean.class,
                    ReadableConfig.class);
            return (String) method.invoke(
                    null,
                    (RowType) aggregate.node.getInputEdges().get(0).getOutputType(),
                    (RowType) aggregate.node.getOutputType(),
                    aggregate.grouping,
                    aggregate.aggregateCalls,
                    aggregate.window,
                    aggregate.properties,
                    aggregate.needRetraction,
                    context == null
                            ? aggregate.node.getPersistedConfig()
                            : context.getPlanner().getTableConfig());
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalStateException("Could not inspect legacy group-window support", e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException("Legacy group-window support inspection failed", e.getCause());
        }
    }

    static String unsupportedReason(ProcessingTimeWindowAggregate aggregate, ProcessorContext context) {
        try {
            Class<?> translator = Class.forName(
                    WINDOW_AGGREGATE_TRANSLATOR_CLASS,
                    true,
                    context.getPlanner().getFlinkContext().getClassLoader());
            Method method = translator.getMethod(
                    "unsupportedReason",
                    RowType.class,
                    RowType.class,
                    int[].class,
                    org.apache.calcite.rel.core.AggregateCall[].class,
                    WindowingStrategy.class,
                    NamedWindowProperty[].class,
                    boolean.class,
                    ReadableConfig.class);
            return (String) method.invoke(
                    null,
                    (RowType) aggregate.inputEdge.getOutputType(),
                    (RowType) aggregate.node.getOutputType(),
                    aggregate.grouping,
                    aggregate.aggregateCalls,
                    aggregate.windowing,
                    windowProperties(aggregate.node),
                    windowNeedRetraction(aggregate.node),
                    aggregate.node.getPersistedConfig());
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalStateException("Could not inspect folded processing-time WindowAggregate support", e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException(
                    "Folded processing-time WindowAggregate support inspection failed", e.getCause());
        }
    }

    static String unsupportedReason(TwoPhaseWindowAggregate aggregate, ProcessorContext context) {
        try {
            Class<?> translator = Class.forName(
                    "tech.streamfusion.flink.window.StreamFusionGlobalWindowAggregateTranslator",
                    true,
                    context.getPlanner().getFlinkContext().getClassLoader());
            Method method = translator.getMethod(
                    "unsupportedStageReason",
                    RowType.class,
                    RowType.class,
                    RowType.class,
                    int.class,
                    org.apache.calcite.rel.core.AggregateCall[].class,
                    WindowingStrategy.class,
                    NamedWindowProperty[].class,
                    boolean.class,
                    ReadableConfig.class);
            RowType originalInput = (RowType) aggregate.inputEdge.getOutputType();
            int[] grouping = localWindowGrouping(aggregate.local);
            var config = org.apache.flink.configuration.Configuration.fromMap(
                    context.getPlanner().getTableConfig().getConfiguration().toMap());
            config.addAll(org.apache.flink.configuration.Configuration.fromMap(
                    aggregate.global.getPersistedConfig().toMap()));
            return (String) method.invoke(
                    null,
                    originalInput,
                    nativeWindowAccumulatorType(originalInput, grouping),
                    aggregate.global.getOutputType(),
                    grouping.length,
                    localWindowAggregateCalls(aggregate.local),
                    localWindowing(aggregate.local),
                    globalWindowProperties(aggregate.global),
                    localWindowNeedRetraction(aggregate.local),
                    config);
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalStateException("Could not inspect shared two-phase WindowAggregate support", e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException("Shared two-phase WindowAggregate support inspection failed", e.getCause());
        }
    }
}
