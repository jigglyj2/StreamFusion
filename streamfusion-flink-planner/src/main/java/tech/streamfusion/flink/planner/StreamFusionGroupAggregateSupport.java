/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import static tech.streamfusion.flink.planner.FlinkExecNodeAccess.*;
import static tech.streamfusion.flink.planner.StreamFusionAggregateShapes.*;
import static tech.streamfusion.flink.planner.StreamFusionJoinSupport.*;
import static tech.streamfusion.flink.planner.StreamFusionOverSupport.*;
import static tech.streamfusion.flink.planner.StreamFusionProcessingTimeShapes.*;
import static tech.streamfusion.flink.planner.StreamFusionRankShapes.*;
import static tech.streamfusion.flink.planner.StreamFusionRankSupport.*;
import static tech.streamfusion.flink.planner.StreamFusionRuntimeClasses.*;
import static tech.streamfusion.flink.planner.StreamFusionStatelessSupport.*;
import static tech.streamfusion.flink.planner.StreamFusionWindowAggregateSupport.*;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.StateMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecHashAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.processor.ProcessorContext;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecGlobalGroupAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecGroupAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecLocalGroupAggregate;
import org.apache.flink.table.types.logical.RowType;

/** StreamFusion GroupAggregateSupport for native physical planning. */
final class StreamFusionGroupAggregateSupport {
    static String unsupportedReason(BatchExecHashAggregate aggregate, ProcessorContext context) {
        if (auxiliaryGrouping(aggregate).length != 0) {
            return "auxiliary grouping: bounded native hash aggregation does not yet encode auxiliary keys";
        }
        if (batchAggregateBooleanField(aggregate, "isMerge")) {
            return "merge phase: bounded native hash aggregation currently requires a one-phase final plan";
        }
        if (!batchAggregateBooleanField(aggregate, "isFinal")) {
            return "local phase: bounded native hash aggregation currently requires a one-phase final plan";
        }
        ExecEdge input = aggregate.getInputEdges().get(0);
        RowType inputType = (RowType) input.getOutputType();
        if (!aggregateInputType(aggregate).equals(inputType)) {
            return "aggregate input schema: planned aggregate input "
                    + aggregateInputType(aggregate)
                    + " does not match edge input "
                    + inputType;
        }
        try {
            Class<?> translator = Class.forName(
                    GROUP_AGGREGATE_TRANSLATOR_CLASS, true, StreamFusionRuntimeClasses.class.getClassLoader());
            Method method = translator.getMethod(
                    "unsupportedBatchReason",
                    RowType.class,
                    RowType.class,
                    int[].class,
                    org.apache.calcite.rel.core.AggregateCall[].class,
                    ReadableConfig.class);
            return (String) method.invoke(
                    null,
                    inputType,
                    (RowType) aggregate.getOutputType(),
                    grouping(aggregate),
                    aggregateCalls(aggregate),
                    aggregate.getPersistedConfig());
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException failure) {
            throw new IllegalStateException("Could not inspect bounded native hash aggregate support", failure);
        } catch (InvocationTargetException failure) {
            throw new IllegalStateException(
                    "Bounded native hash aggregate support inspection failed", failure.getCause());
        }
    }

    static String unsupportedReason(BatchGroupAggregatePair pair, ProcessorContext context) {
        int[] localGrouping = batchGrouping(pair.local);
        int[] finalGrouping = batchGrouping(pair.global);
        if (batchAuxiliaryGrouping(pair.local).length != 0 || batchAuxiliaryGrouping(pair.global).length != 0) {
            return "auxiliary grouping: bounded native two-phase aggregation does not yet encode auxiliary keys";
        }
        if (localGrouping.length != finalGrouping.length) {
            return "grouping: bounded local and global aggregate key counts do not match";
        }
        if (batchAggregateCalls(pair.local).length != batchAggregateCalls(pair.global).length) {
            return "aggregate: bounded local and global call counts do not match";
        }
        RowType inputType = (RowType) pair.inputEdge.getOutputType();
        if (!batchAggregateInputType(pair.local).equals(inputType)) {
            return "aggregate input schema: planned local input "
                    + batchAggregateInputType(pair.local)
                    + " does not match edge input "
                    + inputType;
        }
        try {
            Class<?> translator = Class.forName(
                    GROUP_AGGREGATE_TRANSLATOR_CLASS, true, StreamFusionRuntimeClasses.class.getClassLoader());
            Method method = translator.getMethod(
                    "unsupportedBatchReason",
                    RowType.class,
                    RowType.class,
                    int[].class,
                    org.apache.calcite.rel.core.AggregateCall[].class,
                    ReadableConfig.class);
            return (String) method.invoke(
                    null,
                    inputType,
                    (RowType) pair.global.getOutputType(),
                    localGrouping,
                    batchAggregateCalls(pair.local),
                    pair.global.getPersistedConfig());
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException failure) {
            throw new IllegalStateException("Could not inspect bounded native two-phase aggregate support", failure);
        } catch (InvocationTargetException failure) {
            throw new IllegalStateException(
                    "Bounded native two-phase aggregate support inspection failed", failure.getCause());
        }
    }

    static String unsupportedReason(StreamExecGroupAggregate aggregate, ProcessorContext context) {
        ExecEdge input = aggregate.getInputEdges().get(0);
        Configuration config = Configuration.fromMap(
                context.getPlanner().getTableConfig().getConfiguration().toMap());
        config.addAll(Configuration.fromMap(aggregate.getPersistedConfig().toMap()));
        long retention = StateMetadata.getStateTtlForOneInputOperator(
                ExecNodeConfig.ofNodeConfig(config, false), aggregateStateMetadata(aggregate));
        try {
            Class<?> translator = Class.forName(
                    GROUP_AGGREGATE_TRANSLATOR_CLASS, true, StreamFusionRuntimeClasses.class.getClassLoader());
            Method method = translator.getMethod(
                    "unsupportedStageReason",
                    RowType.class,
                    RowType.class,
                    int[].class,
                    org.apache.calcite.rel.core.AggregateCall[].class,
                    boolean[].class,
                    boolean.class,
                    boolean.class,
                    long.class,
                    ReadableConfig.class);
            return (String) method.invoke(
                    null,
                    (RowType) input.getOutputType(),
                    (RowType) aggregate.getOutputType(),
                    grouping(aggregate),
                    aggregateCalls(aggregate),
                    aggregateCallNeedRetractions(aggregate),
                    aggregateBooleanField(aggregate, "generateUpdateBefore"),
                    aggregateBooleanField(aggregate, "needRetraction"),
                    retention,
                    config);
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalStateException("Could not inspect StreamFusion GroupAggregate support", e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException("StreamFusion GroupAggregate support inspection failed", e.getCause());
        }
    }

    static String unsupportedReason(TwoPhaseGroupAggregate aggregate, ProcessorContext context) {
        String localReason = StreamFusionLocalGroupAggregateSupport.unsupportedReason(aggregate.local, context);
        if (localReason != null) return localReason;
        StreamExecLocalGroupAggregate local = aggregate.local;
        StreamExecGlobalGroupAggregate global = aggregate.global;
        String globalReason = StreamFusionGlobalGroupAggregateSupport.unsupportedReason(global, context);
        if (globalReason != null) return globalReason;
        int[] expectedGlobalGrouping = java.util.stream.IntStream.range(0, localGroupGrouping(local).length)
                .toArray();
        if (!java.util.Arrays.equals(globalGroupGrouping(global), expectedGlobalGrouping)) {
            return "two-phase aggregate: global grouping must address the local grouping prefix";
        }
        if (localGroupAggregateCalls(local).length != globalGroupAggregateCalls(global).length) {
            return "two-phase aggregate: local and global aggregate call counts differ";
        }
        try {
            Class<?> translator = Class.forName(
                    GROUP_AGGREGATE_TRANSLATOR_CLASS, true, StreamFusionRuntimeClasses.class.getClassLoader());
            Method method = translator.getMethod(
                    "unsupportedReason",
                    RowType.class,
                    RowType.class,
                    int[].class,
                    org.apache.calcite.rel.core.AggregateCall[].class,
                    boolean[].class,
                    boolean.class,
                    boolean.class,
                    long.class,
                    ReadableConfig.class);
            return (String) method.invoke(
                    null,
                    (RowType) aggregate.inputEdge.getOutputType(),
                    (RowType) global.getOutputType(),
                    localGroupGrouping(local),
                    localGroupAggregateCalls(local),
                    localGroupCallNeedRetractions(local),
                    globalGroupGenerateUpdateBefore(global),
                    localGroupNeedRetraction(local),
                    globalGroupStateTtl(global),
                    global.getPersistedConfig());
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalStateException("Could not inspect two-phase GroupAggregate support", e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException("Two-phase GroupAggregate support inspection failed", e.getCause());
        }
    }

    static String unsupportedReason(IncrementalGroupAggregate aggregate, ProcessorContext context) {
        String localReason = StreamFusionLocalGroupAggregateSupport.unsupportedReason(aggregate.local, context);
        if (localReason != null) return localReason;
        String globalReason = StreamFusionGlobalGroupAggregateSupport.unsupportedReason(aggregate.global, context);
        if (globalReason != null) return "incremental final " + globalReason;
        RowType partialInput = incrementalPartialOriginalInputType(aggregate.incremental);
        org.apache.calcite.rel.core.AggregateCall[] partialCalls = incrementalOriginalCalls(aggregate.incremental);
        String partialReason = groupAggregateUnsupported(
                context,
                partialInput,
                aggregateOutputType(partialInput, localGroupGrouping(aggregate.local), partialCalls),
                localGroupGrouping(aggregate.local),
                partialCalls,
                incrementalCallNeedRetractions(aggregate.incremental),
                false,
                incrementalNeedRetraction(aggregate.incremental),
                incrementalStateTtl(aggregate.incremental),
                aggregate.incremental.getPersistedConfig());
        if (partialReason != null) {
            return "incremental partial " + partialReason;
        }
        String finalReason = groupAggregateUnsupported(
                context,
                globalGroupOriginalInputType(aggregate.global),
                (RowType) aggregate.global.getOutputType(),
                globalGroupGrouping(aggregate.global),
                globalGroupAggregateCalls(aggregate.global),
                globalGroupCallNeedRetractions(aggregate.global),
                globalGroupGenerateUpdateBefore(aggregate.global),
                globalGroupNeedRetraction(aggregate.global),
                globalGroupStateTtl(aggregate.global),
                aggregate.global.getPersistedConfig());
        return finalReason == null ? null : "incremental final " + finalReason;
    }

    static String groupAggregateUnsupported(
            ProcessorContext context,
            RowType inputType,
            RowType outputType,
            int[] grouping,
            org.apache.calcite.rel.core.AggregateCall[] calls,
            boolean[] retractable,
            boolean generateUpdateBefore,
            boolean needRetraction,
            long stateTtl,
            ReadableConfig config) {
        try {
            Class<?> translator = Class.forName(
                    GROUP_AGGREGATE_TRANSLATOR_CLASS, true, StreamFusionRuntimeClasses.class.getClassLoader());
            Method method = translator.getMethod(
                    "unsupportedReason",
                    RowType.class,
                    RowType.class,
                    int[].class,
                    org.apache.calcite.rel.core.AggregateCall[].class,
                    boolean[].class,
                    boolean.class,
                    boolean.class,
                    long.class,
                    ReadableConfig.class);
            return (String) method.invoke(
                    null,
                    inputType,
                    outputType,
                    grouping,
                    calls,
                    retractable,
                    generateUpdateBefore,
                    needRetraction,
                    stateTtl,
                    config);
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalStateException("Could not inspect incremental GroupAggregate support", e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException("Incremental GroupAggregate support inspection failed", e.getCause());
        }
    }
}
