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
import static tech.streamfusion.flink.planner.StreamFusionRuntimeClasses.*;
import static tech.streamfusion.flink.planner.StreamFusionStatelessSupport.*;
import static tech.streamfusion.flink.planner.StreamFusionWindowAggregateSupport.*;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.planner.plan.logical.WindowingStrategy;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecLimit;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecRank;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecSort;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecSortLimit;
import org.apache.flink.table.planner.plan.nodes.exec.processor.ProcessorContext;
import org.apache.flink.table.planner.plan.nodes.exec.spec.SortSpec;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecChangelogNormalize;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecDeduplicate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecRank;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecSort;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecTemporalSort;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecWindowDeduplicate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecWindowRank;
import org.apache.flink.table.runtime.operators.rank.ConstantRankRange;
import org.apache.flink.table.runtime.operators.rank.RankRange;
import org.apache.flink.table.runtime.operators.rank.RankType;
import org.apache.flink.table.runtime.operators.rank.VariableRankRange;
import org.apache.flink.table.types.logical.RowType;

/** StreamFusion RankSupport for native physical planning. */
final class StreamFusionRankSupport {
    static String unsupportedReason(StreamExecDeduplicate deduplicate, ProcessorContext context) {
        ExecEdge input = deduplicate.getInputEdges().get(0);
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
            return (String) method.invoke(
                    null,
                    (RowType) input.getOutputType(),
                    (RowType) deduplicate.getOutputType(),
                    uniqueKeys(deduplicate),
                    booleanField(deduplicate, "isRowtime"),
                    booleanField(deduplicate, "keepLastRow"),
                    booleanField(deduplicate, "outputInsertOnly"),
                    booleanField(deduplicate, "generateUpdateBefore"),
                    stateTtl(deduplicate),
                    deduplicate.getPersistedConfig());
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalStateException("Could not inspect StreamFusion Deduplicate support", e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException("StreamFusion Deduplicate support inspection failed", e.getCause());
        }
    }

    static String unsupportedReason(StreamExecChangelogNormalize normalize, ProcessorContext context) {
        ExecEdge input = normalize.getInputEdges().get(0);
        try {
            Class<?> translator = Class.forName(
                    CHANGELOG_NORMALIZE_TRANSLATOR_CLASS,
                    true,
                    context.getPlanner().getFlinkContext().getClassLoader());
            Method method = translator.getMethod(
                    "unsupportedReason", RowType.class, RowType.class, int[].class, ReadableConfig.class);
            return (String) method.invoke(
                    null,
                    (RowType) input.getOutputType(),
                    (RowType) normalize.getOutputType(),
                    changelogNormalizeUniqueKeys(normalize),
                    normalize.getPersistedConfig());
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalStateException("Could not inspect StreamFusion ChangelogNormalize support", e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException("StreamFusion ChangelogNormalize support inspection failed", e.getCause());
        }
    }

    static String unsupportedReason(StreamExecWindowDeduplicate deduplicate, ProcessorContext context) {
        ExecEdge input = deduplicate.getInputEdges().get(0);
        try {
            Class<?> translator = Class.forName(
                    WINDOW_DEDUPLICATE_TRANSLATOR_CLASS, true, StreamFusionRuntimeClasses.class.getClassLoader());
            Method method = translator.getMethod(
                    "unsupportedReason",
                    RowType.class,
                    RowType.class,
                    int[].class,
                    int.class,
                    WindowingStrategy.class,
                    ReadableConfig.class);
            return (String) method.invoke(
                    null,
                    (RowType) input.getOutputType(),
                    (RowType) deduplicate.getOutputType(),
                    windowDeduplicatePartitionKeys(deduplicate),
                    windowDeduplicateOrderKey(deduplicate),
                    windowDeduplicateWindowing(deduplicate),
                    deduplicate.getPersistedConfig());
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalStateException("Could not inspect StreamFusion WindowDeduplicate support", e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException("StreamFusion WindowDeduplicate support inspection failed", e.getCause());
        }
    }

    static String unsupportedReason(StreamExecRank rank, ProcessorContext context) {
        if (rankType(rank) != RankType.ROW_NUMBER) {
            return "rank type: Flink streaming Top-N only implements ROW_NUMBER";
        }
        RankRange range = rankRange(rank);
        long start;
        Long end;
        Integer variableEnd;
        if (range instanceof ConstantRankRange) {
            start = ((ConstantRankRange) range).getRankStart();
            end = ((ConstantRankRange) range).getRankEnd();
            variableEnd = null;
        } else if (range instanceof VariableRankRange) {
            start = 1L;
            end = null;
            variableEnd = ((VariableRankRange) range).getRankEndIndex();
        } else {
            return "rank range: Flink Top-N requires a constant or variable rank end";
        }
        ExecEdge input = rank.getInputEdges().get(0);
        try {
            Class<?> translator = Class.forName(
                    TOP_N_TRANSLATOR_CLASS,
                    true,
                    context.getPlanner().getFlinkContext().getClassLoader());
            Method method = translator.getMethod(
                    "unsupportedReason",
                    RowType.class,
                    RowType.class,
                    int[].class,
                    SortSpec.class,
                    int[].class,
                    long.class,
                    Long.class,
                    Integer.class,
                    boolean.class,
                    String.class,
                    long.class,
                    ReadableConfig.class);
            return (String) method.invoke(
                    null,
                    (RowType) input.getOutputType(),
                    (RowType) rank.getOutputType(),
                    rankPartitionKeys(rank),
                    rankSortSpec(rank),
                    rankPrimaryKeys(rank),
                    start,
                    end,
                    variableEnd,
                    rankOutputNumber(rank),
                    rankStrategyName(rank),
                    rankStateTtl(rank),
                    rank.getPersistedConfig());
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalStateException("Could not inspect StreamFusion Top-N support", e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException("StreamFusion Top-N support inspection failed", e.getCause());
        }
    }

    static String unsupportedReason(StreamExecWindowRank rank, ProcessorContext context) {
        if (windowRankType(rank) != RankType.ROW_NUMBER) {
            return "rank type: Flink Window Top-N only implements ROW_NUMBER";
        }
        RankRange range = windowRankRange(rank);
        if (!(range instanceof ConstantRankRange)) {
            return "rank range: Window Top-N requires a constant range";
        }
        ConstantRankRange constant = (ConstantRankRange) range;
        ExecEdge input = rank.getInputEdges().get(0);
        try {
            Class<?> translator = Class.forName(
                    WINDOW_RANK_TRANSLATOR_CLASS, true, StreamFusionRuntimeClasses.class.getClassLoader());
            Method method = translator.getMethod(
                    "unsupportedReason",
                    RowType.class,
                    RowType.class,
                    int[].class,
                    SortSpec.class,
                    long.class,
                    long.class,
                    boolean.class,
                    WindowingStrategy.class,
                    ReadableConfig.class);
            return (String) method.invoke(
                    null,
                    (RowType) input.getOutputType(),
                    (RowType) rank.getOutputType(),
                    windowRankPartitionKeys(rank),
                    windowRankSortSpec(rank),
                    constant.getRankStart(),
                    constant.getRankEnd(),
                    windowRankOutputNumber(rank),
                    windowRankWindowing(rank),
                    rank.getPersistedConfig());
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalStateException("Could not inspect StreamFusion WindowRank support", e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException("StreamFusion WindowRank support inspection failed", e.getCause());
        }
    }

    static String unsupportedReason(StreamExecTemporalSort sort, ProcessorContext context) {
        SortSpec sortSpec = temporalSortSpec(sort);
        try {
            Class<?> translator = Class.forName(
                    TEMPORAL_SORT_TRANSLATOR_CLASS,
                    true,
                    context.getPlanner().getFlinkContext().getClassLoader());
            Method method = translator.getMethod(
                    "unsupportedReason", RowType.class, SortSpec.class, boolean.class, ReadableConfig.class);
            return (String) method.invoke(
                    null,
                    (RowType) sort.getInputEdges().get(0).getOutputType(),
                    sortSpec,
                    temporalSortProcessingTime(sort, sortSpec),
                    sort.getPersistedConfig());
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException failure) {
            throw new IllegalStateException("Could not inspect StreamFusion TemporalSort support", failure);
        } catch (InvocationTargetException failure) {
            throw new IllegalStateException("StreamFusion TemporalSort support inspection failed", failure.getCause());
        }
    }

    static String unsupportedReason(StreamExecSort sort, ProcessorContext context) {
        try {
            Class<?> translator = Class.forName(
                    BOUNDED_SORT_TRANSLATOR_CLASS,
                    true,
                    context.getPlanner().getFlinkContext().getClassLoader());
            Method method =
                    translator.getMethod("unsupportedReason", RowType.class, SortSpec.class, ReadableConfig.class);
            return (String) method.invoke(
                    null,
                    (RowType) sort.getInputEdges().get(0).getOutputType(),
                    boundedSortSpec(sort),
                    sort.getPersistedConfig());
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException failure) {
            throw new IllegalStateException("Could not inspect StreamFusion bounded sort support", failure);
        } catch (InvocationTargetException failure) {
            throw new IllegalStateException("StreamFusion bounded sort support inspection failed", failure.getCause());
        }
    }

    static String unsupportedReason(BatchExecSort sort, ProcessorContext context) {
        try {
            Class<?> translator = Class.forName(
                    BOUNDED_SORT_TRANSLATOR_CLASS,
                    true,
                    context.getPlanner().getFlinkContext().getClassLoader());
            Method method =
                    translator.getMethod("unsupportedReason", RowType.class, SortSpec.class, ReadableConfig.class);
            return (String) method.invoke(
                    null,
                    (RowType) sort.getInputEdges().get(0).getOutputType(),
                    boundedSortSpec(sort),
                    sort.getPersistedConfig());
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException failure) {
            throw new IllegalStateException("Could not inspect StreamFusion batch bounded sort support", failure);
        } catch (InvocationTargetException failure) {
            throw new IllegalStateException(
                    "StreamFusion batch bounded sort support inspection failed", failure.getCause());
        }
    }

    static String unsupportedReason(BatchExecSortLimit sort, ProcessorContext context) {
        try {
            Class<?> translator = Class.forName(
                    BOUNDED_SORT_LIMIT_TRANSLATOR_CLASS,
                    true,
                    context.getPlanner().getFlinkContext().getClassLoader());
            Method method = translator.getMethod(
                    "unsupportedReason", RowType.class, SortSpec.class, long.class, long.class, ReadableConfig.class);
            return (String) method.invoke(
                    null,
                    (RowType) sort.getInputEdges().get(0).getOutputType(),
                    boundedSortLimitSpec(sort),
                    boundedSortLimitStart(sort),
                    boundedSortLimitEnd(sort),
                    sort.getPersistedConfig());
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException failure) {
            throw new IllegalStateException("Could not inspect StreamFusion bounded SortLimit support", failure);
        } catch (InvocationTargetException failure) {
            throw new IllegalStateException(
                    "StreamFusion bounded SortLimit support inspection failed", failure.getCause());
        }
    }

    static String unsupportedReason(BatchExecLimit limit, ProcessorContext context) {
        try {
            Class<?> translator = Class.forName(
                    BOUNDED_LIMIT_TRANSLATOR_CLASS,
                    true,
                    context.getPlanner().getFlinkContext().getClassLoader());
            Method method = translator.getMethod("unsupportedReason", RowType.class, long.class, long.class);
            return (String) method.invoke(
                    null,
                    (RowType) limit.getInputEdges().get(0).getOutputType(),
                    boundedLimitStart(limit),
                    boundedLimitEnd(limit));
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException failure) {
            throw new IllegalStateException("Could not inspect StreamFusion bounded LIMIT support", failure);
        } catch (InvocationTargetException failure) {
            throw new IllegalStateException("StreamFusion bounded LIMIT support inspection failed", failure.getCause());
        }
    }

    static String unsupportedReason(BatchExecRank rank, ProcessorContext context) {
        try {
            Class<?> translator = Class.forName(
                    BOUNDED_RANK_TRANSLATOR_CLASS,
                    true,
                    context.getPlanner().getFlinkContext().getClassLoader());
            Method method = translator.getMethod(
                    "unsupportedReason",
                    RowType.class,
                    RowType.class,
                    int[].class,
                    int[].class,
                    long.class,
                    long.class,
                    boolean.class);
            return (String) method.invoke(
                    null,
                    (RowType) rank.getInputEdges().get(0).getOutputType(),
                    (RowType) rank.getOutputType(),
                    boundedRankPartitionFields(rank),
                    boundedRankSortFields(rank),
                    boundedRankStart(rank),
                    boundedRankEnd(rank),
                    boundedRankOutputNumber(rank));
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException failure) {
            throw new IllegalStateException("Could not inspect StreamFusion bounded RANK support", failure);
        } catch (InvocationTargetException failure) {
            throw new IllegalStateException("StreamFusion bounded RANK support inspection failed", failure.getCause());
        }
    }
}
