/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.planner.plan.logical.LogicalWindow;
import org.apache.flink.table.planner.plan.logical.TimeAttributeWindowingStrategy;
import org.apache.flink.table.planner.plan.logical.WindowingStrategy;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.StateMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecAdaptiveJoin;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecHashAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecHashJoin;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecHashWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecLimit;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecNestedLoopJoin;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecRank;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecSort;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecSortAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecSortLimit;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecSortMergeJoin;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecSortWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecCalc;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecExpand;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecWindowTableFunction;
import org.apache.flink.table.planner.plan.nodes.exec.spec.IntervalJoinSpec;
import org.apache.flink.table.planner.plan.nodes.exec.spec.JoinSpec;
import org.apache.flink.table.planner.plan.nodes.exec.spec.OverSpec;
import org.apache.flink.table.planner.plan.nodes.exec.spec.PartitionSpec;
import org.apache.flink.table.planner.plan.nodes.exec.spec.SortSpec;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecChangelogNormalize;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecDeduplicate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecGlobalGroupAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecGroupAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecIncrementalGroupAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecIntervalJoin;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecJoin;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecLocalGroupAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecMultiJoin;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecOverAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecRank;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecSort;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecTemporalJoin;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecTemporalSort;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecWatermarkAssigner;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecWindowDeduplicate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecWindowJoin;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecWindowRank;
import org.apache.flink.table.planner.plan.utils.RankProcessStrategy;
import org.apache.flink.table.runtime.groupwindow.NamedWindowProperty;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.stream.keyselector.AttributeBasedJoinKeyExtractor;
import org.apache.flink.table.runtime.operators.join.stream.keyselector.AttributeBasedJoinKeyExtractor.ConditionAttributeRef;
import org.apache.flink.table.runtime.operators.rank.RankRange;
import org.apache.flink.table.runtime.operators.rank.RankType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.util.TimeUtils;

/** Typed access to Flink exec-node fields that do not have public compatibility APIs. */
final class FlinkExecNodeAccess {
    private FlinkExecNodeAccess() {}

    static List<RexNode> projection(CommonExecCalc calc) {
        return (List<RexNode>) field(calc, CommonExecCalc.class, "projection");
    }

    static RexNode condition(CommonExecCalc calc) {
        return (RexNode) field(calc, CommonExecCalc.class, "condition");
    }

    @SuppressWarnings("unchecked")
    static List<List<RexNode>> projects(CommonExecExpand expand) {
        return (List<List<RexNode>>) field(expand, CommonExecExpand.class, "projects");
    }

    static TimeAttributeWindowingStrategy windowStrategy(CommonExecWindowTableFunction window) {
        return (TimeAttributeWindowingStrategy) field(window, CommonExecWindowTableFunction.class, "windowingStrategy");
    }

    static RexNode watermarkExpression(StreamExecWatermarkAssigner watermark) {
        return (RexNode) field(watermark, StreamExecWatermarkAssigner.class, "watermarkExpr");
    }

    static int watermarkRowtimeFieldIndex(StreamExecWatermarkAssigner watermark) {
        return (int) field(watermark, StreamExecWatermarkAssigner.class, "rowtimeFieldIndex");
    }

    static int[] uniqueKeys(StreamExecDeduplicate deduplicate) {
        return ((int[]) field(deduplicate, StreamExecDeduplicate.class, "uniqueKeys")).clone();
    }

    static boolean booleanField(StreamExecDeduplicate deduplicate, String name) {
        return (boolean) field(deduplicate, StreamExecDeduplicate.class, name);
    }

    @SuppressWarnings("unchecked")
    static List<StateMetadata> stateMetadata(StreamExecDeduplicate deduplicate) {
        return (List<StateMetadata>) field(deduplicate, StreamExecDeduplicate.class, "stateMetadataList");
    }

    static long stateTtl(StreamExecDeduplicate deduplicate) {
        List<StateMetadata> metadata = stateMetadata(deduplicate);
        if (metadata == null || metadata.isEmpty()) {
            return deduplicate
                    .getPersistedConfig()
                    .get(ExecutionConfigOptions.IDLE_STATE_RETENTION)
                    .toMillis();
        }
        return TimeUtils.parseDuration(metadata.get(0).getStateTtl()).toMillis();
    }

    static int[] changelogNormalizeUniqueKeys(StreamExecChangelogNormalize normalize) {
        return ((int[]) field(normalize, StreamExecChangelogNormalize.class, "uniqueKeys")).clone();
    }

    static boolean changelogNormalizeGenerateUpdateBefore(StreamExecChangelogNormalize normalize) {
        return (boolean) field(normalize, StreamExecChangelogNormalize.class, "generateUpdateBefore");
    }

    static RexNode changelogNormalizeFilter(StreamExecChangelogNormalize normalize) {
        return (RexNode) field(normalize, StreamExecChangelogNormalize.class, "filterCondition");
    }

    @SuppressWarnings("unchecked")
    static List<StateMetadata> changelogNormalizeStateMetadata(StreamExecChangelogNormalize normalize) {
        return (List<StateMetadata>) field(normalize, StreamExecChangelogNormalize.class, "stateMetadataList");
    }

    static int[] grouping(StreamExecGroupAggregate aggregate) {
        return ((int[]) field(aggregate, StreamExecGroupAggregate.class, "grouping")).clone();
    }

    static int[] grouping(BatchExecHashAggregate aggregate) {
        return ((int[]) field(aggregate, BatchExecHashAggregate.class, "grouping")).clone();
    }

    static int[] auxiliaryGrouping(BatchExecHashAggregate aggregate) {
        return ((int[]) field(aggregate, BatchExecHashAggregate.class, "auxGrouping")).clone();
    }

    static org.apache.calcite.rel.core.AggregateCall[] aggregateCalls(BatchExecHashAggregate aggregate) {
        return ((org.apache.calcite.rel.core.AggregateCall[])
                        field(aggregate, BatchExecHashAggregate.class, "aggCalls"))
                .clone();
    }

    static RowType aggregateInputType(BatchExecHashAggregate aggregate) {
        return (RowType) field(aggregate, BatchExecHashAggregate.class, "aggInputRowType");
    }

    static boolean batchAggregateBooleanField(BatchExecHashAggregate aggregate, String name) {
        return (boolean) field(aggregate, BatchExecHashAggregate.class, name);
    }

    static JoinSpec batchHashJoinSpec(BatchExecHashJoin join) {
        return (JoinSpec) field(join, BatchExecHashJoin.class, "joinSpec");
    }

    static JoinSpec batchAdaptiveJoinSpec(BatchExecAdaptiveJoin join) {
        return (JoinSpec) field(join, BatchExecAdaptiveJoin.class, "joinSpec");
    }

    static JoinSpec batchSortMergeJoinSpec(BatchExecSortMergeJoin join) {
        return new JoinSpec(
                (org.apache.flink.table.runtime.operators.join.FlinkJoinType)
                        field(join, BatchExecSortMergeJoin.class, "joinType"),
                ((int[]) field(join, BatchExecSortMergeJoin.class, "leftKeys")).clone(),
                ((int[]) field(join, BatchExecSortMergeJoin.class, "rightKeys")).clone(),
                ((boolean[]) field(join, BatchExecSortMergeJoin.class, "filterNulls")).clone(),
                (RexNode) field(join, BatchExecSortMergeJoin.class, "nonEquiCondition"));
    }

    static JoinSpec batchNestedLoopJoinSpec(BatchExecNestedLoopJoin join) {
        return new JoinSpec(
                (org.apache.flink.table.runtime.operators.join.FlinkJoinType)
                        field(join, BatchExecNestedLoopJoin.class, "joinType"),
                new int[0],
                new int[0],
                new boolean[0],
                (RexNode) field(join, BatchExecNestedLoopJoin.class, "condition"));
    }

    static boolean batchNestedLoopJoinSingleRow(BatchExecNestedLoopJoin join) {
        return (boolean) field(join, BatchExecNestedLoopJoin.class, "singleRowJoin");
    }

    static int[] grouping(BatchExecSortAggregate aggregate) {
        return ((int[]) field(aggregate, BatchExecSortAggregate.class, "grouping")).clone();
    }

    static int[] auxiliaryGrouping(BatchExecSortAggregate aggregate) {
        return ((int[]) field(aggregate, BatchExecSortAggregate.class, "auxGrouping")).clone();
    }

    static org.apache.calcite.rel.core.AggregateCall[] aggregateCalls(BatchExecSortAggregate aggregate) {
        return ((org.apache.calcite.rel.core.AggregateCall[])
                        field(aggregate, BatchExecSortAggregate.class, "aggCalls"))
                .clone();
    }

    static RowType aggregateInputType(BatchExecSortAggregate aggregate) {
        return (RowType) field(aggregate, BatchExecSortAggregate.class, "aggInputRowType");
    }

    static boolean batchAggregateBooleanField(BatchExecSortAggregate aggregate, String name) {
        return (boolean) field(aggregate, BatchExecSortAggregate.class, name);
    }

    static int[] batchWindowGrouping(Object aggregate) {
        return ((int[]) field(aggregate, batchWindowClass(aggregate), "grouping")).clone();
    }

    static int[] batchWindowAuxiliaryGrouping(Object aggregate) {
        return ((int[]) field(aggregate, batchWindowClass(aggregate), "auxGrouping")).clone();
    }

    static org.apache.calcite.rel.core.AggregateCall[] batchWindowAggregateCalls(Object aggregate) {
        return ((org.apache.calcite.rel.core.AggregateCall[]) field(aggregate, batchWindowClass(aggregate), "aggCalls"))
                .clone();
    }

    static LogicalWindow batchWindow(Object aggregate) {
        return (LogicalWindow) field(aggregate, batchWindowClass(aggregate), "window");
    }

    static NamedWindowProperty[] batchWindowProperties(Object aggregate) {
        return ((NamedWindowProperty[]) field(aggregate, batchWindowClass(aggregate), "namedWindowProperties")).clone();
    }

    static RowType batchWindowInputType(Object aggregate) {
        return (RowType) field(aggregate, batchWindowClass(aggregate), "aggInputRowType");
    }

    static boolean batchWindowBoolean(Object aggregate, String name) {
        return (boolean) field(aggregate, batchWindowClass(aggregate), name);
    }

    private static Class<?> batchWindowClass(Object aggregate) {
        if (aggregate instanceof BatchExecHashWindowAggregate) {
            return BatchExecHashWindowAggregate.class;
        }
        if (aggregate instanceof BatchExecSortWindowAggregate) {
            return BatchExecSortWindowAggregate.class;
        }
        throw new IllegalArgumentException(
                "Not a bounded window aggregate: " + aggregate.getClass().getName());
    }

    static OverSpec overSpec(StreamExecOverAggregate aggregate) {
        return (OverSpec) field(aggregate, StreamExecOverAggregate.class, "overSpec");
    }

    static org.apache.calcite.rel.core.AggregateCall[] aggregateCalls(StreamExecGroupAggregate aggregate) {
        return ((org.apache.calcite.rel.core.AggregateCall[])
                        field(aggregate, StreamExecGroupAggregate.class, "aggCalls"))
                .clone();
    }

    static boolean[] aggregateCallNeedRetractions(StreamExecGroupAggregate aggregate) {
        return ((boolean[]) field(aggregate, StreamExecGroupAggregate.class, "aggCallNeedRetractions")).clone();
    }

    static boolean aggregateBooleanField(StreamExecGroupAggregate aggregate, String name) {
        return (boolean) field(aggregate, StreamExecGroupAggregate.class, name);
    }

    @SuppressWarnings("unchecked")
    static List<StateMetadata> aggregateStateMetadata(StreamExecGroupAggregate aggregate) {
        return (List<StateMetadata>) field(aggregate, StreamExecGroupAggregate.class, "stateMetadataList");
    }

    static long aggregateStateTtl(StreamExecGroupAggregate aggregate) {
        List<StateMetadata> metadata = aggregateStateMetadata(aggregate);
        if (metadata == null || metadata.isEmpty()) {
            return aggregate
                    .getPersistedConfig()
                    .get(ExecutionConfigOptions.IDLE_STATE_RETENTION)
                    .toMillis();
        }
        return TimeUtils.parseDuration(metadata.get(0).getStateTtl()).toMillis();
    }

    static int[] localGroupGrouping(StreamExecLocalGroupAggregate aggregate) {
        return ((int[]) field(aggregate, StreamExecLocalGroupAggregate.class, "grouping")).clone();
    }

    static org.apache.calcite.rel.core.AggregateCall[] localGroupAggregateCalls(
            StreamExecLocalGroupAggregate aggregate) {
        return ((org.apache.calcite.rel.core.AggregateCall[])
                        field(aggregate, StreamExecLocalGroupAggregate.class, "aggCalls"))
                .clone();
    }

    static boolean[] localGroupCallNeedRetractions(StreamExecLocalGroupAggregate aggregate) {
        return ((boolean[]) field(aggregate, StreamExecLocalGroupAggregate.class, "aggCallNeedRetractions")).clone();
    }

    static boolean localGroupNeedRetraction(StreamExecLocalGroupAggregate aggregate) {
        return (boolean) field(aggregate, StreamExecLocalGroupAggregate.class, "needRetraction");
    }

    static int[] globalGroupGrouping(StreamExecGlobalGroupAggregate aggregate) {
        return ((int[]) field(aggregate, StreamExecGlobalGroupAggregate.class, "grouping")).clone();
    }

    static org.apache.calcite.rel.core.AggregateCall[] globalGroupAggregateCalls(
            StreamExecGlobalGroupAggregate aggregate) {
        return ((org.apache.calcite.rel.core.AggregateCall[])
                        field(aggregate, StreamExecGlobalGroupAggregate.class, "aggCalls"))
                .clone();
    }

    static boolean[] globalGroupCallNeedRetractions(StreamExecGlobalGroupAggregate aggregate) {
        return ((boolean[]) field(aggregate, StreamExecGlobalGroupAggregate.class, "aggCallNeedRetractions")).clone();
    }

    static boolean globalGroupNeedRetraction(StreamExecGlobalGroupAggregate aggregate) {
        return (boolean) field(aggregate, StreamExecGlobalGroupAggregate.class, "needRetraction");
    }

    static boolean globalGroupGenerateUpdateBefore(StreamExecGlobalGroupAggregate aggregate) {
        return (boolean) field(aggregate, StreamExecGlobalGroupAggregate.class, "generateUpdateBefore");
    }

    @SuppressWarnings("unchecked")
    static List<StateMetadata> globalGroupStateMetadata(StreamExecGlobalGroupAggregate aggregate) {
        return (List<StateMetadata>) field(aggregate, StreamExecGlobalGroupAggregate.class, "stateMetadataList");
    }

    static RowType globalGroupOriginalInputType(StreamExecGlobalGroupAggregate aggregate) {
        return (RowType) field(aggregate, StreamExecGlobalGroupAggregate.class, "localAggInputRowType");
    }

    @SuppressWarnings("unchecked")
    static long globalGroupStateTtl(StreamExecGlobalGroupAggregate aggregate) {
        List<StateMetadata> metadata = globalGroupStateMetadata(aggregate);
        if (metadata == null || metadata.isEmpty()) {
            return aggregate
                    .getPersistedConfig()
                    .get(ExecutionConfigOptions.IDLE_STATE_RETENTION)
                    .toMillis();
        }
        return TimeUtils.parseDuration(metadata.get(0).getStateTtl()).toMillis();
    }

    static org.apache.calcite.rel.core.AggregateCall[] incrementalOriginalCalls(
            StreamExecIncrementalGroupAggregate aggregate) {
        return ((org.apache.calcite.rel.core.AggregateCall[])
                        field(aggregate, StreamExecIncrementalGroupAggregate.class, "partialOriginalAggCalls"))
                .clone();
    }

    static int[] incrementalFinalGrouping(StreamExecIncrementalGroupAggregate aggregate) {
        return ((int[]) field(aggregate, StreamExecIncrementalGroupAggregate.class, "finalAggGrouping")).clone();
    }

    static RowType incrementalPartialOriginalInputType(StreamExecIncrementalGroupAggregate aggregate) {
        return (RowType) field(aggregate, StreamExecIncrementalGroupAggregate.class, "partialLocalAggInputType");
    }

    static boolean[] incrementalCallNeedRetractions(StreamExecIncrementalGroupAggregate aggregate) {
        return ((boolean[])
                        field(aggregate, StreamExecIncrementalGroupAggregate.class, "partialAggCallNeedRetractions"))
                .clone();
    }

    static boolean incrementalNeedRetraction(StreamExecIncrementalGroupAggregate aggregate) {
        return (boolean) field(aggregate, StreamExecIncrementalGroupAggregate.class, "partialAggNeedRetraction");
    }

    @SuppressWarnings("unchecked")
    static long incrementalStateTtl(StreamExecIncrementalGroupAggregate aggregate) {
        List<StateMetadata> metadata =
                (List<StateMetadata>) field(aggregate, StreamExecIncrementalGroupAggregate.class, "stateMetadataList");
        if (metadata == null || metadata.isEmpty()) {
            return aggregate
                    .getPersistedConfig()
                    .get(ExecutionConfigOptions.IDLE_STATE_RETENTION)
                    .toMillis();
        }
        return TimeUtils.parseDuration(metadata.get(0).getStateTtl()).toMillis();
    }

    static RowType nativeGroupAccumulatorType(RowType inputType, int[] grouping) {
        List<RowType.RowField> fields = new ArrayList<>(grouping.length + 1);
        for (int index : grouping) {
            RowType.RowField field = inputType.getFields().get(index);
            fields.add(new RowType.RowField(field.getName(), field.getType()));
        }
        fields.add(
                new RowType.RowField("__streamfusion_accumulator", new VarBinaryType(false, VarBinaryType.MAX_LENGTH)));
        return new RowType(false, fields);
    }

    static RowType nativeGroupingAccumulatorType(RowType inputType, int[] grouping) {
        List<RowType.RowField> fields = new ArrayList<>(grouping.length + 1);
        for (int index : grouping) {
            RowType.RowField field = inputType.getFields().get(index);
            fields.add(new RowType.RowField(field.getName(), field.getType()));
        }
        fields.add(
                new RowType.RowField("__streamfusion_accumulator", new VarBinaryType(false, VarBinaryType.MAX_LENGTH)));
        return new RowType(false, fields);
    }

    static RowType nativeWindowAccumulatorType(RowType inputType, int[] grouping) {
        List<RowType.RowField> fields = new ArrayList<>(grouping.length + 3);
        for (int index : grouping) {
            RowType.RowField field = inputType.getFields().get(index);
            fields.add(new RowType.RowField(field.getName(), field.getType()));
        }
        fields.add(
                new RowType.RowField("__streamfusion_accumulator", new VarBinaryType(false, VarBinaryType.MAX_LENGTH)));
        fields.add(new RowType.RowField("__streamfusion_window_start", new BigIntType(false)));
        fields.add(new RowType.RowField("__streamfusion_slice_end", new BigIntType(false)));
        return new RowType(false, fields);
    }

    static RowType aggregateOutputType(
            RowType inputType, int[] grouping, org.apache.calcite.rel.core.AggregateCall[] calls) {
        List<RowType.RowField> fields = new ArrayList<>(grouping.length + calls.length);
        for (int index : grouping) {
            RowType.RowField field = inputType.getFields().get(index);
            fields.add(new RowType.RowField(field.getName(), field.getType()));
        }
        for (int index = 0; index < calls.length; index++) {
            fields.add(new RowType.RowField(
                    "aggregate_" + index,
                    org.apache.flink.table.planner.calcite.FlinkTypeFactory.toLogicalType(calls[index].getType())));
        }
        return new RowType(false, fields);
    }

    static int[] windowGrouping(StreamExecWindowAggregate aggregate) {
        return ((int[]) field(aggregate, StreamExecWindowAggregate.class, "grouping")).clone();
    }

    static int[] windowDeduplicatePartitionKeys(StreamExecWindowDeduplicate deduplicate) {
        return ((int[]) field(deduplicate, StreamExecWindowDeduplicate.class, "partitionKeys")).clone();
    }

    static int windowDeduplicateOrderKey(StreamExecWindowDeduplicate deduplicate) {
        return (int) field(deduplicate, StreamExecWindowDeduplicate.class, "orderKey");
    }

    static boolean windowDeduplicateKeepLast(StreamExecWindowDeduplicate deduplicate) {
        return (boolean) field(deduplicate, StreamExecWindowDeduplicate.class, "keepLastRow");
    }

    static WindowingStrategy windowDeduplicateWindowing(StreamExecWindowDeduplicate deduplicate) {
        return (WindowingStrategy) field(deduplicate, StreamExecWindowDeduplicate.class, "windowing");
    }

    static RankType rankType(StreamExecRank rank) {
        return (RankType) field(rank, StreamExecRank.class, "rankType");
    }

    static int[] rankPartitionKeys(StreamExecRank rank) {
        return ((PartitionSpec) field(rank, StreamExecRank.class, "partitionSpec")).getFieldIndices();
    }

    static SortSpec rankSortSpec(StreamExecRank rank) {
        return (SortSpec) field(rank, StreamExecRank.class, "sortSpec");
    }

    static SortSpec temporalSortSpec(StreamExecTemporalSort sort) {
        return (SortSpec) field(sort, StreamExecTemporalSort.class, "sortSpec");
    }

    static SortSpec boundedSortSpec(StreamExecSort sort) {
        return (SortSpec) field(sort, StreamExecSort.class, "sortSpec");
    }

    static SortSpec boundedSortSpec(BatchExecSort sort) {
        return (SortSpec) field(sort, BatchExecSort.class, "sortSpec");
    }

    static SortSpec boundedSortLimitSpec(BatchExecSortLimit sort) {
        return (SortSpec) field(sort, BatchExecSortLimit.class, "sortSpec");
    }

    static long boundedSortLimitStart(BatchExecSortLimit sort) {
        return (long) field(sort, BatchExecSortLimit.class, "limitStart");
    }

    static long boundedSortLimitEnd(BatchExecSortLimit sort) {
        return (long) field(sort, BatchExecSortLimit.class, "limitEnd");
    }

    static boolean boundedSortLimitGlobal(BatchExecSortLimit sort) {
        return (boolean) field(sort, BatchExecSortLimit.class, "isGlobal");
    }

    static long boundedLimitStart(BatchExecLimit limit) {
        return (long) field(limit, BatchExecLimit.class, "limitStart");
    }

    static long boundedLimitEnd(BatchExecLimit limit) {
        return (long) field(limit, BatchExecLimit.class, "limitEnd");
    }

    static boolean boundedLimitGlobal(BatchExecLimit limit) {
        return (boolean) field(limit, BatchExecLimit.class, "isGlobal");
    }

    static int[] boundedRankPartitionFields(BatchExecRank rank) {
        return ((int[]) field(rank, BatchExecRank.class, "partitionFields")).clone();
    }

    static int[] boundedRankSortFields(BatchExecRank rank) {
        return ((int[]) field(rank, BatchExecRank.class, "sortFields")).clone();
    }

    static long boundedRankStart(BatchExecRank rank) {
        return (long) field(rank, BatchExecRank.class, "rankStart");
    }

    static long boundedRankEnd(BatchExecRank rank) {
        return (long) field(rank, BatchExecRank.class, "rankEnd");
    }

    static boolean boundedRankOutputNumber(BatchExecRank rank) {
        return (boolean) field(rank, BatchExecRank.class, "outputRankNumber");
    }

    static boolean temporalSortProcessingTime(StreamExecTemporalSort sort, SortSpec sortSpec) {
        int timeIndex = sortSpec.getFieldSpec(0).getFieldIndex();
        RowType inputType = (RowType) sort.getInputEdges().get(0).getOutputType();
        return org.apache.flink.table.types.logical.utils.LogicalTypeChecks.isProctimeAttribute(
                inputType.getTypeAt(timeIndex));
    }

    static RankRange rankRange(StreamExecRank rank) {
        return (RankRange) field(rank, StreamExecRank.class, "rankRange");
    }

    static RankProcessStrategy rankStrategy(StreamExecRank rank) {
        return (RankProcessStrategy) field(rank, StreamExecRank.class, "rankStrategy");
    }

    static String rankStrategyName(StreamExecRank rank) {
        RankProcessStrategy strategy = rankStrategy(rank);
        if (strategy instanceof RankProcessStrategy.AppendFastStrategy) {
            return "APPEND_FAST";
        }
        if (strategy instanceof RankProcessStrategy.UpdateFastStrategy) {
            return "UPDATE_FAST";
        }
        if (strategy instanceof RankProcessStrategy.RetractStrategy) {
            return "RETRACT";
        }
        return null;
    }

    static int[] rankPrimaryKeys(StreamExecRank rank) {
        RankProcessStrategy strategy = rankStrategy(rank);
        return strategy instanceof RankProcessStrategy.UpdateFastStrategy
                ? ((RankProcessStrategy.UpdateFastStrategy) strategy)
                        .getPrimaryKeys()
                        .clone()
                : new int[0];
    }

    static boolean rankOutputNumber(StreamExecRank rank) {
        return (boolean) field(rank, StreamExecRank.class, "outputRankNumber");
    }

    static boolean rankGenerateUpdateBefore(StreamExecRank rank) {
        return (boolean) field(rank, StreamExecRank.class, "generateUpdateBefore");
    }

    @SuppressWarnings("unchecked")
    static long rankStateTtl(StreamExecRank rank) {
        List<StateMetadata> metadata = (List<StateMetadata>) field(rank, StreamExecRank.class, "stateMetadataList");
        if (metadata == null || metadata.isEmpty()) {
            return rank.getPersistedConfig()
                    .get(ExecutionConfigOptions.IDLE_STATE_RETENTION)
                    .toMillis();
        }
        return TimeUtils.parseDuration(metadata.get(0).getStateTtl()).toMillis();
    }

    static RankType windowRankType(StreamExecWindowRank rank) {
        return (RankType) field(rank, StreamExecWindowRank.class, "rankType");
    }

    static int[] windowRankPartitionKeys(StreamExecWindowRank rank) {
        return ((PartitionSpec) field(rank, StreamExecWindowRank.class, "partitionSpec")).getFieldIndices();
    }

    static SortSpec windowRankSortSpec(StreamExecWindowRank rank) {
        return (SortSpec) field(rank, StreamExecWindowRank.class, "sortSpec");
    }

    static RankRange windowRankRange(StreamExecWindowRank rank) {
        return (RankRange) field(rank, StreamExecWindowRank.class, "rankRange");
    }

    static boolean windowRankOutputNumber(StreamExecWindowRank rank) {
        return (boolean) field(rank, StreamExecWindowRank.class, "outputRankNumber");
    }

    static WindowingStrategy windowRankWindowing(StreamExecWindowRank rank) {
        return (WindowingStrategy) field(rank, StreamExecWindowRank.class, "windowing");
    }

    static JoinSpec windowJoinSpec(StreamExecWindowJoin join) {
        return (JoinSpec) field(join, StreamExecWindowJoin.class, "joinSpec");
    }

    static JoinSpec regularJoinSpec(StreamExecJoin join) {
        return (JoinSpec) field(join, StreamExecJoin.class, "joinSpec");
    }

    @SuppressWarnings("unchecked")
    static List<FlinkJoinType> multiJoinTypes(StreamExecMultiJoin join) {
        return (List<FlinkJoinType>) field(join, StreamExecMultiJoin.class, "joinTypes");
    }

    @SuppressWarnings("unchecked")
    static Map<Integer, List<ConditionAttributeRef>> multiJoinAttributeMap(StreamExecMultiJoin join) {
        return (Map<Integer, List<ConditionAttributeRef>>) field(join, StreamExecMultiJoin.class, "joinAttributeMap");
    }

    @SuppressWarnings("unchecked")
    static List<List<int[]>> multiJoinUniqueKeys(StreamExecMultiJoin join) {
        return (List<List<int[]>>) field(join, StreamExecMultiJoin.class, "inputUniqueKeys");
    }

    @SuppressWarnings("unchecked")
    static List<RexNode> multiJoinConditions(StreamExecMultiJoin join) {
        return (List<RexNode>) field(join, StreamExecMultiJoin.class, "joinConditions");
    }

    static JoinSpec binaryMultiJoinSpec(StreamExecMultiJoin join) {
        if (join.getInputEdges().size() != 2) {
            return null;
        }
        List<FlinkJoinType> joinTypes = multiJoinTypes(join);
        List<RexNode> conditions = multiJoinConditions(join);
        if (joinTypes.size() != 2 || conditions.size() != 2) {
            return null;
        }
        List<RowType> inputTypes = join.getInputEdges().stream()
                .map(edge -> (RowType) edge.getOutputType())
                .collect(Collectors.toList());
        AttributeBasedJoinKeyExtractor extractor =
                new AttributeBasedJoinKeyExtractor(multiJoinAttributeMap(join), inputTypes);
        int[] leftKeys = extractor.getCommonJoinKeyIndices(0);
        int[] rightKeys = extractor.getCommonJoinKeyIndices(1);
        if (leftKeys.length == 0 || leftKeys.length != rightKeys.length) {
            return null;
        }
        boolean[] filterNulls = new boolean[leftKeys.length];
        java.util.Arrays.fill(filterNulls, true);
        return new JoinSpec(joinTypes.get(1), leftKeys, rightKeys, filterNulls, conditions.get(1));
    }

    static boolean multiJoinEquiOnly(StreamExecMultiJoin join) {
        List<RexNode> conditions = multiJoinConditions(join);
        Map<Integer, List<ConditionAttributeRef>> attributes = multiJoinAttributeMap(join);
        List<ExecEdge> inputs = join.getInputEdges();
        if (conditions.size() != inputs.size()) {
            return false;
        }
        for (int depth = 1; depth < conditions.size(); depth++) {
            RexNode condition = conditions.get(depth);
            List<RexNode> conjuncts = condition == null ? List.of() : RelOptUtil.conjunctions(condition);
            Set<String> actual = new HashSet<>();
            int leftArity = 0;
            int[] offsets = new int[depth];
            for (int input = 0; input < depth; input++) {
                offsets[input] = leftArity;
                leftArity += ((RowType) inputs.get(input).getOutputType()).getFieldCount();
            }
            for (RexNode conjunct : conjuncts) {
                if (!(conjunct instanceof RexCall)) {
                    return false;
                }
                RexCall call = (RexCall) conjunct;
                if (call.getKind() != org.apache.calcite.sql.SqlKind.EQUALS
                        || call.getOperands().size() != 2
                        || !(call.getOperands().get(0) instanceof RexInputRef)
                        || !(call.getOperands().get(1) instanceof RexInputRef)) {
                    return false;
                }
                int first = ((RexInputRef) call.getOperands().get(0)).getIndex();
                int second = ((RexInputRef) call.getOperands().get(1)).getIndex();
                int left = Math.min(first, second);
                int right = Math.max(first, second);
                if (left >= leftArity || right < leftArity) {
                    return false;
                }
                actual.add(left + ":" + (right - leftArity));
            }
            Set<String> expected = new HashSet<>();
            for (ConditionAttributeRef attribute : attributes.getOrDefault(depth, List.of())) {
                expected.add(
                        (offsets[attribute.leftInputId] + attribute.leftFieldIndex) + ":" + attribute.rightFieldIndex);
            }
            if (!actual.equals(expected)) {
                return false;
            }
        }
        return true;
    }

    @SuppressWarnings("unchecked")
    static long[] multiJoinStateTtl(StreamExecMultiJoin join) {
        int inputCount = join.getInputEdges().size();
        List<StateMetadata> metadata =
                (List<StateMetadata>) field(join, StreamExecMultiJoin.class, "stateMetadataList");
        long[] ttl = new long[inputCount];
        if (metadata == null || metadata.isEmpty()) {
            java.util.Arrays.fill(
                    ttl,
                    join.getPersistedConfig()
                            .get(ExecutionConfigOptions.IDLE_STATE_RETENTION)
                            .toMillis());
            return ttl;
        }
        boolean[] seen = new boolean[inputCount];
        for (StateMetadata state : metadata) {
            int index = state.getStateIndex();
            if (index < 0 || index >= inputCount || seen[index]) {
                throw new IllegalStateException("Multi-join state TTL indices must be unique and cover every input");
            }
            ttl[index] = TimeUtils.parseDuration(state.getStateTtl()).toMillis();
            seen[index] = true;
        }
        for (boolean present : seen) {
            if (!present) {
                throw new IllegalStateException("Multi-join state TTL must cover every input");
            }
        }
        return ttl;
    }

    static JoinSpec temporalJoinSpec(StreamExecTemporalJoin join) {
        return (JoinSpec) field(join, StreamExecTemporalJoin.class, "joinSpec");
    }

    static boolean temporalJoinFunction(StreamExecTemporalJoin join) {
        return (boolean) field(join, StreamExecTemporalJoin.class, "isTemporalFunctionJoin");
    }

    static int temporalJoinLeftTimeIndex(StreamExecTemporalJoin join) {
        return (int) field(join, StreamExecTemporalJoin.class, "leftTimeAttributeIndex");
    }

    static int temporalJoinRightTimeIndex(StreamExecTemporalJoin join) {
        return (int) field(join, StreamExecTemporalJoin.class, "rightTimeAttributeIndex");
    }

    static IntervalJoinSpec intervalJoinSpec(StreamExecIntervalJoin join) {
        return (IntervalJoinSpec) field(join, StreamExecIntervalJoin.class, "intervalJoinSpec");
    }

    @SuppressWarnings("unchecked")
    static List<int[]> regularJoinLeftUpsertKeys(StreamExecJoin join) {
        return (List<int[]>) field(join, StreamExecJoin.class, "leftUpsertKeys");
    }

    @SuppressWarnings("unchecked")
    static List<int[]> regularJoinRightUpsertKeys(StreamExecJoin join) {
        return (List<int[]>) field(join, StreamExecJoin.class, "rightUpsertKeys");
    }

    @SuppressWarnings("unchecked")
    static List<Long> regularJoinStateTtl(StreamExecJoin join) {
        List<StateMetadata> metadata = (List<StateMetadata>) field(join, StreamExecJoin.class, "stateMetadataList");
        if (metadata == null || metadata.isEmpty()) {
            long fallback = join.getPersistedConfig()
                    .get(ExecutionConfigOptions.IDLE_STATE_RETENTION)
                    .toMillis();
            return List.of(fallback, fallback);
        }
        if (metadata.size() != 2) {
            throw new IllegalStateException("Regular join must define exactly two state TTL entries");
        }
        long[] ttl = new long[2];
        boolean[] seen = new boolean[2];
        for (StateMetadata state : metadata) {
            int index = state.getStateIndex();
            if (index < 0 || index >= 2 || seen[index]) {
                throw new IllegalStateException("Regular join state TTL indices must be unique 0 and 1");
            }
            ttl[index] = TimeUtils.parseDuration(state.getStateTtl()).toMillis();
            seen[index] = true;
        }
        if (!seen[0] || !seen[1]) {
            throw new IllegalStateException("Regular join state TTL indices must contain 0 and 1");
        }
        return List.of(ttl[0], ttl[1]);
    }

    static WindowingStrategy windowJoinLeftWindowing(StreamExecWindowJoin join) {
        return (WindowingStrategy) field(join, StreamExecWindowJoin.class, "leftWindowing");
    }

    static WindowingStrategy windowJoinRightWindowing(StreamExecWindowJoin join) {
        return (WindowingStrategy) field(join, StreamExecWindowJoin.class, "rightWindowing");
    }

    static org.apache.calcite.rel.core.AggregateCall[] windowAggregateCalls(StreamExecWindowAggregate aggregate) {
        return ((org.apache.calcite.rel.core.AggregateCall[])
                        field(aggregate, StreamExecWindowAggregate.class, "aggCalls"))
                .clone();
    }

    static WindowingStrategy windowing(StreamExecWindowAggregate aggregate) {
        return (WindowingStrategy) field(aggregate, StreamExecWindowAggregate.class, "windowing");
    }

    static NamedWindowProperty[] windowProperties(StreamExecWindowAggregate aggregate) {
        return ((NamedWindowProperty[]) field(aggregate, StreamExecWindowAggregate.class, "namedWindowProperties"))
                .clone();
    }

    static boolean windowNeedRetraction(StreamExecWindowAggregate aggregate) {
        return (boolean) field(aggregate, StreamExecWindowAggregate.class, "needRetraction");
    }

    static Object field(Object node, Class<?> declaringClass, String name) {
        try {
            Field field = declaringClass.getDeclaredField(name);
            field.setAccessible(true);
            return field.get(node);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new IllegalStateException("Could not read Flink exec node " + name, e);
        }
    }
}
