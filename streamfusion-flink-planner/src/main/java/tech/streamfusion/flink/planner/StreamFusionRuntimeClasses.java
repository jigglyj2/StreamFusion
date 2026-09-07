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
import static tech.streamfusion.flink.planner.StreamFusionStatelessSupport.*;
import static tech.streamfusion.flink.planner.StreamFusionWindowAggregateSupport.*;

/** StreamFusion RuntimeClasses for native physical planning. */
final class StreamFusionRuntimeClasses {
    static final String TRANSLATOR_CLASS = "tech.streamfusion.flink.calc.StreamFusionCalcTranslator";

    static final String UNNEST_TRANSLATOR_CLASS = "tech.streamfusion.flink.unnest.StreamFusionArrayUnnestTranslator";

    static final String REPLICATE_ROWS_TRANSLATOR_CLASS =
            "tech.streamfusion.flink.replicate.StreamFusionReplicateRowsTranslator";

    static final String EXPAND_TRANSLATOR_CLASS = "tech.streamfusion.flink.expand.StreamFusionExpandTranslator";

    static final String VALUES_TRANSLATOR_CLASS = "tech.streamfusion.flink.values.StreamFusionValuesTranslator";

    static final String UNION_TRANSLATOR_CLASS = "tech.streamfusion.flink.union.StreamFusionUnionTranslator";

    static final String WINDOW_TRANSLATOR_CLASS =
            "tech.streamfusion.flink.window.StreamFusionWindowTableFunctionTranslator";

    static final String DEDUPLICATE_TRANSLATOR_CLASS =
            "tech.streamfusion.flink.deduplicate.StreamFusionDeduplicateTranslator";

    static final String CHANGELOG_NORMALIZE_TRANSLATOR_CLASS =
            "tech.streamfusion.flink.changelog.StreamFusionChangelogNormalizeTranslator";

    static final String GROUP_AGGREGATE_TRANSLATOR_CLASS =
            "tech.streamfusion.flink.aggregate.StreamFusionGroupAggregateTranslator";

    static final String WINDOW_AGGREGATE_TRANSLATOR_CLASS =
            "tech.streamfusion.flink.window.StreamFusionWindowAggregateTranslator";

    static final String GROUP_WINDOW_AGGREGATE_TRANSLATOR_CLASS =
            "tech.streamfusion.flink.window.StreamFusionGroupWindowAggregateTranslator";

    static final String WINDOW_DEDUPLICATE_TRANSLATOR_CLASS =
            "tech.streamfusion.flink.window.StreamFusionWindowDeduplicateTranslator";

    static final String WINDOW_RANK_TRANSLATOR_CLASS =
            "tech.streamfusion.flink.window.StreamFusionWindowRankTranslator";

    static final String TOP_N_TRANSLATOR_CLASS = "tech.streamfusion.flink.topn.StreamFusionTopNTranslator";

    static final String WINDOW_JOIN_TRANSLATOR_CLASS =
            "tech.streamfusion.flink.window.StreamFusionWindowJoinTranslator";

    static final String REGULAR_JOIN_TRANSLATOR_CLASS =
            "tech.streamfusion.flink.join.StreamFusionRegularJoinTranslator";

    static final String MULTI_JOIN_TRANSLATOR_CLASS = "tech.streamfusion.flink.join.StreamFusionMultiJoinTranslator";

    static final String INTERVAL_JOIN_TRANSLATOR_CLASS =
            "tech.streamfusion.flink.join.StreamFusionIntervalJoinTranslator";

    static final String TEMPORAL_JOIN_TRANSLATOR_CLASS =
            "tech.streamfusion.flink.join.StreamFusionTemporalJoinTranslator";

    static final String OVER_AGGREGATE_TRANSLATOR_CLASS =
            "tech.streamfusion.flink.over.StreamFusionOverAggregateTranslator";

    static final String TEMPORAL_SORT_TRANSLATOR_CLASS =
            "tech.streamfusion.flink.sort.StreamFusionTemporalSortTranslator";

    static final String BOUNDED_SORT_TRANSLATOR_CLASS =
            "tech.streamfusion.flink.sort.StreamFusionBoundedSortTranslator";

    static final String BOUNDED_SORT_LIMIT_TRANSLATOR_CLASS =
            "tech.streamfusion.flink.sort.StreamFusionBoundedSortLimitTranslator";

    static final String BOUNDED_LIMIT_TRANSLATOR_CLASS =
            "tech.streamfusion.flink.limit.StreamFusionBoundedLimitTranslator";

    static final String BOUNDED_RANK_TRANSLATOR_CLASS =
            "tech.streamfusion.flink.rank.StreamFusionBoundedRankTranslator";

    static final String NATIVE_PLAN_CLASS = "tech.streamfusion.proto.plan.v1.NativePlan";

    static final String NATIVE_OPERATOR_CLASS = "tech.streamfusion.proto.plan.v1.Operator";

    static final String NATIVE_PREFLIGHT_CLASS = "tech.streamfusion.nativebridge.NativeRuntimePreflight";
}
