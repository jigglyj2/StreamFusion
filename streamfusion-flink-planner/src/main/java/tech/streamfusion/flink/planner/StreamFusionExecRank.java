/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import java.util.Collections;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.spec.SortSpec;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.types.logical.RowType;

/** Distinct StreamFusion physical node for streaming non-window Top-N/ROW_NUMBER. */
public final class StreamFusionExecRank extends ExecNodeBase<RowData>
        implements StreamExecNode<RowData>, StreamFusionNativePlanNode {
    private static final String TRANSLATOR_CLASS = "tech.streamfusion.flink.topn.StreamFusionTopNTranslator";

    private final StreamFusionNativeNodeMetadata nativeMetadata = new StreamFusionNativeNodeMetadata();
    private final int[] partitionKeys;
    private final SortSpec sortSpec;
    private final int[] primaryKeys;
    private final long rankStart;
    private final Long rankEnd;
    private final Integer variableRankEndIndex;
    private final boolean outputRankNumber;
    private final boolean generateUpdateBefore;
    private final String strategy;
    private final long stateTtlMillis;

    public StreamFusionExecRank(
            ReadableConfig config,
            int[] partitionKeys,
            SortSpec sortSpec,
            int[] primaryKeys,
            long rankStart,
            Long rankEnd,
            Integer variableRankEndIndex,
            boolean outputRankNumber,
            boolean generateUpdateBefore,
            String strategy,
            long stateTtlMillis,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        super(
                ExecNodeContext.newNodeId(),
                new ExecNodeContext("streamfusion-exec-rank_1"),
                config,
                Collections.singletonList(inputProperty),
                outputType,
                description);
        this.partitionKeys = partitionKeys.clone();
        this.sortSpec = sortSpec;
        this.primaryKeys = primaryKeys.clone();
        this.rankStart = rankStart;
        this.rankEnd = rankEnd;
        this.variableRankEndIndex = variableRankEndIndex;
        this.outputRankNumber = outputRankNumber;
        this.generateUpdateBefore = generateUpdateBefore;
        this.strategy = strategy;
        this.stateTtlMillis = stateTtlMillis;
    }

    @Override
    public StreamFusionNativeNodeMetadata nativeMetadata() {
        return nativeMetadata;
    }

    @Override
    public boolean ownsNativeKeyedState() {
        return true;
    }

    @Override
    public byte[] nativePlanFragment(PlannerBase planner) {
        Configuration config = Configuration.fromMap(
                planner.getTableConfig().getConfiguration().toMap());
        config.addAll(Configuration.fromMap(getPersistedConfig().toMap()));
        return StreamFusionNativePlanNode.invokeBuilder(
                planner,
                TRANSLATOR_CLASS,
                new Class<?>[] {
                    RowType.class, RowType.class, int[].class, SortSpec.class, int[].class,
                    long.class, Long.class, Integer.class, boolean.class, boolean.class,
                    String.class, long.class, ReadableConfig.class
                },
                getInputEdges().get(0).getOutputType(),
                getOutputType(),
                partitionKeys,
                sortSpec,
                primaryKeys,
                rankStart,
                rankEnd,
                variableRankEndIndex,
                outputRankNumber,
                generateUpdateBefore,
                strategy,
                stateTtlMillis,
                config);
    }

    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner, ExecNodeConfig config) {
        return StreamFusionStatelessRegion.translate(this, planner);
    }
}
