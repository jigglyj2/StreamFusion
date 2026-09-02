/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.spec.SortSpec;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

/** Distinct StreamFusion physical node for streaming non-window Top-N/ROW_NUMBER. */
public final class StreamFusionExecRank extends ExecNodeBase<RowData> implements StreamExecNode<RowData> {
    private static final String TRANSLATOR_CLASS = "tech.streamfusion.flink.topn.StreamFusionTopNTranslator";

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

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner, ExecNodeConfig config) {
        ExecEdge edge = getInputEdges().get(0);
        Transformation<RowData> input = (Transformation<RowData>) edge.translateToPlan(planner);
        RowType inputType = (RowType) edge.getOutputType();
        InternalTypeInfo<RowData> inputInfo = InternalTypeInfo.of(inputType);
        RowDataKeySelector partitionSelector = KeySelectorUtil.getRowDataSelector(
                planner.getFlinkContext().getClassLoader(), partitionKeys, inputInfo);
        try {
            Class<?> translator = Class.forName(
                    TRANSLATOR_CLASS, true, planner.getFlinkContext().getClassLoader());
            Method method = translator.getMethod(
                    "translate",
                    Transformation.class,
                    RowType.class,
                    RowType.class,
                    int[].class,
                    SortSpec.class,
                    int[].class,
                    long.class,
                    Long.class,
                    Integer.class,
                    boolean.class,
                    boolean.class,
                    String.class,
                    long.class,
                    ReadableConfig.class,
                    org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.class,
                    RowDataKeySelector.class);
            Transformation<RowData> result = (Transformation<RowData>) method.invoke(
                    null,
                    input,
                    inputType,
                    (RowType) getOutputType(),
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
                    getPersistedConfig(),
                    planner.getExecEnv(),
                    partitionSelector);
            if (result == null) {
                throw new IllegalStateException("A selected StreamFusion Rank failed translation");
            }
            return result;
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalStateException("Could not invoke the StreamFusion Rank runtime", e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException("StreamFusion Rank translation failed", e.getCause());
        }
    }
}
