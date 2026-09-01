/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.stream.IntStream;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.sort.ComparatorCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.logical.WindowingStrategy;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.spec.SortSpec;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

/** Distinct StreamFusion physical node for event-time Window Top-N/ROW_NUMBER. */
public final class StreamFusionExecWindowRank extends ExecNodeBase<RowData> implements StreamExecNode<RowData> {
    private static final String TRANSLATOR_CLASS = "tech.streamfusion.flink.window.StreamFusionWindowRankTranslator";

    private final int[] partitionKeys;
    private final SortSpec sortSpec;
    private final long rankStart;
    private final long rankEnd;
    private final boolean outputRankNumber;
    private final WindowingStrategy windowing;

    public StreamFusionExecWindowRank(
            ReadableConfig config,
            int[] partitionKeys,
            SortSpec sortSpec,
            long rankStart,
            long rankEnd,
            boolean outputRankNumber,
            WindowingStrategy windowing,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        super(
                ExecNodeContext.newNodeId(),
                new ExecNodeContext("streamfusion-exec-window-rank_1"),
                config,
                Collections.singletonList(inputProperty),
                outputType,
                description);
        this.partitionKeys = partitionKeys.clone();
        this.sortSpec = sortSpec;
        this.rankStart = rankStart;
        this.rankEnd = rankEnd;
        this.outputRankNumber = outputRankNumber;
        this.windowing = windowing;
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
        int[] sortFields = sortSpec.getFieldIndices();
        RowDataKeySelector sortSelector =
                KeySelectorUtil.getRowDataSelector(planner.getFlinkContext().getClassLoader(), sortFields, inputInfo);
        SortSpec.SortSpecBuilder comparatorSpec = SortSpec.builder();
        IntStream.range(0, sortFields.length)
                .forEach(index -> comparatorSpec.addField(
                        index,
                        sortSpec.getFieldSpec(index).getIsAscendingOrder(),
                        sortSpec.getFieldSpec(index).getNullIsLast()));
        GeneratedRecordComparator comparator = ComparatorCodeGenerator.gen(
                config,
                planner.getFlinkContext().getClassLoader(),
                "StreamFusionWindowRankComparator",
                RowType.of(sortSpec.getFieldTypes(inputType)),
                comparatorSpec.build());
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
                    long.class,
                    long.class,
                    boolean.class,
                    WindowingStrategy.class,
                    ReadableConfig.class,
                    org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.class,
                    RowDataKeySelector.class,
                    RowDataKeySelector.class,
                    GeneratedRecordComparator.class);
            Transformation<RowData> result = (Transformation<RowData>) method.invoke(
                    null,
                    input,
                    inputType,
                    (RowType) getOutputType(),
                    partitionKeys,
                    sortSpec,
                    rankStart,
                    rankEnd,
                    outputRankNumber,
                    windowing,
                    getPersistedConfig(),
                    planner.getExecEnv(),
                    partitionSelector,
                    sortSelector,
                    comparator);
            if (result == null) {
                throw new IllegalStateException("A selected StreamFusion WindowRank failed translation");
            }
            return result;
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalStateException("Could not invoke the StreamFusion WindowRank runtime", e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException("StreamFusion WindowRank translation failed", e.getCause());
        }
    }
}
