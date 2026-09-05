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
import org.apache.flink.table.planner.codegen.sort.ComparatorCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.spec.SortSpec;
import org.apache.flink.table.planner.plan.utils.SortUtil;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.types.logical.RowType;

/** Distinct StreamFusion physical node for bounded RANK. */
public final class StreamFusionBatchExecRank extends ExecNodeBase<RowData> implements BatchExecNode<RowData> {
    private static final String TRANSLATOR = "tech.streamfusion.flink.rank.StreamFusionBoundedRankTranslator";

    private final int[] partitionFields;
    private final int[] sortFields;
    private final long rankStart;
    private final long rankEnd;
    private final boolean outputRankNumber;
    private final SortSpec inputSortSpec;

    public StreamFusionBatchExecRank(
            ReadableConfig config,
            int[] partitionFields,
            int[] sortFields,
            long rankStart,
            long rankEnd,
            boolean outputRankNumber,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        this(
                config,
                partitionFields,
                sortFields,
                rankStart,
                rankEnd,
                outputRankNumber,
                null,
                inputProperty,
                outputType,
                description);
    }

    public StreamFusionBatchExecRank(
            ReadableConfig config,
            int[] partitionFields,
            int[] sortFields,
            long rankStart,
            long rankEnd,
            boolean outputRankNumber,
            SortSpec inputSortSpec,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        super(
                ExecNodeContext.newNodeId(),
                new ExecNodeContext("streamfusion-batch-exec-rank_1"),
                config,
                Collections.singletonList(inputProperty),
                outputType,
                description);
        this.partitionFields = partitionFields.clone();
        this.sortFields = sortFields.clone();
        this.rankStart = rankStart;
        this.rankEnd = rankEnd;
        this.outputRankNumber = outputRankNumber;
        this.inputSortSpec = inputSortSpec;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner, ExecNodeConfig config) {
        ExecEdge edge = getInputEdges().get(0);
        Transformation<RowData> input = (Transformation<RowData>) edge.translateToPlan(planner);
        RowType inputType = (RowType) edge.getOutputType();
        if (inputSortSpec != null) {
            try {
                Class<?> translator = Class.forName(
                        TRANSLATOR, true, planner.getFlinkContext().getClassLoader());
                Method method = translator.getMethod(
                        "translateSelection",
                        Transformation.class,
                        RowType.class,
                        RowType.class,
                        int[].class,
                        int[].class,
                        SortSpec.class,
                        long.class,
                        long.class,
                        boolean.class,
                        ReadableConfig.class,
                        org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.class);
                return (Transformation<RowData>) method.invoke(
                        null,
                        input,
                        inputType,
                        (RowType) getOutputType(),
                        partitionFields,
                        sortFields,
                        inputSortSpec,
                        rankStart,
                        rankEnd,
                        outputRankNumber,
                        getPersistedConfig(),
                        planner.getExecEnv());
            } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException failure) {
                throw new IllegalStateException("Could not invoke the StreamFusion bounded RANK selection", failure);
            } catch (InvocationTargetException failure) {
                throw new IllegalStateException(
                        "StreamFusion bounded RANK selection translation failed", failure.getCause());
            }
        }
        GeneratedRecordComparator partitionComparator = ComparatorCodeGenerator.gen(
                config,
                planner.getFlinkContext().getClassLoader(),
                "StreamFusionBoundedRankPartitionComparator",
                inputType,
                SortUtil.getAscendingSortSpec(partitionFields));
        GeneratedRecordComparator orderComparator = ComparatorCodeGenerator.gen(
                config,
                planner.getFlinkContext().getClassLoader(),
                "StreamFusionBoundedRankOrderComparator",
                inputType,
                SortUtil.getAscendingSortSpec(sortFields));
        try {
            Class<?> translator =
                    Class.forName(TRANSLATOR, true, planner.getFlinkContext().getClassLoader());
            Method method = translator.getMethod(
                    "translate",
                    Transformation.class,
                    RowType.class,
                    RowType.class,
                    int[].class,
                    int[].class,
                    long.class,
                    long.class,
                    boolean.class,
                    GeneratedRecordComparator.class,
                    GeneratedRecordComparator.class);
            Transformation<RowData> result = (Transformation<RowData>) method.invoke(
                    null,
                    input,
                    inputType,
                    (RowType) getOutputType(),
                    partitionFields,
                    sortFields,
                    rankStart,
                    rankEnd,
                    outputRankNumber,
                    partitionComparator,
                    orderComparator);
            if (result == null) {
                throw new IllegalStateException("A selected StreamFusion bounded RANK failed translation");
            }
            return result;
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException failure) {
            throw new IllegalStateException("Could not invoke the StreamFusion bounded RANK runtime", failure);
        } catch (InvocationTargetException failure) {
            throw new IllegalStateException("StreamFusion bounded RANK translation failed", failure.getCause());
        }
    }
}
