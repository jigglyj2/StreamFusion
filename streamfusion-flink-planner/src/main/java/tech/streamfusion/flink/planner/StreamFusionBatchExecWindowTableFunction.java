/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.rex.RexNode;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.logical.TimeAttributeWindowingStrategy;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecWindowTableFunction;
import org.apache.flink.table.types.logical.RowType;

/** StreamFusion bounded physical coverage for aligned event-time Window TVFs. */
public final class StreamFusionBatchExecWindowTableFunction extends CommonExecWindowTableFunction
        implements BatchExecNode<RowData> {
    private static final String TRANSLATOR_CLASS =
            "tech.streamfusion.flink.window.StreamFusionWindowTableFunctionTranslator";
    private final TimeAttributeWindowingStrategy streamFusionStrategy;

    public StreamFusionBatchExecWindowTableFunction(
            ReadableConfig persistedConfig,
            TimeAttributeWindowingStrategy strategy,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        super(
                ExecNodeContext.newNodeId(),
                new ExecNodeContext("streamfusion-batch-exec-window-table-function_1"),
                persistedConfig,
                strategy,
                Collections.singletonList(inputProperty),
                outputType,
                description);
        this.streamFusionStrategy = strategy;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner, ExecNodeConfig config) {
        ExecEdge inputEdge = getInputEdges().get(0);
        RowType inputType = (RowType) inputEdge.getOutputType();
        List<StreamFusionBatchExecCalc> inputCalcs = inputEdge.getSource() instanceof StreamFusionBatchExecCalc
                ? StreamFusionBatchExecCalc.adjacentChain((StreamFusionBatchExecCalc) inputEdge.getSource())
                : Collections.emptyList();
        ExecEdge boundaryEdge = inputCalcs.isEmpty()
                ? inputEdge
                : inputCalcs.get(0).getInputEdges().get(0);
        Transformation<RowData> input = (Transformation<RowData>) boundaryEdge.translateToPlan(planner);
        try {
            Class<?> translator = Class.forName(
                    TRANSLATOR_CLASS, true, planner.getFlinkContext().getClassLoader());
            Method method;
            Transformation<RowData> result;
            if (inputCalcs.isEmpty()) {
                method = translator.getMethod(
                        "translate",
                        Transformation.class,
                        RowType.class,
                        RowType.class,
                        TimeAttributeWindowingStrategy.class,
                        ReadableConfig.class,
                        org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.class,
                        org.apache.flink.table.runtime.keyselector.RowDataKeySelector.class);
                result = (Transformation<RowData>) method.invoke(
                        null,
                        input,
                        inputType,
                        (RowType) getOutputType(),
                        streamFusionStrategy,
                        getPersistedConfig(),
                        planner.getExecEnv(),
                        null);
            } else {
                List<RowType> calcInputTypes = new ArrayList<>(inputCalcs.size());
                List<RowType> calcOutputTypes = new ArrayList<>(inputCalcs.size());
                List<List<RexNode>> calcProjections = new ArrayList<>(inputCalcs.size());
                List<RexNode> calcConditions = new ArrayList<>(inputCalcs.size());
                for (StreamFusionBatchExecCalc calc : inputCalcs) {
                    calcInputTypes.add((RowType) calc.getInputEdges().get(0).getOutputType());
                    calcOutputTypes.add((RowType) calc.getOutputType());
                    calcProjections.add(calc.streamFusionProjection());
                    calcConditions.add(calc.streamFusionCondition());
                }
                method = translator.getMethod(
                        "translateInputCalcChain",
                        Transformation.class,
                        List.class,
                        List.class,
                        List.class,
                        List.class,
                        RowType.class,
                        TimeAttributeWindowingStrategy.class,
                        ReadableConfig.class);
                result = (Transformation<RowData>) method.invoke(
                        null,
                        input,
                        calcInputTypes,
                        calcOutputTypes,
                        calcProjections,
                        calcConditions,
                        (RowType) getOutputType(),
                        streamFusionStrategy,
                        getPersistedConfig());
            }
            if (result == null) {
                throw new IllegalStateException("A selected StreamFusion bounded Window TVF failed translation");
            }
            return result;
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException failure) {
            throw new IllegalStateException("Could not invoke the StreamFusion bounded Window TVF runtime", failure);
        } catch (InvocationTargetException failure) {
            throw new IllegalStateException("StreamFusion bounded Window TVF translation failed", failure.getCause());
        }
    }

    @Override
    protected Transformation<RowData> translateWithUnalignedWindow(
            PlannerBase planner, ExecNodeConfig config, RowType inputType, Transformation<RowData> input) {
        throw new UnsupportedOperationException("StreamFusion does not select unaligned bounded Window TVFs");
    }
}
