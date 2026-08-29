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
import org.apache.flink.table.planner.plan.logical.TimeAttributeWindowingStrategy;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecWindowTableFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.types.logical.RowType;

/** Distinct StreamFusion physical node for TUMBLE, HOP, and CUMULATE. */
public final class StreamFusionExecWindowTableFunction extends CommonExecWindowTableFunction
        implements StreamExecNode<RowData> {
    private static final String TRANSLATOR_CLASS =
            "tech.streamfusion.flink.window.StreamFusionWindowTableFunctionTranslator";
    private final TimeAttributeWindowingStrategy streamFusionStrategy;

    public StreamFusionExecWindowTableFunction(
            ReadableConfig persistedConfig,
            TimeAttributeWindowingStrategy strategy,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        super(
                ExecNodeContext.newNodeId(),
                new ExecNodeContext("streamfusion-exec-window-table-function_1"),
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
        Transformation<RowData> input = (Transformation<RowData>) inputEdge.translateToPlan(planner);
        try {
            Class<?> translator = Class.forName(
                    TRANSLATOR_CLASS, true, planner.getFlinkContext().getClassLoader());
            Method method = translator.getMethod(
                    "translate",
                    Transformation.class,
                    RowType.class,
                    RowType.class,
                    TimeAttributeWindowingStrategy.class);
            Transformation<RowData> result = (Transformation<RowData>) method.invoke(
                    null, input, (RowType) inputEdge.getOutputType(), (RowType) getOutputType(), streamFusionStrategy);
            if (result == null) {
                throw new IllegalStateException("A selected StreamFusion Window TVF failed translation");
            }
            return result;
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalStateException("Could not invoke the StreamFusion Window TVF runtime", e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException("StreamFusion Window TVF translation failed", e.getCause());
        }
    }

    @Override
    protected Transformation<RowData> translateWithUnalignedWindow(
            PlannerBase planner, ExecNodeConfig config, RowType inputRowType, Transformation<RowData> inputTransform) {
        throw new UnsupportedOperationException("StreamFusion does not select unaligned Window TVFs");
    }
}
