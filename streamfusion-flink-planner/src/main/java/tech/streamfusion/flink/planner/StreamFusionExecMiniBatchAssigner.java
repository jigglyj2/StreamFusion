/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.planner.plan.trait.MiniBatchInterval;
import org.apache.flink.table.planner.plan.trait.MiniBatchMode;
import org.apache.flink.table.types.logical.RowType;

/** StreamFusion physical node retaining Flink's mini-batch watermark control stage. */
public final class StreamFusionExecMiniBatchAssigner extends ExecNodeBase<RowData> implements StreamExecNode<RowData> {
    private static final String TRANSLATOR_CLASS =
            "tech.streamfusion.flink.minibatch.StreamFusionMiniBatchAssignerTranslator";
    private final MiniBatchInterval interval;

    public StreamFusionExecMiniBatchAssigner(
            ReadableConfig config,
            MiniBatchInterval interval,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        super(
                ExecNodeContext.newNodeId(),
                new ExecNodeContext("streamfusion-exec-mini-batch-assigner_1"),
                config,
                List.of(inputProperty),
                outputType,
                description);
        this.interval = interval;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner, ExecNodeConfig config) {
        Transformation<RowData> input =
                (Transformation<RowData>) getInputEdges().get(0).translateToPlan(planner);
        try {
            Class<?> translator = Class.forName(
                    TRANSLATOR_CLASS, true, planner.getFlinkContext().getClassLoader());
            Method method = translator.getMethod(
                    "translate", Transformation.class, RowType.class, long.class, MiniBatchMode.class);
            return (Transformation<RowData>)
                    method.invoke(null, input, (RowType) getOutputType(), interval.getInterval(), interval.getMode());
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalStateException("Could not invoke the StreamFusion mini-batch runtime", e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException("StreamFusion mini-batch translation failed", e.getCause());
        }
    }
}
