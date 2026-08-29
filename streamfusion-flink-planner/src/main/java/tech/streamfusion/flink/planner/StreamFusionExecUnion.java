/*
 * Copyright 2026 StreamFusion Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package tech.streamfusion.flink.planner;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecUnion;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.types.logical.RowType;

/** StreamFusion physical coverage for Flink's zero-work streaming UNION ALL wiring. */
public final class StreamFusionExecUnion extends CommonExecUnion implements StreamExecNode<RowData> {
    private static final String TRANSLATOR_CLASS = "tech.streamfusion.flink.union.StreamFusionUnionTranslator";

    public StreamFusionExecUnion(
            ReadableConfig persistedConfig,
            List<InputProperty> inputProperties,
            RowType outputType,
            String description) {
        super(
                ExecNodeContext.newNodeId(),
                new ExecNodeContext("streamfusion-exec-union_1"),
                persistedConfig,
                inputProperties,
                outputType,
                description);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner, ExecNodeConfig config) {
        List<Transformation<RowData>> inputs = new ArrayList<>(getInputEdges().size());
        for (ExecEdge edge : getInputEdges()) {
            inputs.add((Transformation<RowData>) edge.translateToPlan(planner));
        }
        try {
            Class<?> translator = Class.forName(
                    TRANSLATOR_CLASS, true, planner.getFlinkContext().getClassLoader());
            Method translate = translator.getMethod("translate", List.class, RowType.class);
            Transformation<RowData> result =
                    (Transformation<RowData>) translate.invoke(null, inputs, (RowType) getOutputType());
            if (result == null) {
                throw new IllegalStateException("A selected StreamFusion UNION ALL failed translation");
            }
            return result;
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalStateException("Could not invoke the StreamFusion UNION ALL runtime", e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException("StreamFusion UNION ALL translation failed", e.getCause());
        }
    }
}
