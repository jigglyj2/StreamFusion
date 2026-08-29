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
import java.util.Collections;
import java.util.List;
import org.apache.calcite.rex.RexNode;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecExpand;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.types.logical.RowType;

/** StreamFusion physical Expand node; Flink's original node remains available for fallback. */
public final class StreamFusionExecExpand extends CommonExecExpand implements StreamExecNode<RowData> {
    private static final String TRANSLATOR_CLASS = "tech.streamfusion.flink.expand.StreamFusionExpandTranslator";
    private final List<List<RexNode>> streamFusionProjects;

    public StreamFusionExecExpand(
            ReadableConfig persistedConfig,
            List<List<RexNode>> projects,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        super(
                ExecNodeContext.newNodeId(),
                new ExecNodeContext("streamfusion-exec-expand_1"),
                persistedConfig,
                projects,
                true,
                Collections.singletonList(inputProperty),
                outputType,
                description);
        this.streamFusionProjects = projects;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner, ExecNodeConfig config) {
        ExecEdge inputEdge = getInputEdges().get(0);
        Transformation<RowData> input = (Transformation<RowData>) inputEdge.translateToPlan(planner);
        try {
            Class<?> translator = Class.forName(
                    TRANSLATOR_CLASS, true, planner.getFlinkContext().getClassLoader());
            Method translate =
                    translator.getMethod("translate", Transformation.class, RowType.class, RowType.class, List.class);
            Transformation<RowData> result = (Transformation<RowData>) translate.invoke(
                    null, input, (RowType) inputEdge.getOutputType(), (RowType) getOutputType(), streamFusionProjects);
            if (result == null) {
                throw new IllegalStateException("A selected StreamFusion Expand failed translation");
            }
            return result;
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalStateException("Could not invoke the StreamFusion Expand runtime", e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException("StreamFusion Expand translation failed", e.getCause());
        }
    }
}
