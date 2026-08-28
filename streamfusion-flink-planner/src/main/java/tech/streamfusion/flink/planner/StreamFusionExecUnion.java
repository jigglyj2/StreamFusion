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

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.transformations.UnionTransformation;
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
        return new UnionTransformation<>(inputs);
    }
}
