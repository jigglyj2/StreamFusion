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

import java.util.Collections;
import java.util.List;
import org.apache.calcite.rex.RexNode;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecExpand;
import org.apache.flink.table.types.logical.RowType;

/** StreamFusion bounded physical Expand node; Flink's original node remains available for fallback. */
public final class StreamFusionBatchExecExpand extends CommonExecExpand
        implements BatchExecNode<RowData>, StreamFusionNativePlanNode {
    private final StreamFusionNativeNodeMetadata nativeMetadata = new StreamFusionNativeNodeMetadata();

    @Override
    public StreamFusionNativeNodeMetadata nativeMetadata() {
        return nativeMetadata;
    }

    private final List<List<RexNode>> streamFusionProjects;

    public StreamFusionBatchExecExpand(
            ReadableConfig persistedConfig,
            List<List<RexNode>> projects,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        super(
                ExecNodeContext.newNodeId(),
                new ExecNodeContext("streamfusion-batch-exec-expand_1"),
                persistedConfig,
                projects,
                false,
                Collections.singletonList(inputProperty),
                outputType,
                description);
        this.streamFusionProjects = projects;
    }

    List<List<RexNode>> streamFusionProjects() {
        return streamFusionProjects;
    }

    @Override
    public byte[] nativePlanFragment(PlannerBase planner) {
        return StreamFusionNativePlanNode.invokeBuilder(
                planner,
                "tech.streamfusion.flink.expand.StreamFusionExpandTranslator",
                new Class<?>[] {RowType.class, RowType.class, List.class},
                (RowType) getInputEdges().get(0).getOutputType(),
                (RowType) getOutputType(),
                streamFusionProjects);
    }

    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner, ExecNodeConfig config) {
        return StreamFusionStatelessRegion.translate(this, planner);
    }
}
