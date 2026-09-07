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
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.calcite.rex.RexNode;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecCalc;
import org.apache.flink.table.runtime.operators.TableStreamOperator;
import org.apache.flink.table.types.logical.RowType;

/** StreamFusion's bounded physical Calc node; the original Flink node remains the fallback. */
public final class StreamFusionBatchExecCalc extends CommonExecCalc
        implements BatchExecNode<RowData>, StreamFusionNativePlanNode {
    private final StreamFusionNativeNodeMetadata nativeMetadata = new StreamFusionNativeNodeMetadata();

    @Override
    public StreamFusionNativeNodeMetadata nativeMetadata() {
        return nativeMetadata;
    }

    private static final String TRANSLATOR_CLASS = "tech.streamfusion.flink.calc.StreamFusionCalcTranslator";

    private final List<RexNode> streamFusionProjection;
    private final @Nullable RexNode streamFusionCondition;

    public StreamFusionBatchExecCalc(
            ReadableConfig persistedConfig,
            List<RexNode> projection,
            @Nullable RexNode condition,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        super(
                ExecNodeContext.newNodeId(),
                new ExecNodeContext("streamfusion-batch-exec-calc_1"),
                persistedConfig,
                projection,
                condition,
                TableStreamOperator.class,
                false,
                Collections.singletonList(inputProperty),
                outputType,
                description);
        this.streamFusionProjection = projection;
        this.streamFusionCondition = condition;
    }

    @Override
    public byte[] nativePlanFragment(PlannerBase planner) {
        return StreamFusionNativePlanNode.invokeBuilder(
                planner,
                TRANSLATOR_CLASS,
                new Class<?>[] {RowType.class, RowType.class, List.class, Object.class},
                (RowType) getInputEdges().get(0).getOutputType(),
                (RowType) getOutputType(),
                streamFusionProjection,
                streamFusionCondition);
    }

    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner, ExecNodeConfig config) {
        // Compatibility lifecycle adapters remain until stateful region ownership is migrated.
        List<StreamFusionBatchExecCalc> chain = adjacentChain(this);
        ExecEdge inputEdge = chain.get(0).getInputEdges().get(0);
        if (inputEdge.getSource() instanceof StreamFusionBatchExecHashJoin) {
            return ((StreamFusionBatchExecHashJoin) inputEdge.getSource()).translateWithOutputCalcs(planner, chain);
        }
        if (inputEdge.getSource() instanceof StreamFusionBatchExecNestedLoopJoin) {
            return ((StreamFusionBatchExecNestedLoopJoin) inputEdge.getSource())
                    .translateWithOutputCalcs(planner, chain);
        }
        return StreamFusionStatelessRegion.translate(this, planner);
    }

    static List<StreamFusionBatchExecCalc> adjacentChain(StreamFusionBatchExecCalc root) {
        List<StreamFusionBatchExecCalc> chain = new ArrayList<>();
        StreamFusionBatchExecCalc current = root;
        while (true) {
            chain.add(0, current);
            ExecEdge inputEdge = current.getInputEdges().get(0);
            if (!(inputEdge.getSource() instanceof StreamFusionBatchExecCalc)) {
                return chain;
            }
            current = (StreamFusionBatchExecCalc) inputEdge.getSource();
        }
    }

    List<RexNode> streamFusionProjection() {
        return streamFusionProjection;
    }

    @Nullable RexNode streamFusionCondition() {
        return streamFusionCondition;
    }
}
