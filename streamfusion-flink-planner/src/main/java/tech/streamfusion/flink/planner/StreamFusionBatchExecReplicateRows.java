/*
 * Copyright 2026 StreamFusion Authors
 *
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import org.apache.calcite.rex.RexCall;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecCorrelate;
import org.apache.flink.table.runtime.operators.TableStreamOperator;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.types.logical.RowType;

/** Bounded accelerator exec node for Flink's optimizer-only set-operation row replicator. */
public final class StreamFusionBatchExecReplicateRows extends CommonExecCorrelate implements BatchExecNode<RowData> {
    private static final String TRANSLATOR_CLASS =
            "tech.streamfusion.flink.replicate.StreamFusionReplicateRowsTranslator";

    private final RexCall streamFusionInvocation;
    private final FlinkJoinType streamFusionJoinType;

    public StreamFusionBatchExecReplicateRows(
            ReadableConfig persistedConfig,
            FlinkJoinType joinType,
            RexCall invocation,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        super(
                ExecNodeContext.newNodeId(),
                new ExecNodeContext("streamfusion-batch-exec-replicate-rows_1"),
                persistedConfig,
                joinType,
                invocation,
                null,
                TableStreamOperator.class,
                false,
                Collections.singletonList(inputProperty),
                outputType,
                description);
        this.streamFusionInvocation = invocation;
        this.streamFusionJoinType = joinType;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner, ExecNodeConfig config) {
        Transformation<RowData> input =
                (Transformation<RowData>) getInputEdges().get(0).translateToPlan(planner);
        try {
            Class<?> translator = Class.forName(
                    TRANSLATOR_CLASS, true, planner.getFlinkContext().getClassLoader());
            Method translate = translator.getMethod(
                    "translate",
                    Transformation.class,
                    RowType.class,
                    RowType.class,
                    Object.class,
                    Object.class,
                    Object.class);
            Transformation<RowData> result = (Transformation<RowData>) translate.invoke(
                    null,
                    input,
                    (RowType) getInputEdges().get(0).getOutputType(),
                    (RowType) getOutputType(),
                    streamFusionJoinType,
                    streamFusionInvocation,
                    null);
            if (result == null) {
                throw new IllegalStateException("A selected bounded StreamFusion REPLICATE_ROWS failed translation");
            }
            return result;
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException failure) {
            throw new IllegalStateException(
                    "Could not invoke the bounded StreamFusion REPLICATE_ROWS runtime", failure);
        } catch (InvocationTargetException failure) {
            throw new IllegalStateException(
                    "Bounded StreamFusion REPLICATE_ROWS translation failed", failure.getCause());
        }
    }
}
