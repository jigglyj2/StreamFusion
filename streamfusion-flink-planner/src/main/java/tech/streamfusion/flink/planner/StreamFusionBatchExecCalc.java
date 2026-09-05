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
public final class StreamFusionBatchExecCalc extends CommonExecCalc implements BatchExecNode<RowData> {
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

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner, ExecNodeConfig config) {
        List<StreamFusionBatchExecCalc> chain = adjacentChain(this);
        ExecEdge boundaryEdge = chain.get(0).getInputEdges().get(0);
        Transformation<RowData> input = (Transformation<RowData>) boundaryEdge.translateToPlan(planner);
        List<RowType> inputTypes = new ArrayList<>(chain.size());
        List<RowType> outputTypes = new ArrayList<>(chain.size());
        List<List<RexNode>> projections = new ArrayList<>(chain.size());
        List<RexNode> conditions = new ArrayList<>(chain.size());
        for (StreamFusionBatchExecCalc calc : chain) {
            inputTypes.add((RowType) calc.getInputEdges().get(0).getOutputType());
            outputTypes.add((RowType) calc.getOutputType());
            projections.add(calc.streamFusionProjection);
            conditions.add(calc.streamFusionCondition);
        }
        try {
            Class<?> translator = Class.forName(
                    TRANSLATOR_CLASS, true, planner.getFlinkContext().getClassLoader());
            Method translate = translator.getMethod(
                    "translateChain", Transformation.class, List.class, List.class, List.class, List.class);
            Transformation<RowData> result = (Transformation<RowData>)
                    translate.invoke(null, input, inputTypes, outputTypes, projections, conditions);
            if (result == null) {
                throw new IllegalStateException("A selected StreamFusion bounded Calc failed translation");
            }
            return result;
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException failure) {
            throw new IllegalStateException("Could not invoke the StreamFusion bounded Calc runtime", failure);
        } catch (InvocationTargetException failure) {
            throw new IllegalStateException("StreamFusion bounded Calc translation failed", failure.getCause());
        }
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
}
