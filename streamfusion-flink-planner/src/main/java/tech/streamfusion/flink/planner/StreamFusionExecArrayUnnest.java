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
import org.apache.calcite.rex.RexCall;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecCorrelate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.runtime.operators.TableStreamOperator;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.types.logical.RowType;

/** StreamFusion physical node for parity-safe inner array UNNEST. */
public final class StreamFusionExecArrayUnnest extends CommonExecCorrelate implements StreamExecNode<RowData> {
    private static final String TRANSLATOR_CLASS = "tech.streamfusion.flink.unnest.StreamFusionArrayUnnestTranslator";

    private final RexCall streamFusionInvocation;
    private final FlinkJoinType streamFusionJoinType;

    public StreamFusionExecArrayUnnest(
            ReadableConfig persistedConfig,
            FlinkJoinType joinType,
            RexCall invocation,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        super(
                ExecNodeContext.newNodeId(),
                new ExecNodeContext("streamfusion-exec-array-unnest_1"),
                persistedConfig,
                joinType,
                invocation,
                null,
                TableStreamOperator.class,
                true,
                Collections.singletonList(inputProperty),
                outputType,
                description);
        this.streamFusionInvocation = invocation;
        this.streamFusionJoinType = joinType;
    }

    RexCall streamFusionInvocation() {
        return streamFusionInvocation;
    }

    FlinkJoinType streamFusionJoinType() {
        return streamFusionJoinType;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner, ExecNodeConfig config) {
        List<StreamFusionExecArrayUnnest> chain = adjacentChain(this);
        ExecEdge boundaryEdge = chain.get(0).getInputEdges().get(0);
        Transformation<RowData> input = (Transformation<RowData>) boundaryEdge.translateToPlan(planner);
        List<RowType> inputTypes = new ArrayList<>(chain.size());
        List<RowType> outputTypes = new ArrayList<>(chain.size());
        List<Object> joinTypes = new ArrayList<>(chain.size());
        List<Object> invocations = new ArrayList<>(chain.size());
        for (StreamFusionExecArrayUnnest unnest : chain) {
            inputTypes.add((RowType) unnest.getInputEdges().get(0).getOutputType());
            outputTypes.add((RowType) unnest.getOutputType());
            joinTypes.add(unnest.streamFusionJoinType);
            invocations.add(unnest.streamFusionInvocation);
        }
        try {
            Class<?> translator = Class.forName(
                    TRANSLATOR_CLASS, true, planner.getFlinkContext().getClassLoader());
            Method translate = translator.getMethod(
                    "translateChain", Transformation.class, List.class, List.class, List.class, List.class);
            Transformation<RowData> result = (Transformation<RowData>)
                    translate.invoke(null, input, inputTypes, outputTypes, joinTypes, invocations);
            if (result == null) {
                throw new IllegalStateException("A selected StreamFusion array UNNEST failed translation");
            }
            return result;
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalStateException("Could not invoke the StreamFusion array UNNEST runtime", e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException("StreamFusion array UNNEST translation failed", e.getCause());
        }
    }

    static List<StreamFusionExecArrayUnnest> adjacentChain(StreamFusionExecArrayUnnest root) {
        List<StreamFusionExecArrayUnnest> chain = new ArrayList<>();
        StreamFusionExecArrayUnnest current = root;
        while (true) {
            chain.add(0, current);
            ExecEdge inputEdge = current.getInputEdges().get(0);
            if (!(inputEdge.getSource() instanceof StreamFusionExecArrayUnnest)) {
                return chain;
            }
            current = (StreamFusionExecArrayUnnest) inputEdge.getSource();
        }
    }
}
