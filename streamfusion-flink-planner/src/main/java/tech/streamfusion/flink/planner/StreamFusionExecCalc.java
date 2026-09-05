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
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecCalc;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.runtime.operators.TableStreamOperator;
import org.apache.flink.table.types.logical.RowType;

/** StreamFusion's parallel physical calc node; the original Flink calc remains the fallback. */
public final class StreamFusionExecCalc extends CommonExecCalc implements StreamExecNode<RowData> {
    private static final String TRANSLATOR_CLASS = "tech.streamfusion.flink.calc.StreamFusionCalcTranslator";
    private final List<RexNode> streamFusionProjection;
    private final @Nullable RexNode streamFusionCondition;

    public StreamFusionExecCalc(
            ReadableConfig persistedConfig,
            List<RexNode> projection,
            @Nullable RexNode condition,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        super(
                ExecNodeContext.newNodeId(),
                new ExecNodeContext("streamfusion-exec-calc_1"),
                persistedConfig,
                projection,
                condition,
                TableStreamOperator.class,
                true,
                Collections.singletonList(inputProperty),
                outputType,
                description);
        this.streamFusionProjection = projection;
        this.streamFusionCondition = condition;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner, ExecNodeConfig config) {
        List<StreamFusionExecCalc> chain = adjacentChain(this);
        ExecEdge inputEdge = chain.get(0).getInputEdges().get(0);
        if (inputEdge.getSource() instanceof StreamFusionExecRegularJoin) {
            return ((StreamFusionExecRegularJoin) inputEdge.getSource()).translateWithOutputCalcs(planner, chain);
        }
        List<StreamFusionExecArrayUnnest> fusedUnnests = inputEdge.getSource() instanceof StreamFusionExecArrayUnnest
                ? StreamFusionExecArrayUnnest.adjacentChain((StreamFusionExecArrayUnnest) inputEdge.getSource())
                : Collections.emptyList();
        StreamFusionExecReplicateRows fusedReplicate = inputEdge.getSource() instanceof StreamFusionExecReplicateRows
                ? (StreamFusionExecReplicateRows) inputEdge.getSource()
                : null;
        ExecEdge unnestBoundaryEdge =
                !fusedUnnests.isEmpty() ? fusedUnnests.get(0).getInputEdges().get(0) : inputEdge;
        List<StreamFusionExecCalc> boundaryCalcs =
                !fusedUnnests.isEmpty() && unnestBoundaryEdge.getSource() instanceof StreamFusionExecCalc
                        ? adjacentChain((StreamFusionExecCalc) unnestBoundaryEdge.getSource())
                        : Collections.emptyList();
        ExecEdge boundaryEdge = !boundaryCalcs.isEmpty()
                ? boundaryCalcs.get(0).getInputEdges().get(0)
                : fusedReplicate != null ? fusedReplicate.getInputEdges().get(0) : unnestBoundaryEdge;
        Transformation<RowData> input = (Transformation<RowData>) boundaryEdge.translateToPlan(planner);
        List<RowType> inputTypes = new ArrayList<>(chain.size());
        List<RowType> outputTypes = new ArrayList<>(chain.size());
        List<List<RexNode>> projections = new ArrayList<>(chain.size());
        List<RexNode> conditions = new ArrayList<>(chain.size());
        for (StreamFusionExecCalc calc : chain) {
            inputTypes.add((RowType) calc.getInputEdges().get(0).getOutputType());
            outputTypes.add((RowType) calc.getOutputType());
            projections.add(calc.streamFusionProjection);
            conditions.add(calc.streamFusionCondition);
        }
        try {
            Class<?> translator = Class.forName(
                    TRANSLATOR_CLASS, true, planner.getFlinkContext().getClassLoader());
            Method translate;
            Transformation<RowData> result;
            if (fusedReplicate != null) {
                translate = translator.getMethod(
                        "translateReplicateRowsChain",
                        Transformation.class,
                        RowType.class,
                        RowType.class,
                        Object.class,
                        Object.class,
                        List.class,
                        List.class,
                        List.class,
                        List.class);
                result = (Transformation<RowData>) translate.invoke(
                        null,
                        input,
                        (RowType) fusedReplicate.getInputEdges().get(0).getOutputType(),
                        (RowType) fusedReplicate.getOutputType(),
                        fusedReplicate.streamFusionJoinType(),
                        fusedReplicate.streamFusionInvocation(),
                        inputTypes,
                        outputTypes,
                        projections,
                        conditions);
            } else if (fusedUnnests.isEmpty()) {
                translate = translator.getMethod(
                        "translateChain", Transformation.class, List.class, List.class, List.class, List.class);
                result = (Transformation<RowData>)
                        translate.invoke(null, input, inputTypes, outputTypes, projections, conditions);
            } else {
                List<RowType> boundaryCalcInputTypes = new ArrayList<>(boundaryCalcs.size());
                List<RowType> boundaryCalcOutputTypes = new ArrayList<>(boundaryCalcs.size());
                List<List<RexNode>> boundaryCalcProjections = new ArrayList<>(boundaryCalcs.size());
                List<RexNode> boundaryCalcConditions = new ArrayList<>(boundaryCalcs.size());
                for (StreamFusionExecCalc calc : boundaryCalcs) {
                    boundaryCalcInputTypes.add(
                            (RowType) calc.getInputEdges().get(0).getOutputType());
                    boundaryCalcOutputTypes.add((RowType) calc.getOutputType());
                    boundaryCalcProjections.add(calc.streamFusionProjection);
                    boundaryCalcConditions.add(calc.streamFusionCondition);
                }
                List<RowType> unnestInputTypes = new ArrayList<>(fusedUnnests.size());
                List<RowType> unnestOutputTypes = new ArrayList<>(fusedUnnests.size());
                List<Object> joinTypes = new ArrayList<>(fusedUnnests.size());
                List<Object> invocations = new ArrayList<>(fusedUnnests.size());
                for (StreamFusionExecArrayUnnest unnest : fusedUnnests) {
                    unnestInputTypes.add((RowType) unnest.getInputEdges().get(0).getOutputType());
                    unnestOutputTypes.add((RowType) unnest.getOutputType());
                    joinTypes.add(unnest.streamFusionJoinType());
                    invocations.add(unnest.streamFusionInvocation());
                }
                translate = translator.getMethod(
                        "translateCalcArrayUnnestCalcChains",
                        Transformation.class,
                        List.class,
                        List.class,
                        List.class,
                        List.class,
                        List.class,
                        List.class,
                        List.class,
                        List.class,
                        List.class,
                        List.class,
                        List.class,
                        List.class);
                result = (Transformation<RowData>) translate.invoke(
                        null,
                        input,
                        boundaryCalcInputTypes,
                        boundaryCalcOutputTypes,
                        boundaryCalcProjections,
                        boundaryCalcConditions,
                        unnestInputTypes,
                        unnestOutputTypes,
                        joinTypes,
                        invocations,
                        inputTypes,
                        outputTypes,
                        projections,
                        conditions);
            }
            if (result == null) {
                throw new IllegalStateException("A selected StreamFusion calc failed translation");
            }
            return result;
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalStateException("Could not invoke the StreamFusion calc runtime", e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException("StreamFusion calc translation failed", e.getCause());
        }
    }

    static List<StreamFusionExecCalc> adjacentChain(StreamFusionExecCalc root) {
        List<StreamFusionExecCalc> chain = new ArrayList<>();
        StreamFusionExecCalc current = root;
        while (true) {
            chain.add(0, current);
            ExecEdge inputEdge = current.getInputEdges().get(0);
            if (!(inputEdge.getSource() instanceof StreamFusionExecCalc)) {
                return chain;
            }
            current = (StreamFusionExecCalc) inputEdge.getSource();
        }
    }

    List<RexNode> streamFusionProjection() {
        return streamFusionProjection;
    }

    @Nullable RexNode streamFusionCondition() {
        return streamFusionCondition;
    }
}
