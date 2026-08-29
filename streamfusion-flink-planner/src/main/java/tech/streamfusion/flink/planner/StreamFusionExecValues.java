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
import java.util.List;
import org.apache.calcite.rex.RexLiteral;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecValues;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.types.logical.RowType;

/** StreamFusion physical VALUES node; Flink's original node remains available for fallback. */
public final class StreamFusionExecValues extends CommonExecValues implements StreamExecNode<RowData> {
    private static final String TRANSLATOR_CLASS = "tech.streamfusion.flink.values.StreamFusionValuesTranslator";
    private final List<List<RexLiteral>> streamFusionTuples;

    public StreamFusionExecValues(
            ReadableConfig persistedConfig, List<List<RexLiteral>> tuples, RowType outputType, String description) {
        super(
                ExecNodeContext.newNodeId(),
                new ExecNodeContext("streamfusion-exec-values_1"),
                persistedConfig,
                tuples,
                outputType,
                description);
        this.streamFusionTuples = tuples;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner, ExecNodeConfig config) {
        try {
            Class<?> translator = Class.forName(
                    TRANSLATOR_CLASS, true, planner.getFlinkContext().getClassLoader());
            Method translate =
                    translator.getMethod("translate", StreamExecutionEnvironment.class, RowType.class, List.class);
            Transformation<RowData> result = (Transformation<RowData>)
                    translate.invoke(null, planner.getExecEnv(), (RowType) getOutputType(), streamFusionTuples);
            if (result == null) {
                throw new IllegalStateException("A selected StreamFusion VALUES failed translation");
            }
            return result;
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalStateException("Could not invoke the StreamFusion VALUES runtime", e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException("StreamFusion VALUES translation failed", e.getCause());
        }
    }
}
