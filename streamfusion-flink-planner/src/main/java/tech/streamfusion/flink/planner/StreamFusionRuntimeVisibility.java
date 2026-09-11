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

import static tech.streamfusion.flink.planner.FlinkExecNodeAccess.*;
import static tech.streamfusion.flink.planner.StreamFusionAggregateShapes.*;
import static tech.streamfusion.flink.planner.StreamFusionGroupAggregateSupport.*;
import static tech.streamfusion.flink.planner.StreamFusionJoinSupport.*;
import static tech.streamfusion.flink.planner.StreamFusionOverSupport.*;
import static tech.streamfusion.flink.planner.StreamFusionProcessingTimeShapes.*;
import static tech.streamfusion.flink.planner.StreamFusionRankShapes.*;
import static tech.streamfusion.flink.planner.StreamFusionRankSupport.*;
import static tech.streamfusion.flink.planner.StreamFusionRuntimeClasses.*;
import static tech.streamfusion.flink.planner.StreamFusionStatelessSupport.*;
import static tech.streamfusion.flink.planner.StreamFusionWindowAggregateSupport.*;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import org.apache.flink.table.types.logical.RowType;

/** Planner/runtime class visibility and native protocol negotiation before graph replacement. */
final class StreamFusionRuntimeVisibility {
    private StreamFusionRuntimeVisibility() {}

    static String rejection(ClassLoader classLoader) {
        try {
            Class.forName(NATIVE_PLAN_CLASS, true, classLoader);
            Class.forName(NATIVE_OPERATOR_CLASS, true, classLoader);
            Class<?> region = Class.forName(
                    "tech.streamfusion.flink.operator.StreamFusionNativeRegionTranslator", true, classLoader);
            region.getMethod("identifyStage", byte[].class, int.class, String.class, String.class);
            region.getMethod("inputPlan", int.class);
            region.getMethod("composeWithInputs", byte[].class, List.class);
            region.getMethod("translateInputs", List.class, List.class, RowType.class, byte[].class);
            region.getMethod(
                    "translateInputsWithResources",
                    List.class,
                    List.class,
                    RowType.class,
                    byte[].class,
                    java.util.function.Function.class);
            region.getMethod(
                    "translateKeyedInputsWithResources",
                    List.class,
                    List.class,
                    RowType.class,
                    byte[].class,
                    List.class,
                    org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.class,
                    java.util.function.Function.class);
            Class.forName("tech.streamfusion.flink.metrics.StreamFusionNativeMetricTree", true, classLoader)
                    .getMethod(
                            "forRegion",
                            byte[].class,
                            org.apache.flink.runtime.jobgraph.OperatorID.class,
                            org.apache.flink.runtime.metrics.groups.TaskMetricGroup.class,
                            org.apache.flink.configuration.Configuration.class,
                            int.class);
            region.getMethod(
                    "translateKeyedInputs",
                    List.class,
                    List.class,
                    RowType.class,
                    byte[].class,
                    List.class,
                    org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.class);
            Class.forName(
                    "tech.streamfusion.flink.operator.StreamFusionNativeRegionOperatorFactory", true, classLoader);
            Class.forName(UNION_TRANSLATOR_CLASS, true, classLoader)
                    .getMethod("createStagePlan", RowType.class, int.class);
            Class.forName(GROUP_AGGREGATE_TRANSLATOR_CLASS, true, StreamFusionRuntimeClasses.class.getClassLoader())
                    .getMethod(
                            "createStagePlan",
                            RowType.class,
                            RowType.class,
                            int[].class,
                            org.apache.calcite.rel.core.AggregateCall[].class,
                            boolean[].class,
                            boolean.class,
                            boolean.class,
                            long.class,
                            org.apache.flink.configuration.ReadableConfig.class);
            Class.forName(
                            "tech.streamfusion.flink.planner.window.StreamFusionGlobalWindowAggregateTranslator",
                            true,
                            StreamFusionRuntimeClasses.class.getClassLoader())
                    .getMethod(
                            "createStagePlan",
                            RowType.class,
                            RowType.class,
                            RowType.class,
                            int.class,
                            org.apache.calcite.rel.core.AggregateCall[].class,
                            org.apache.flink.table.planner.plan.logical.WindowingStrategy.class,
                            org.apache.flink.table.runtime.groupwindow.NamedWindowProperty[].class,
                            boolean.class,
                            org.apache.flink.configuration.ReadableConfig.class);
            Class.forName(
                            "tech.streamfusion.flink.planner.window.StreamFusionSingleStageWindowAggregateTranslator",
                            true,
                            StreamFusionRuntimeClasses.class.getClassLoader())
                    .getMethod(
                            "createStagePlan",
                            RowType.class,
                            RowType.class,
                            int[].class,
                            org.apache.calcite.rel.core.AggregateCall[].class,
                            org.apache.flink.table.planner.plan.logical.WindowingStrategy.class,
                            org.apache.flink.table.runtime.groupwindow.NamedWindowProperty[].class,
                            boolean.class,
                            org.apache.flink.configuration.ReadableConfig.class);
            Class.forName(NATIVE_PREFLIGHT_CLASS, true, classLoader)
                    .getMethod("verify")
                    .invoke(null);
            return null;
        } catch (ClassNotFoundException
                | NoSuchMethodException
                | IllegalAccessException
                | InvocationTargetException
                | RuntimeException
                | LinkageError failure) {
            return "StreamFusion runtime classes are not consistently visible from Flink's planner classloader: "
                    + failureDescription(failure);
        }
    }

    static String failureDescription(Throwable failure) {
        Throwable root = failure;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        String message = root.getMessage();
        return root.getClass().getName() + (message == null || message.isBlank() ? "" : ": " + message);
    }
}
