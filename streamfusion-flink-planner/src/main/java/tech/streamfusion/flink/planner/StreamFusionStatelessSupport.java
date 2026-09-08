/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
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
import static tech.streamfusion.flink.planner.StreamFusionWindowAggregateSupport.*;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.planner.plan.logical.TimeAttributeWindowingStrategy;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecCalc;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecCorrelate;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecExpand;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecUnion;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecValues;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecWindowTableFunction;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecCorrelate;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecExpand;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecWindowTableFunction;
import org.apache.flink.table.planner.plan.nodes.exec.processor.ProcessorContext;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecCalc;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecCorrelate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecExpand;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecUnion;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecValues;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecWindowTableFunction;
import org.apache.flink.table.types.logical.RowType;

/** StreamFusion StatelessSupport for native physical planning. */
final class StreamFusionStatelessSupport {
    static String unsupportedReason(StreamExecCalc calc, ProcessorContext context) {
        ExecEdge input = calc.getInputEdges().get(0);
        return unsupportedCalcReason(
                (RowType) input.getOutputType(),
                (RowType) calc.getOutputType(),
                projection(calc),
                condition(calc),
                context);
    }

    static String unsupportedReason(BatchExecCalc calc, ProcessorContext context) {
        ExecEdge input = calc.getInputEdges().get(0);
        return unsupportedCalcReason(
                (RowType) input.getOutputType(),
                (RowType) calc.getOutputType(),
                projection(calc),
                condition(calc),
                context);
    }

    static String unsupportedCalcReason(
            RowType inputType,
            RowType outputType,
            List<RexNode> projections,
            RexNode condition,
            ProcessorContext context) {
        try {
            Class<?> translator = Class.forName(
                    TRANSLATOR_CLASS,
                    true,
                    context.getPlanner().getFlinkContext().getClassLoader());
            Method method =
                    translator.getMethod("unsupportedReason", RowType.class, RowType.class, List.class, Object.class);
            return (String) method.invoke(null, inputType, outputType, projections, condition);
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalStateException("Could not inspect StreamFusion calc support", e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException("StreamFusion calc support inspection failed", e.getCause());
        }
    }

    static String unsupportedReason(StreamExecCorrelate correlate, ProcessorContext context) {
        return unsupportedCorrelateReason(correlate, context);
    }

    static String unsupportedReason(BatchExecCorrelate correlate, ProcessorContext context) {
        Object invocation = field(correlate, CommonExecCorrelate.class, "invocation");
        if (invocation instanceof RexCall
                && "$REPLICATE_ROWS$1"
                        .equals(((RexCall) invocation).getOperator().getName())) {
            return "bounded set-operation row replication has no StreamFusion physical implementation";
        }
        return unsupportedCorrelateReason(correlate, context);
    }

    static String unsupportedCorrelateReason(CommonExecCorrelate correlate, ProcessorContext context) {
        ExecEdge input = correlate.getInputEdges().get(0);
        Object joinType = field(correlate, CommonExecCorrelate.class, "joinType");
        Object invocation = field(correlate, CommonExecCorrelate.class, "invocation");
        Object condition = field(correlate, CommonExecCorrelate.class, "condition");
        String translatorClass = invocation instanceof RexCall
                        && "$REPLICATE_ROWS$1"
                                .equals(((RexCall) invocation).getOperator().getName())
                ? REPLICATE_ROWS_TRANSLATOR_CLASS
                : UNNEST_TRANSLATOR_CLASS;
        try {
            Class<?> translator = Class.forName(
                    translatorClass,
                    true,
                    context.getPlanner().getFlinkContext().getClassLoader());
            Method method = translator.getMethod(
                    "unsupportedReason", RowType.class, RowType.class, Object.class, Object.class, Object.class);
            return (String) method.invoke(
                    null,
                    (RowType) input.getOutputType(),
                    (RowType) correlate.getOutputType(),
                    joinType,
                    invocation,
                    condition);
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalStateException("Could not inspect StreamFusion array UNNEST support", e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException("StreamFusion array UNNEST support inspection failed", e.getCause());
        }
    }

    static String unsupportedReason(StreamExecExpand expand, ProcessorContext context) {
        return unsupportedExpandReason(expand, context);
    }

    static String unsupportedReason(BatchExecExpand expand, ProcessorContext context) {
        return unsupportedExpandReason(expand, context);
    }

    static String unsupportedExpandReason(CommonExecExpand expand, ProcessorContext context) {
        ExecEdge input = expand.getInputEdges().get(0);
        try {
            Class<?> translator = Class.forName(
                    EXPAND_TRANSLATOR_CLASS,
                    true,
                    context.getPlanner().getFlinkContext().getClassLoader());
            Method method = translator.getMethod("unsupportedReason", RowType.class, RowType.class, List.class);
            return (String) method.invoke(
                    null, (RowType) input.getOutputType(), (RowType) expand.getOutputType(), projects(expand));
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalStateException("Could not inspect StreamFusion Expand support", e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException("StreamFusion Expand support inspection failed", e.getCause());
        }
    }

    static String unsupportedReason(StreamExecValues values, ProcessorContext context) {
        return unsupportedValuesReason((RowType) values.getOutputType(), values.getTuples(), context);
    }

    static String unsupportedReason(BatchExecValues values, ProcessorContext context) {
        return unsupportedValuesReason((RowType) values.getOutputType(), values.getTuples(), context);
    }

    static String unsupportedValuesReason(RowType outputType, List<?> tuples, ProcessorContext context) {
        try {
            Class<?> translator = Class.forName(
                    VALUES_TRANSLATOR_CLASS,
                    true,
                    context.getPlanner().getFlinkContext().getClassLoader());
            Method method = translator.getMethod("unsupportedReason", RowType.class, List.class);
            return (String) method.invoke(null, outputType, tuples);
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalStateException("Could not inspect StreamFusion VALUES support", e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException("StreamFusion VALUES support inspection failed", e.getCause());
        }
    }

    static String unsupportedReason(StreamExecUnion union, ProcessorContext context) {
        return unsupportedUnionReason((RowType) union.getOutputType(), context);
    }

    static String unsupportedReason(BatchExecUnion union, ProcessorContext context) {
        return unsupportedUnionReason((RowType) union.getOutputType(), context);
    }

    static String unsupportedUnionReason(RowType outputType, ProcessorContext context) {
        if (context == null) {
            return null;
        }
        try {
            Class<?> translator = Class.forName(
                    UNION_TRANSLATOR_CLASS,
                    true,
                    context.getPlanner().getFlinkContext().getClassLoader());
            Method method = translator.getMethod("unsupportedReason", RowType.class);
            return (String) method.invoke(null, outputType);
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalStateException("Could not inspect StreamFusion UNION ALL support", e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException("StreamFusion UNION ALL support inspection failed", e.getCause());
        }
    }

    static String unsupportedReason(StreamExecWindowTableFunction window, ProcessorContext context) {
        return unsupportedWindowTableFunctionReason(window, context);
    }

    static String unsupportedReason(BatchExecWindowTableFunction window, ProcessorContext context) {
        TimeAttributeWindowingStrategy strategy = windowStrategy(window);
        if (strategy.isProctime()) {
            return "processing-time Window TVFs are not supported by Flink in batch mode";
        }
        if (strategy.getWindow() instanceof org.apache.flink.table.planner.plan.logical.SessionWindowSpec) {
            return "unaligned Window TVFs such as SESSION are not supported by Flink in batch mode";
        }
        return unsupportedWindowTableFunctionReason(window, context);
    }

    static String unsupportedWindowTableFunctionReason(CommonExecWindowTableFunction window, ProcessorContext context) {
        ExecEdge input = window.getInputEdges().get(0);
        try {
            Class<?> translator =
                    Class.forName(WINDOW_TRANSLATOR_CLASS, true, StreamFusionRuntimeClasses.class.getClassLoader());
            Method method = translator.getMethod(
                    "unsupportedReason",
                    RowType.class,
                    RowType.class,
                    TimeAttributeWindowingStrategy.class,
                    ReadableConfig.class);
            return (String) method.invoke(
                    null,
                    (RowType) input.getOutputType(),
                    (RowType) window.getOutputType(),
                    windowStrategy(window),
                    window.getPersistedConfig());
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalStateException("Could not inspect StreamFusion Window TVF support", e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException("StreamFusion Window TVF support inspection failed", e.getCause());
        }
    }
}
