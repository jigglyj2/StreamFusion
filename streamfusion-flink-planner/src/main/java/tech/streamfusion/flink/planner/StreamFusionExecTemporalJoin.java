/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Optional;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.CodeGenUtils;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.ExprCodeGenerator;
import org.apache.flink.table.planner.codegen.FunctionCodeGenerator;
import org.apache.flink.table.planner.codegen.GeneratedExpression;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.spec.JoinSpec;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

/** Distinct StreamFusion physical node for temporal table joins. */
public final class StreamFusionExecTemporalJoin extends ExecNodeBase<RowData> implements StreamExecNode<RowData> {
    private static final String TRANSLATOR_CLASS = "tech.streamfusion.flink.join.StreamFusionTemporalJoinTranslator";

    private final JoinSpec joinSpec;
    private final boolean temporalFunctionJoin;
    private final int leftTimeIndex;
    private final int rightTimeIndex;

    public StreamFusionExecTemporalJoin(
            ReadableConfig config,
            JoinSpec joinSpec,
            boolean temporalFunctionJoin,
            int leftTimeIndex,
            int rightTimeIndex,
            InputProperty leftInput,
            InputProperty rightInput,
            RowType outputType,
            String description) {
        super(
                ExecNodeContext.newNodeId(),
                new ExecNodeContext("streamfusion-exec-temporal-join_1"),
                config,
                List.of(leftInput, rightInput),
                outputType,
                description);
        this.joinSpec = joinSpec;
        this.temporalFunctionJoin = temporalFunctionJoin;
        this.leftTimeIndex = leftTimeIndex;
        this.rightTimeIndex = rightTimeIndex;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner, ExecNodeConfig config) {
        ExecEdge leftEdge = getInputEdges().get(0);
        ExecEdge rightEdge = getInputEdges().get(1);
        Transformation<RowData> left = (Transformation<RowData>) leftEdge.translateToPlan(planner);
        Transformation<RowData> right = (Transformation<RowData>) rightEdge.translateToPlan(planner);
        RowType leftType = (RowType) leftEdge.getOutputType();
        RowType rightType = (RowType) rightEdge.getOutputType();
        RowDataKeySelector leftSelector = KeySelectorUtil.getRowDataSelector(
                planner.getFlinkContext().getClassLoader(), joinSpec.getLeftKeys(), InternalTypeInfo.of(leftType));
        RowDataKeySelector rightSelector = KeySelectorUtil.getRowDataSelector(
                planner.getFlinkContext().getClassLoader(), joinSpec.getRightKeys(), InternalTypeInfo.of(rightType));
        GeneratedJoinCondition condition =
                generatedCondition(config, planner.getFlinkContext().getClassLoader(), leftType, rightType);
        try {
            Class<?> translator = Class.forName(
                    TRANSLATOR_CLASS, true, planner.getFlinkContext().getClassLoader());
            Method method = translator.getMethod(
                    "translate",
                    Transformation.class,
                    Transformation.class,
                    RowType.class,
                    RowType.class,
                    RowType.class,
                    JoinSpec.class,
                    boolean.class,
                    int.class,
                    int.class,
                    ReadableConfig.class,
                    StreamExecutionEnvironment.class,
                    RowDataKeySelector.class,
                    RowDataKeySelector.class,
                    GeneratedJoinCondition.class);
            Transformation<RowData> result = (Transformation<RowData>) method.invoke(
                    null,
                    left,
                    right,
                    leftType,
                    rightType,
                    (RowType) getOutputType(),
                    joinSpec,
                    temporalFunctionJoin,
                    leftTimeIndex,
                    rightTimeIndex,
                    getPersistedConfig(),
                    planner.getExecEnv(),
                    leftSelector,
                    rightSelector,
                    condition);
            if (result == null) {
                throw new IllegalStateException("A selected StreamFusion temporal join failed translation");
            }
            return result;
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException failure) {
            throw new IllegalStateException("Could not invoke the StreamFusion temporal join runtime", failure);
        } catch (InvocationTargetException failure) {
            throw new IllegalStateException("StreamFusion temporal join translation failed", failure.getCause());
        }
    }

    private GeneratedJoinCondition generatedCondition(
            ExecNodeConfig config, ClassLoader classLoader, RowType leftType, RowType rightType) {
        if (joinSpec.getNonEquiCondition().isEmpty()) {
            return null;
        }
        CodeGeneratorContext context = new CodeGeneratorContext(config, classLoader);
        ExprCodeGenerator generator = new ExprCodeGenerator(context, false)
                .bindInput(
                        leftType, CodeGenUtils.DEFAULT_INPUT1_TERM(), JavaScalaConversionUtil.toScala(Optional.empty()))
                .bindSecondInput(
                        rightType,
                        CodeGenUtils.DEFAULT_INPUT2_TERM(),
                        JavaScalaConversionUtil.toScala(Optional.empty()));
        GeneratedExpression condition =
                generator.generateExpression(joinSpec.getNonEquiCondition().get());
        String body = String.format("%s\nreturn %s;", condition.code(), condition.resultTerm());
        return FunctionCodeGenerator.generateJoinCondition(
                context,
                "StreamFusionTemporalCondition",
                body,
                CodeGenUtils.DEFAULT_INPUT1_TERM(),
                CodeGenUtils.DEFAULT_INPUT2_TERM());
    }
}
