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

import java.util.List;
import java.util.Optional;
import org.apache.calcite.rex.RexNode;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.WatermarkGeneratorCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.runtime.generated.GeneratedWatermarkGenerator;
import org.apache.flink.table.runtime.operators.wmassigners.WatermarkAssignerOperatorFactory;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

/** StreamFusion physical node that retains Flink's watermark state machine and coordination. */
public final class StreamFusionExecWatermarkAssigner extends ExecNodeBase<RowData> implements StreamExecNode<RowData> {
    private static final String TRANSFORMATION_NAME = "streamfusion-watermark-assigner";

    private final RexNode watermarkExpression;
    private final int rowtimeFieldIndex;

    public StreamFusionExecWatermarkAssigner(
            ReadableConfig persistedConfig,
            RexNode watermarkExpression,
            int rowtimeFieldIndex,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        super(
                ExecNodeContext.newNodeId(),
                new ExecNodeContext("streamfusion-exec-watermark-assigner_1"),
                persistedConfig,
                List.of(inputProperty),
                outputType,
                description);
        this.watermarkExpression = watermarkExpression;
        this.rowtimeFieldIndex = rowtimeFieldIndex;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner, ExecNodeConfig config) {
        Transformation<RowData> input =
                (Transformation<RowData>) getInputEdges().get(0).translateToPlan(planner);
        GeneratedWatermarkGenerator generator = WatermarkGeneratorCodeGenerator.generateWatermarkGenerator(
                config,
                planner.getFlinkContext().getClassLoader(),
                (RowType) getInputEdges().get(0).getOutputType(),
                watermarkExpression,
                JavaScalaConversionUtil.toScala(Optional.empty()),
                JavaScalaConversionUtil.toScala(Optional.empty()));
        long idleTimeout = config.get(ExecutionConfigOptions.TABLE_EXEC_SOURCE_IDLE_TIMEOUT)
                .toMillis();
        WatermarkAssignerOperatorFactory factory =
                new WatermarkAssignerOperatorFactory(rowtimeFieldIndex, idleTimeout, generator);
        return ExecNodeUtil.createOneInputTransformation(
                input,
                createTransformationMeta(TRANSFORMATION_NAME, config),
                factory,
                InternalTypeInfo.of(getOutputType()),
                input.getParallelism(),
                false);
    }
}
