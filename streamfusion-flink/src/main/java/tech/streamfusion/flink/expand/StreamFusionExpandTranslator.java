/*
 * Copyright 2026 StreamFusion Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tech.streamfusion.flink.expand;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchTypeInfo;
import tech.streamfusion.flink.arrow.StreamFusionArrowBoundaries;
import tech.streamfusion.flink.calc.StreamFusionCalcTranslator;
import tech.streamfusion.flink.memory.StreamFusionTaskMemory;
import tech.streamfusion.flink.operator.StreamFusionArrowNativeOperator;
import tech.streamfusion.proto.plan.v1.Expression;

/** Reflection entry point used by the planner extension for native Expand. */
public final class StreamFusionExpandTranslator {
    private StreamFusionExpandTranslator() {}

    public static Transformation<RowData> translate(
            Transformation<RowData> input, RowType inputType, RowType outputType, List<List<?>> projects) {
        if (unsupportedReason(inputType, outputType, projects) != null) {
            return null;
        }
        List<List<Expression>> nativeProjects = new ArrayList<>(projects.size());
        for (List<?> project : projects) {
            List<Expression> expressions = new ArrayList<>(project.size());
            for (int index = 0; index < project.size(); index++) {
                expressions.add(StreamFusionCalcTranslator.operatorExpression(
                        project.get(index), inputType, outputType.getTypeAt(index)));
            }
            nativeProjects.add(expressions);
        }
        Transformation<ArrowRowDataBatch> arrowInput = StreamFusionArrowBoundaries.toArrow(input, inputType);
        OneInputTransformation<ArrowRowDataBatch, ArrowRowDataBatch> transformation = new OneInputTransformation<>(
                arrowInput,
                "streamfusion-expand[" + projects.size() + "]",
                new StreamFusionArrowNativeOperator(
                        outputType, StreamFusionExpandPlan.create(nativeProjects), "streamfusion-expand"),
                ArrowRowDataBatchTypeInfo.INSTANCE,
                input.getParallelism(),
                false);
        transformation.declareManagedMemoryUseCaseAtOperatorScope(
                ManagedMemoryUseCase.OPERATOR, StreamFusionTaskMemory.MANAGED_MEMORY_WEIGHT);
        return StreamFusionArrowBoundaries.asPlannerTransformation(transformation);
    }

    public static String unsupportedReason(RowType inputType, RowType outputType, List<List<?>> projects) {
        if (projects.isEmpty()) {
            return "projects: Expand must contain at least one projection";
        }
        for (int index = 0; index < projects.size(); index++) {
            String reason =
                    StreamFusionCalcTranslator.unsupportedReason(inputType, outputType, projects.get(index), null);
            if (reason != null) {
                return "projects[" + index + "]/" + reason;
            }
        }
        return null;
    }
}
