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
package tech.streamfusion.flink.union;

import java.util.List;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.streaming.api.transformations.MultipleInputTransformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.calc.StreamFusionTimestampRangeSupport;

/** Reflection entry point used by the planner extension for native UNION ALL. */
public final class StreamFusionUnionTranslator {
    private StreamFusionUnionTranslator() {}

    public static String unsupportedReason(RowType rowType) {
        for (int index = 0; index < rowType.getFieldCount(); index++) {
            String reason = StreamFusionTimestampRangeSupport.unsupportedReason(
                    rowType.getTypeAt(index), "fields[" + index + "]");
            if (reason != null) {
                return reason;
            }
        }
        return null;
    }

    public static Transformation<RowData> translate(List<Transformation<RowData>> inputs, RowType rowType) {
        if (inputs.size() < 2 || unsupportedReason(rowType) != null) {
            return null;
        }
        int parallelism =
                inputs.stream().mapToInt(Transformation::getParallelism).max().orElse(1);
        MultipleInputTransformation<RowData> transformation = new MultipleInputTransformation<>(
                "streamfusion-union-all[" + inputs.size() + "]",
                new StreamFusionUnionOperatorFactory(inputs.size(), rowType),
                InternalTypeInfo.of(rowType),
                parallelism,
                false);
        inputs.forEach(transformation::addInput);
        transformation.declareManagedMemoryUseCaseAtOperatorScope(ManagedMemoryUseCase.OPERATOR, 1);
        return transformation;
    }
}
