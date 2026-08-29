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

import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

/** Creates one StreamFusion multi-input operator for a UNION ALL physical node. */
final class StreamFusionUnionOperatorFactory extends AbstractStreamOperatorFactory<RowData> {
    private final int inputCount;
    private final RowType rowType;

    StreamFusionUnionOperatorFactory(int inputCount, RowType rowType) {
        this.inputCount = inputCount;
        this.rowType = rowType;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends StreamOperator<RowData>> T createStreamOperator(StreamOperatorParameters<RowData> parameters) {
        return (T) new StreamFusionUnionOperator(parameters, inputCount, rowType);
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return StreamFusionUnionOperator.class;
    }
}
