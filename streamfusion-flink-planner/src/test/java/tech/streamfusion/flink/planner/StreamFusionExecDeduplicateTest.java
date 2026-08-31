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
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecDeduplicate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecTableSourceScan;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;
import org.junit.jupiter.api.Test;

class StreamFusionExecDeduplicateTest {
    @Test
    void replacesFlinkQ18DeduplicateWithDistinctStreamFusionNode() {
        Configuration configuration = new Configuration();
        RowType rowType = RowType.of(
                new LogicalType[] {
                    new BigIntType(false), new BigIntType(false), new TimestampType(false, TimestampKind.ROWTIME, 3)
                },
                new String[] {"auction", "bidder", "dateTime"});
        StreamExecTableSourceScan source = new StreamExecTableSourceScan(configuration, null, rowType, "source");
        source.setInputEdges(List.of());
        StreamExecDeduplicate deduplicate = new StreamExecDeduplicate(
                configuration,
                new int[] {1, 0},
                true,
                true,
                false,
                false,
                InputProperty.DEFAULT,
                rowType,
                "Q18 rowtime keep-last");
        deduplicate.setInputEdges(
                List.of(ExecEdge.builder().source(source).target(deduplicate).build()));

        ExecNode<?> replacement = new StreamFusionExecGraphProcessor().convert(deduplicate);

        assertThat(replacement).isInstanceOf(StreamFusionExecDeduplicate.class);
        assertThat(replacement.getInputEdges())
                .singleElement()
                .extracting(ExecEdge::getSource)
                .isEqualTo(source);
    }
}
