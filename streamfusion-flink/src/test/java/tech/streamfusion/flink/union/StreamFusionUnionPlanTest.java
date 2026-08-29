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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import tech.streamfusion.proto.plan.v1.NativePlan;

class StreamFusionUnionPlanTest {
    @Test
    void createsOneIndexedNativeInputPerFlinkInput() throws Exception {
        NativePlan plan = NativePlan.parseFrom(StreamFusionUnionOperator.createPlan(3));

        assertThat(plan.getRoot().getUnion().getInputsList())
                .extracting(operator -> operator.getInput().getInputIndex())
                .containsExactly(0, 1, 2);
    }
}
