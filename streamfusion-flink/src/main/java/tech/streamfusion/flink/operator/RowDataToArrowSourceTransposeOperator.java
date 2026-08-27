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
package tech.streamfusion.flink.operator;

/** Planned boundary from a Flink {@code RowData} source to native Arrow batches. */
public final class RowDataToArrowSourceTransposeOperator implements StreamFusionPhysicalOperator {
    @Override
    public String name() {
        return "StreamFusionRowDataToArrow";
    }

    /** The marker participates in planning tests, but the runtime transpose remains TODO. */
    public boolean isRuntimeImplemented() {
        return false;
    }
}
