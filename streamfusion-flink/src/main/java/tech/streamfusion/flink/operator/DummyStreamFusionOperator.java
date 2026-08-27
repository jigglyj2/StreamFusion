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

/** Test-only stand-in proving all-internal-node planning semantics. */
public final class DummyStreamFusionOperator implements StreamFusionPhysicalOperator {
    private final String name;

    public DummyStreamFusionOperator(String name) {
        this.name = name;
    }

    @Override
    public String name() {
        return name;
    }
}
