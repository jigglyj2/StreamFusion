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
package tech.streamfusion.flink.runner;

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

/** A runner-only function that provides a stable negative control for planner fallback. */
public final class PlannerFallbackFixture extends ScalarFunction {
    public static final String FUNCTION_NAME = "STREAMFUSION_RUNNER_FALLBACK";
    public static final String SQL = "SELECT " + FUNCTION_NAME + "(id) FROM (VALUES (1), (2)) AS input(id)";

    public static void register(TableEnvironment tables) {
        tables.createTemporarySystemFunction(FUNCTION_NAME, PlannerFallbackFixture.class);
    }

    public int eval(int value) {
        return -value;
    }
}
