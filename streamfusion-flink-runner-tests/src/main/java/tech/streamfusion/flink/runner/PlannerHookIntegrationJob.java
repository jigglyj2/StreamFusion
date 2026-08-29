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

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import tech.streamfusion.flink.StreamFusionPlannerFactory;

/** A submitted Flink job that proves the installed StreamFusion JAR owns planner creation. */
public final class PlannerHookIntegrationJob {
    private static final String SUCCESS_MARKER = "STREAMFUSION_RUNNER_INTEGRATION_OK";

    private PlannerHookIntegrationJob() {}

    public static void main(String[] args) throws Exception {
        System.setProperty(
                StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, StreamFusionPlannerFactory.class.getName());
        StreamFusionPlannerFactory.resetMetrics();

        TableEnvironment tables = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        List<Integer> ids = new ArrayList<>();
        try (CloseableIterator<Row> rows = tables.executeSql(
                        "SELECT id FROM (VALUES (1), (2), (3)) AS input(id) WHERE id >= 2")
                .collect()) {
            while (rows.hasNext()) {
                ids.add((Integer) rows.next().getField(0));
            }
        }
        ids.sort(Integer::compareTo);

        if (!ids.equals(List.of(2, 3))) {
            throw new IllegalStateException("Unexpected SQL result: " + ids);
        }
        if (StreamFusionPlannerFactory.createdPlannerCount() != 1) {
            throw new IllegalStateException(
                    "Expected one StreamFusion planner, got " + StreamFusionPlannerFactory.createdPlannerCount());
        }
        if (StreamFusionPlannerFactory.translatedPlanCount() == 0) {
            throw new IllegalStateException("The StreamFusion planner did not translate the submitted job");
        }
        if (StreamFusionPlannerFactory.nativeCalcBatchCount() == 0) {
            throw new IllegalStateException("The submitted job did not execute a native calc batch");
        }

        long batchesBeforeUnion = StreamFusionPlannerFactory.nativeCalcBatchCount();
        List<Integer> unionValues = new ArrayList<>();
        String unionSql = "SELECT id + 10 FROM (VALUES (1), (2)) AS left_input(id) WHERE id >= 1 "
                + "UNION ALL "
                + "SELECT id + 20 FROM (VALUES (2), (3)) AS right_input(id) WHERE id >= 2";
        try (CloseableIterator<Row> rows = tables.executeSql(unionSql).collect()) {
            while (rows.hasNext()) {
                unionValues.add((Integer) rows.next().getField(0));
            }
        }
        unionValues.sort(Integer::compareTo);
        if (!unionValues.equals(List.of(11, 12, 22, 23))) {
            throw new IllegalStateException("Unexpected UNION ALL result: " + unionValues);
        }
        if (StreamFusionPlannerFactory.nativeCalcBatchCount() < batchesBeforeUnion + 2) {
            throw new IllegalStateException("UNION ALL did not execute both native Calc branches");
        }

        long acceleratedBatches = StreamFusionPlannerFactory.nativeCalcBatchCount();
        PlannerFallbackFixture.register(tables);
        List<Integer> fallbackValues = new ArrayList<>();
        try (CloseableIterator<Row> rows =
                tables.executeSql(PlannerFallbackFixture.SQL).collect()) {
            while (rows.hasNext()) {
                fallbackValues.add((Integer) rows.next().getField(0));
            }
        }
        fallbackValues.sort(Integer::compareTo);
        if (!fallbackValues.equals(List.of(-2, -1))) {
            throw new IllegalStateException("Unexpected Flink fallback result: " + fallbackValues);
        }
        if (StreamFusionPlannerFactory.nativeCalcBatchCount() != acceleratedBatches) {
            throw new IllegalStateException("The runner-only fallback fixture unexpectedly ran in StreamFusion");
        }

        System.out.println(SUCCESS_MARKER
                + " planners="
                + StreamFusionPlannerFactory.createdPlannerCount()
                + " translations="
                + StreamFusionPlannerFactory.translatedPlanCount()
                + " nativeCalcBatches="
                + StreamFusionPlannerFactory.nativeCalcBatchCount()
                + " unionAll=verified fallback=verified");
    }
}
