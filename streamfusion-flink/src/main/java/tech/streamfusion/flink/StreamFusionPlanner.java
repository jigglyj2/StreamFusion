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
package tech.streamfusion.flink;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.ExplainFormat;
import org.apache.flink.table.api.PlanReference;
import org.apache.flink.table.delegation.InternalPlan;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.Operation;

/** Planner boundary where StreamFusion will replace supported Flink plans with native operators. */
final class StreamFusionPlanner implements Planner {
    private static final AtomicInteger TRANSLATED_PLANS = new AtomicInteger();

    private final Planner delegate;

    StreamFusionPlanner(Planner delegate) {
        this.delegate = delegate;
    }

    @Override
    public Parser getParser() {
        return delegate.getParser();
    }

    @Override
    public List<Transformation<?>> translate(List<ModifyOperation> modifyOperations) {
        TRANSLATED_PLANS.incrementAndGet();
        return delegate.translate(modifyOperations);
    }

    @Override
    public String explain(List<Operation> operations, ExplainFormat format, ExplainDetail... extraDetails) {
        return appendCurrentPlanningStatus(delegate.explain(operations, format, extraDetails));
    }

    @Override
    public InternalPlan loadPlan(PlanReference planReference) throws IOException {
        return delegate.loadPlan(planReference);
    }

    @Override
    public InternalPlan compilePlan(List<ModifyOperation> modifyOperations) {
        TRANSLATED_PLANS.incrementAndGet();
        return delegate.compilePlan(modifyOperations);
    }

    @Override
    public List<Transformation<?>> translatePlan(InternalPlan plan) {
        TRANSLATED_PLANS.incrementAndGet();
        return delegate.translatePlan(plan);
    }

    @Override
    public String explainPlan(InternalPlan plan, ExplainDetail... extraDetails) {
        return appendCurrentPlanningStatus(delegate.explainPlan(plan, extraDetails));
    }

    static int translatedPlanCount() {
        return TRANSLATED_PLANS.get();
    }

    static void resetMetrics() {
        TRANSLATED_PLANS.set(0);
    }

    private static String appendCurrentPlanningStatus(String flinkExplanation) {
        return flinkExplanation
                + System.lineSeparator()
                + System.lineSeparator()
                + "== StreamFusion Acceleration =="
                + System.lineSeparator()
                + "Accelerated: no"
                + System.lineSeparator()
                + "Plan reason: Flink plan conversion to StreamFusion physical operators is not implemented."
                + System.lineSeparator()
                + "Boundary reason: RowData-to-Arrow and Arrow-to-RowData transposes are TODO skeletons.";
    }
}
