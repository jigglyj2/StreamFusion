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

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.delegation.PlannerFactory;
import org.apache.flink.table.planner.delegation.DefaultPlannerFactory;
import tech.streamfusion.nativebridge.NativeCalcBridge;

/** Entry point loaded by the StreamFusion Flink planner patch. */
public final class StreamFusionPlannerFactory implements PlannerFactory {
    public static final String FACTORY_CLASS_PROPERTY = "tech.streamfusion.flink.planner.factory";
    public static final String CALC_TRANSLATOR_CLASS_PROPERTY = "tech.streamfusion.flink.calc.translator";

    private static final AtomicInteger CREATED_PLANNERS = new AtomicInteger();

    @Override
    public Planner create(Context context) {
        CREATED_PLANNERS.incrementAndGet();
        System.setProperty(CALC_TRANSLATOR_CLASS_PROPERTY, "tech.streamfusion.flink.calc.StreamFusionCalcTranslator");
        return new StreamFusionPlanner(new DefaultPlannerFactory().create(context));
    }

    @Override
    public String factoryIdentifier() {
        return "streamfusion";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Set.of();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Set.of();
    }

    public static int createdPlannerCount() {
        return CREATED_PLANNERS.get();
    }

    public static int translatedPlanCount() {
        return StreamFusionPlanner.translatedPlanCount();
    }

    public static long nativeCalcBatchCount() {
        return NativeCalcBridge.executedBatchCount();
    }

    public static void resetMetrics() {
        CREATED_PLANNERS.set(0);
        StreamFusionPlanner.resetMetrics();
        NativeCalcBridge.resetMetrics();
        System.clearProperty(CALC_TRANSLATOR_CLASS_PROPERTY);
    }
}
