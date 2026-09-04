/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.List;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.ExplainFormat;
import org.apache.flink.table.api.PlanReference;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.delegation.InternalPlan;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.Operation;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.state.StreamFusionStateBackendFactory;

class StreamFusionPlannerStateBackendTest {
    @Test
    void installsBackendInTableConfigBeforeRealTranslation() {
        TableConfig tableConfig = TableConfig.getDefault();
        tableConfig.getConfiguration().set(StateBackendOptions.STATE_BACKEND, "rocksdb");
        ObservingPlanner delegate = new ObservingPlanner(tableConfig);

        new StreamFusionPlanner(delegate).translate(List.of());

        assertThat(delegate.backendAtTranslation).isEqualTo(StreamFusionStateBackendFactory.class.getName());
    }

    public static final class ObservingPlanner implements Planner {
        private final TableConfig tableConfig;
        private String backendAtTranslation;

        private ObservingPlanner(TableConfig tableConfig) {
            this.tableConfig = tableConfig;
        }

        public TableConfig getTableConfig() {
            return tableConfig;
        }

        @Override
        public Parser getParser() {
            return null;
        }

        @Override
        public List<Transformation<?>> translate(List<ModifyOperation> modifyOperations) {
            backendAtTranslation = tableConfig.getConfiguration().get(StateBackendOptions.STATE_BACKEND);
            return List.of();
        }

        @Override
        public String explain(List<Operation> operations, ExplainFormat format, ExplainDetail... extraDetails) {
            return "";
        }

        @Override
        public InternalPlan loadPlan(PlanReference planReference) throws IOException {
            return null;
        }

        @Override
        public InternalPlan compilePlan(List<ModifyOperation> modifyOperations) {
            return null;
        }

        @Override
        public List<Transformation<?>> translatePlan(InternalPlan plan) {
            return List.of();
        }

        @Override
        public String explainPlan(InternalPlan plan, ExplainDetail... extraDetails) {
            return "";
        }
    }
}
