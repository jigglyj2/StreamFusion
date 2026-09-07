/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecCalc;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

class StreamFusionNativeNodeMetadataTest {
    private static final RowType TYPE = RowType.of(new IntType());
    private static final String DESCRIPTION = "Calc(select=[value AS résultat])";

    @Test
    void graphReplacementRetainsOriginalIdentityAndFlinksConfiguredName() {
        var original = original(new Configuration());
        var selected = selected();
        var rewrite = new StreamFusionGraphRewrite();
        assertThat(rewrite.convert(original, ignored -> selected)).isSameAs(selected);
        assertThat(selected.getId()).isNotEqualTo(original.getId());
        assertThat(selected.nativeMetadata().physicalNodeId(selected)).isEqualTo(original.getId());
        for (boolean simplified : List.of(false, true)) {
            var config = new Configuration();
            config.set(ExecutionConfigOptions.TABLE_EXEC_SIMPLIFY_OPERATOR_NAME_ENABLED, simplified);
            assertThat(selected.nativeMetadata().metricName(selected, config))
                    .isEqualTo(simplified ? "Calc[" + original.getId() + "]" : DESCRIPTION);
        }
        assertThat(original.getTransformation()).isNull();
        assertThat(selected.getTransformation()).isNull();
    }

    @Test
    void persistedNodeNamingTakesPrecedenceOverCurrentTableConfig() {
        var nodeConfig = new Configuration();
        nodeConfig.set(ExecutionConfigOptions.TABLE_EXEC_SIMPLIFY_OPERATOR_NAME_ENABLED, false);
        var tableConfig = new Configuration();
        tableConfig.set(ExecutionConfigOptions.TABLE_EXEC_SIMPLIFY_OPERATOR_NAME_ENABLED, true);
        var original = original(nodeConfig);
        var selected = selected();
        selected.nativeMetadata().bindOriginal(original);
        assertThat(selected.nativeMetadata().metricName(selected, tableConfig)).isEqualTo(DESCRIPTION);
    }

    @Test
    void originCanBeReusedButCannotBeReboundToAnotherPhysicalStage() {
        var selected = selected();
        assertThat(selected.nativeMetadata().physicalNodeId(selected)).isEqualTo(selected.getId());
        var original = original(new Configuration());
        selected.nativeMetadata().bindOriginal(original);
        selected.nativeMetadata().bindOriginal(original);
        assertThatThrownBy(() -> selected.nativeMetadata().bindOriginal(original(new Configuration())))
                .hasMessageContaining("cannot change its original metric identity");
    }

    private static StreamFusionExecCalc selected() {
        return new StreamFusionExecCalc(new Configuration(), List.of(), null, InputProperty.DEFAULT, TYPE, DESCRIPTION);
    }

    private static StreamExecCalc original(Configuration persisted) {
        return new StreamExecCalc(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(StreamExecCalc.class),
                persisted,
                List.of(),
                null,
                List.of(InputProperty.DEFAULT),
                TYPE,
                DESCRIPTION);
    }
}
