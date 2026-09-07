/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.ExecutionConfigOptions.UidGeneration;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecCalc;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class StreamFusionNativeUidMetadataTest {
    private static final RowType TYPE = RowType.of(new IntType());

    static Stream<Arguments> uidCases() {
        return Stream.of(UidGeneration.values()).flatMap(policy -> Stream.of(false, true)
                .flatMap(compiled -> Stream.of("<id>_<transformation>", "計算:<type>:<version>:<id>:<transformation>", "")
                        .map(format -> Arguments.of(policy, compiled, format))));
    }

    @ParameterizedTest
    @MethodSource("uidCases")
    void preservesFlinkUidPolicyCompiledStatusAndFormat(UidGeneration policy, boolean compiled, String format) {
        var config = new Configuration();
        config.set(ExecutionConfigOptions.TABLE_EXEC_UID_GENERATION, policy);
        config.set(ExecutionConfigOptions.TABLE_EXEC_UID_FORMAT, format);
        var origin = original(new Configuration());
        origin.setCompiled(compiled);
        var metadata = new StreamFusionNativeNodeMetadata();
        metadata.bindOriginal(origin);
        String expected = policy == UidGeneration.ALWAYS || policy == UidGeneration.PLAN_ONLY && compiled
                ? format.replace("<id>", Integer.toString(origin.getId()))
                        .replace("<type>", "stream-exec-calc")
                        .replace("<version>", "1")
                        .replace("<transformation>", "calc")
                : null;
        if (expected != null && expected.isEmpty()) {
            assertThatThrownBy(() -> metadata.metricUid(config)).hasMessageContaining("Empty string operator uid");
        } else {
            assertThat(metadata.metricUid(config)).isEqualTo(expected);
        }
        assertThat(origin.getTransformation()).isNull();
    }

    @Test
    void persistedPolicyOverridesTheCurrentSessionAndUnboundNodesHaveNoInventedUid() {
        var persisted = new Configuration();
        persisted.set(ExecutionConfigOptions.TABLE_EXEC_UID_GENERATION, UidGeneration.DISABLED);
        var session = new Configuration();
        session.set(ExecutionConfigOptions.TABLE_EXEC_UID_GENERATION, UidGeneration.ALWAYS);
        var metadata = new StreamFusionNativeNodeMetadata();
        assertThat(metadata.metricUid(session)).isNull();
        metadata.bindOriginal(original(persisted));
        assertThat(metadata.metricUid(session)).isNull();
    }

    @Test
    void sharedGraphRewriteRejectsDuplicateExplicitUidsAcrossNativeStages() {
        var config = new Configuration();
        config.set(ExecutionConfigOptions.TABLE_EXEC_UID_GENERATION, UidGeneration.ALWAYS);
        config.set(ExecutionConfigOptions.TABLE_EXEC_UID_FORMAT, "same-uid");
        var rewrite = new StreamFusionGraphRewrite(config);
        var first = original(new Configuration());
        var second = original(new Configuration());
        var replacement = selected();
        assertThat(rewrite.convert(first, ignored -> replacement)).isSameAs(replacement);
        assertThat(rewrite.convert(first, ignored -> {
                    throw new AssertionError("must reuse stage");
                }))
                .isSameAs(replacement);
        assertThatThrownBy(() -> rewrite.convert(second, ignored -> selected()))
                .hasMessageContaining(
                        "Duplicate explicit Flink UID", "same-uid", "node " + first.getId(), "node " + second.getId());
        assertThat(first.getTransformation()).isNull();
        assertThat(second.getTransformation()).isNull();
    }

    private static StreamFusionExecCalc selected() {
        return new StreamFusionExecCalc(new Configuration(), List.of(), null, InputProperty.DEFAULT, TYPE, "Calc");
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
                "Calc");
    }
}
