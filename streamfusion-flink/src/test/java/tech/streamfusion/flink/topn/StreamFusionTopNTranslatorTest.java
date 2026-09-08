/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.topn;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.planner.plan.nodes.exec.spec.SortSpec;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

class StreamFusionTopNTranslatorTest {
    private static final RowType ROW_TYPE =
            RowType.of(new LogicalType[] {new BigIntType(false), new BigIntType(true)}, new String[] {"id", "value"});

    @Test
    void acceptsFlinkGlobalLimitAndOffsetShape() {
        assertThat(reason(new int[0], SortSpec.ANY, 1, 3L, null, false)).isNull();
        assertThat(reason(new int[0], SortSpec.ANY, 3, 7L, null, false)).isNull();
    }

    @Test
    void rejectsOtherUnorderedRankShapes() {
        assertThat(reason(new int[] {0}, SortSpec.ANY, 1, 3L, null, false)).contains("global constant LIMIT/OFFSET");
        assertThat(reason(new int[0], SortSpec.ANY, 1, 3L, null, true)).contains("global constant LIMIT/OFFSET");
        assertThat(reason(new int[0], SortSpec.ANY, 1, null, 1, false)).contains("global constant LIMIT/OFFSET");
    }

    @Test
    void sharedFragmentRequiresVerifiedTopOneRangeAndStateConfiguration() throws Exception {
        var config = new Configuration();
        var node = tech.streamfusion.proto.plan.v1.NativePlan.parseFrom(shared(config, 1))
                .getRoot()
                .getTopN();
        assertThat(node.getRankEnd()).isOne();
        assertThat(node.getInput().hasInput()).isTrue();
        assertThatThrownBy(() -> shared(config, 2)).hasMessageContaining("range [1,1]");
        config.set(org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_ASYNC_STATE_ENABLED, true);
        assertThatThrownBy(() -> shared(config, 1)).hasMessageContaining("synchronous state");
        config.set(org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_ASYNC_STATE_ENABLED, false);
        config.set(org.apache.flink.configuration.StateLatencyTrackOptions.LATENCY_TRACK_ENABLED, true);
        assertThatThrownBy(() -> shared(config, 1)).hasMessageContaining("keyed-state latency");
    }

    private static byte[] shared(Configuration config, long end) {
        return StreamFusionTopNTranslator.createStagePlan(
                ROW_TYPE,
                ROW_TYPE,
                new int[] {0},
                SortSpec.builder().addField(1, false, true).build(),
                new int[0],
                1,
                end,
                null,
                false,
                true,
                "APPEND_FAST",
                0,
                config);
    }

    private static String reason(
            int[] partitionKeys,
            SortSpec sortSpec,
            long rankStart,
            Long rankEnd,
            Integer variableRankEnd,
            boolean outputRankNumber) {
        return StreamFusionTopNTranslator.unsupportedReason(
                ROW_TYPE,
                ROW_TYPE,
                partitionKeys,
                sortSpec,
                new int[0],
                rankStart,
                rankEnd,
                variableRankEnd,
                outputRankNumber,
                "APPEND_FAST",
                0,
                new Configuration());
    }
}
