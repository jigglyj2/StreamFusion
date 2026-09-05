/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.sql;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

class BoundedSortLimitParityTest extends SqlParityTestSupport {
    @Test
    void boundedRankPreservesTiesPartitionResetsAndGappedRanks() throws Exception {
        assertParity(
                "SELECT category, amount, rank_value FROM (SELECT category, amount, "
                        + "RANK() OVER (PARTITION BY category ORDER BY amount DESC) AS rank_value "
                        + "FROM (VALUES ('a', 9), ('a', 9), ('a', 8), ('a', 7), "
                        + "('b', 5), ('b', 4)) AS input(category, amount)) "
                        + "WHERE rank_value BETWEEN 2 AND 3",
                false);

        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }

    @ParameterizedTest(name = "rank-{0}")
    @MethodSource("tech.streamfusion.flink.sql.TopNOrderingTypeParityTest#orderableTypes")
    void everyFlinkOrderableTypeWorksAsABoundedRankKey(
            String description, TypeInformation<?> type, DataType dataType, Object low, Object high) throws Exception {
        assertBatchDataStreamParity(
                "SELECT metric, rank_value FROM (SELECT metric, RANK() OVER (ORDER BY metric ASC NULLS LAST) "
                        + "AS rank_value FROM bounded_rank_input) WHERE rank_value <= 3",
                type,
                dataType,
                List.of(Row.of(high), Row.of(low), Row.of(high), Row.of((Object) null)),
                "bounded_rank_input");

        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }

    @Test
    void unorderedTwoStageLimitOffsetPreservesFlinkPhysicalOrderAndKinds() throws Exception {
        assertParity(
                "SELECT id, label FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')) "
                        + "AS input(id, label) LIMIT 3 OFFSET 1",
                false);

        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }

    @Test
    void twoStageOrderByLimitOffsetMatchesFlinkByteForByte() throws Exception {
        assertParity(
                "SELECT id, label FROM (VALUES (3, 'c'), (1, 'a'), (1, 'again'), "
                        + "(CAST(NULL AS INT), 'null'), (4, 'd'), (2, 'b')) AS input(id, label) "
                        + "ORDER BY id ASC NULLS LAST, label DESC NULLS FIRST LIMIT 3 OFFSET 1",
                false);

        assertThat(StreamFusionPlannerFactory.nativeBoundedSortBatchCount()).isGreaterThan(0);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }

    @Test
    void sortLimitTieAtTheCutoffMatchesFlinksSelectedPayloads() throws Exception {
        assertParity(
                "SELECT score, payload FROM (VALUES (1, 'first'), (1, 'second'), "
                        + "(1, 'third'), (1, 'fourth')) AS input(score, payload) "
                        + "ORDER BY score LIMIT 2",
                false);

        assertThat(StreamFusionPlannerFactory.nativeBoundedSortBatchCount()).isGreaterThan(0);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("tech.streamfusion.flink.sql.TopNOrderingTypeParityTest#orderableTypes")
    void everyFlinkOrderableTypeWorksAsASortLimitKey(
            String description, TypeInformation<?> type, DataType dataType, Object low, Object high) throws Exception {
        assertBatchDataStreamParity(
                "SELECT metric FROM bounded_sort_limit_input ORDER BY metric ASC NULLS LAST LIMIT 2 OFFSET 1",
                type,
                dataType,
                List.of(Row.of(high), Row.of(low), Row.of(high), Row.of((Object) null)),
                "bounded_sort_limit_input");

        assertThat(StreamFusionPlannerFactory.nativeBoundedSortBatchCount()).isGreaterThan(0);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }
}
