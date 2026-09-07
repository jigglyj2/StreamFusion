/* Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0 */
package tech.streamfusion.flink.sql;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.SelectedAggregateSqlProbe;

/** Real Flink SQL local/exchange/global graphs, retaining the production admission preflight. */
class SelectedTwoPhaseAggregateSqlIntegrationTest extends SqlParityTestSupport {
    @Test
    void generatedTwoPhaseGraphsMatchFlinkOnBothBackends() throws Exception {
        for (boolean rocks : List.of(false, true))
            for (int trigger : List.of(1, 3, 100))
                for (int seed = 0; seed < 3; seed++)
                    for (String sql : List.of(
                            "SELECT k, COUNT(*), SUM(v), MIN(v), MAX(v) FROM native_input GROUP BY k",
                            "SELECT COUNT(*), SUM(v) FROM native_input")) {
                        var rows = rows(seed);
                        byte[] flink = SelectedAggregateSqlIntegrationTest.executeGraph(
                                sql, rocks, false, trigger, true, rows);
                        String graph = SelectedAggregateSqlProbe.inputGraph;
                        assertThat(graph).contains("LocalGroupAggregate", "GlobalGroupAggregate");
                        byte[] nativeResult =
                                SelectedAggregateSqlIntegrationTest.executeGraph(sql, rocks, true, trigger, true, rows);
                        assertThat(SelectedAggregateSqlProbe.inputGraph).isEqualTo(graph);
                        assertThat(nativeResult)
                                .as("rocks=%s trigger=%s seed=%s SQL=%s", rocks, trigger, seed, sql)
                                .isEqualTo(flink);
                        assertThat(StreamFusionPlannerFactory.nativePlanBatchCount())
                                .isPositive();
                        assertThat(StreamFusionPlannerFactory.nativeGroupAggregateBatchCount())
                                .isZero();
                        assertThat(StreamFusionPlannerFactory.nativeLocalGroupAggregateBatchCount())
                                .isZero();
                        SelectedAggregateSqlProbe.verifyTranslatedArrowTopology();
                    }
    }

    private static List<org.apache.flink.types.Row> rows(int seed) {
        var random = new java.util.Random(seed);
        var live = new java.util.ArrayList<org.apache.flink.types.Row>();
        var result = new java.util.ArrayList<org.apache.flink.types.Row>();
        for (int i = 0; i < 41; i++) {
            org.apache.flink.types.Row row;
            if (!live.isEmpty() && random.nextBoolean()) {
                var old = live.remove(random.nextInt(live.size()));
                row = org.apache.flink.types.Row.ofKind(
                        i % 2 == 0
                                ? org.apache.flink.types.RowKind.DELETE
                                : org.apache.flink.types.RowKind.UPDATE_BEFORE,
                        old.getField(0),
                        old.getField(1));
            } else {
                row = org.apache.flink.types.Row.ofKind(
                        i % 2 == 0
                                ? org.apache.flink.types.RowKind.INSERT
                                : org.apache.flink.types.RowKind.UPDATE_AFTER,
                        i % 7 == 0 ? null : "é-" + random.nextInt(5),
                        i % 5 == 0 ? null : (long) random.nextInt(17));
                live.add(row);
            }
            result.add(row);
        }
        for (var row : live)
            result.add(org.apache.flink.types.Row.ofKind(
                    org.apache.flink.types.RowKind.DELETE, row.getField(0), row.getField(1)));
        return result;
    }
}
