/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.spec.SortSpec;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecTemporalSort;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;
import org.junit.jupiter.api.Test;

class StreamFusionExecTemporalSortTest {
    @Test
    void replacesFlinkEventTimeSortWithADistinctNativeNode() {
        Configuration configuration = new Configuration();
        RowType rowType = RowType.of(
                new LogicalType[] {new TimestampType(false, TimestampKind.ROWTIME, 3), new IntType()},
                new String[] {"event_time", "priority"});
        StreamExecTableSourceScan source = new StreamExecTableSourceScan(configuration, null, rowType, "source");
        source.setInputEdges(List.of());
        StreamExecTemporalSort sort = new StreamExecTemporalSort(
                configuration,
                SortSpec.builder()
                        .addField(0, true, true)
                        .addField(1, false, false)
                        .build(),
                InputProperty.DEFAULT,
                rowType,
                "ORDER BY event_time, priority DESC");
        sort.setInputEdges(
                List.of(ExecEdge.builder().source(source).target(sort).build()));

        ExecNode<?> replacement = new StreamFusionExecGraphProcessor().convert(sort);

        assertThat(replacement).isInstanceOf(StreamFusionExecTemporalSort.class);
        assertThat(replacement.getInputEdges())
                .singleElement()
                .extracting(ExecEdge::getSource)
                .isEqualTo(source);
    }
}
