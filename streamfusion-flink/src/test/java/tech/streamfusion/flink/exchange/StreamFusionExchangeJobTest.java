/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.exchange;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

class StreamFusionExchangeJobTest {
    @Test
    void runsNativeHashExchangeThroughARealFlinkRunner() throws Exception {
        RowType rowType = RowType.of(new IntType(false));
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(2);
        DataStream<RowData> source = environment
                .fromData(
                        (RowData) GenericRowData.of(-1),
                        GenericRowData.of(0),
                        GenericRowData.of(1),
                        GenericRowData.of(42))
                .returns(InternalTypeInfo.of(rowType));
        Transformation<RowData> exchange =
                StreamFusionExchangeTranslator.hash(source.getTransformation(), rowType, new int[] {0}, 128);

        List<Integer> results = new DataStream<>(environment, exchange)
                .executeAndCollect(4).stream()
                        .map(row -> row.getInt(0))
                        .sorted()
                        .collect(Collectors.toList());

        assertThat(results).containsExactly(-1, 0, 1, 42);
    }
}
