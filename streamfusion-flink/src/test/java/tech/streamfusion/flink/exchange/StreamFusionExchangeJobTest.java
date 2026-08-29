/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.exchange;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.arrow.StreamFusionArrowBoundaries;

class StreamFusionExchangeJobTest {
    @Test
    void runsNativeHashExchangeThroughARealFlinkRunner() throws Exception {
        RowType rowType = RowType.of(new IntType(false));
        List<RowData> rows = new ArrayList<>();
        for (int value = -100; value < 100; value++) {
            rows.add(GenericRowData.of(value));
        }
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(2);
        DataStream<RowData> source = environment.fromCollection(rows).returns(InternalTypeInfo.of(rowType));
        Transformation<RowData> arrowExchange =
                StreamFusionExchangeTranslator.hash(source.getTransformation(), rowType, new int[] {0}, 128);
        Transformation<RowData> exchange = StreamFusionArrowBoundaries.toRowData(arrowExchange, rowType);

        List<RoutedValue> results = new DataStream<>(environment, exchange)
                .map(new CaptureSubtask())
                .executeAndCollect(200);

        assertThat(results).hasSize(200);
        for (RoutedValue result : results) {
            assertThat(result.subtask).isEqualTo(flinkSubtask(result.value, 128, 2));
        }
    }

    private static int flinkSubtask(int value, int maxParallelism, int parallelism) {
        BinaryRowData key = new BinaryRowData(1);
        BinaryRowWriter writer = new BinaryRowWriter(key);
        writer.reset();
        writer.writeInt(0, value);
        writer.complete();
        int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(key, maxParallelism);
        return KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(maxParallelism, parallelism, keyGroup);
    }

    private static final class CaptureSubtask extends RichMapFunction<RowData, RoutedValue> {
        private static final long serialVersionUID = 1L;

        private transient int subtask;

        @Override
        public void open(OpenContext openContext) {
            subtask = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
        }

        @Override
        public RoutedValue map(RowData row) {
            return new RoutedValue(row.getInt(0), subtask);
        }
    }

    private static final class RoutedValue implements java.io.Serializable {
        private static final long serialVersionUID = 1L;

        private final int value;
        private final int subtask;

        private RoutedValue(int value, int subtask) {
            this.value = value;
            this.subtask = subtask;
        }
    }
}
