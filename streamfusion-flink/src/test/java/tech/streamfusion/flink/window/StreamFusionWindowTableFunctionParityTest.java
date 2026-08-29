/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.window;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.operators.window.groupwindow.assigners.CumulativeWindowAssigner;
import org.apache.flink.table.runtime.operators.window.groupwindow.assigners.GroupWindowAssigner;
import org.apache.flink.table.runtime.operators.window.groupwindow.assigners.SlidingWindowAssigner;
import org.apache.flink.table.runtime.operators.window.groupwindow.assigners.TumblingWindowAssigner;
import org.apache.flink.table.runtime.operators.window.tvf.operator.AlignedWindowTableFunctionOperator;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import tech.streamfusion.proto.plan.v1.WindowKind;

class StreamFusionWindowTableFunctionParityTest {
    private static final RowType INPUT_TYPE = RowType.of(new IntType(false), new TimestampType(true, 3));
    private static final RowType OUTPUT_TYPE = RowType.of(
            new IntType(false),
            new TimestampType(true, 3),
            new TimestampType(false, 3),
            new TimestampType(false, 3),
            new TimestampType(false, 3));

    @Test
    void generatedInputsMatchFlinkForTumbleHopAndCumulate() throws Exception {
        List<Case> cases = List.of(
                new Case(
                        WindowKind.WINDOW_KIND_TUMBLE,
                        5_000,
                        0,
                        1_000,
                        TumblingWindowAssigner.of(Duration.ofMillis(5_000)).withOffset(Duration.ofMillis(1_000))),
                new Case(
                        WindowKind.WINDOW_KIND_HOP,
                        10_000,
                        4_000,
                        0,
                        SlidingWindowAssigner.of(Duration.ofMillis(10_000), Duration.ofMillis(4_000))),
                new Case(
                        WindowKind.WINDOW_KIND_CUMULATE,
                        10_000,
                        2_000,
                        0,
                        CumulativeWindowAssigner.of(Duration.ofMillis(10_000), Duration.ofMillis(2_000))));

        for (Case testCase : cases) {
            StreamFusionWindowTableFunctionOperator streamFusion = streamFusion(testCase);
            AlignedWindowTableFunctionOperator flink = flink(testCase.assigner);

            List<String> streamFusionOutput = run(streamFusion);
            List<String> flinkOutput = run(flink);

            assertThat(streamFusionOutput).as(testCase.kind.name()).containsExactlyElementsOf(flinkOutput);
            assertThat(streamFusion.getNumNullRowTimeRecordsDropped().getCount())
                    .as(testCase.kind.name() + " null-row metric")
                    .isEqualTo(flink.getNumNullRowTimeRecordsDropped().getCount())
                    .isEqualTo(1);
        }
    }

    private static StreamFusionWindowTableFunctionOperator streamFusion(Case testCase) {
        return new StreamFusionWindowTableFunctionOperator(
                INPUT_TYPE,
                OUTPUT_TYPE,
                1,
                new StreamFusionWindowTableFunctionTranslator.WindowParameters(
                        testCase.kind, testCase.size, testCase.slideOrStep, testCase.offset));
    }

    private static AlignedWindowTableFunctionOperator flink(GroupWindowAssigner<?> assigner) {
        @SuppressWarnings("unchecked")
        GroupWindowAssigner<org.apache.flink.table.runtime.operators.window.TimeWindow> timeAssigner =
                (GroupWindowAssigner<org.apache.flink.table.runtime.operators.window.TimeWindow>) assigner;
        return new AlignedWindowTableFunctionOperator(timeAssigner, 1, 3, ZoneOffset.UTC);
    }

    private static List<String> run(OneInputStreamOperator<RowData, RowData> operator) throws Exception {
        try (OneInputStreamOperatorTestHarness<RowData, RowData> harness =
                new OneInputStreamOperatorTestHarness<>(operator)) {
            harness.setup(new RowDataSerializer(OUTPUT_TYPE));
            harness.open();
            int id = 0;
            for (long timestamp : new long[] {-1, 0, 1, 3_999, 4_000, 9_000}) {
                GenericRowData row = GenericRowData.of(id++, TimestampData.fromEpochMillis(timestamp));
                row.setRowKind(id % 2 == 0 ? RowKind.UPDATE_AFTER : RowKind.INSERT);
                harness.processElement(new StreamRecord<>(row, timestamp + 100));
            }
            harness.processElement(new StreamRecord<>(GenericRowData.of(id, null)));
            harness.processWatermark(new Watermark(20_000));

            List<String> output = new ArrayList<>();
            for (Object event : harness.getOutput()) {
                if (event instanceof Watermark) {
                    output.add("watermark:" + ((Watermark) event).getTimestamp());
                    continue;
                }
                @SuppressWarnings("unchecked")
                StreamRecord<RowData> record = (StreamRecord<RowData>) event;
                RowData row = record.getValue();
                output.add(row.getRowKind().shortString()
                        + ":"
                        + row.getInt(0)
                        + ":"
                        + row.getTimestamp(2, 3).getMillisecond()
                        + ":"
                        + row.getTimestamp(3, 3).getMillisecond()
                        + ":"
                        + row.getTimestamp(4, 3).getMillisecond()
                        + ":record-ts="
                        + (record.hasTimestamp() ? record.getTimestamp() : "none"));
            }
            return output;
        }
    }

    private static final class Case {
        private final WindowKind kind;
        private final long size;
        private final long slideOrStep;
        private final long offset;
        private final GroupWindowAssigner<?> assigner;

        private Case(WindowKind kind, long size, long slideOrStep, long offset, GroupWindowAssigner<?> assigner) {
            this.kind = kind;
            this.size = size;
            this.slideOrStep = slideOrStep;
            this.offset = offset;
            this.assigner = assigner;
        }
    }
}
