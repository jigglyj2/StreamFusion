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
import org.apache.arrow.memory.RootAllocator;
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
import tech.streamfusion.flink.arrow.ArrowCDataBridge;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.NativeCalcResult;
import tech.streamfusion.flink.proto.FlinkLogicalTypeProto;
import tech.streamfusion.nativebridge.NativeExecutionContext;
import tech.streamfusion.nativebridge.NativeMemoryManager;
import tech.streamfusion.proto.plan.v1.Calc;
import tech.streamfusion.proto.plan.v1.Expression;
import tech.streamfusion.proto.plan.v1.InputReference;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;
import tech.streamfusion.proto.plan.v1.WindowKind;

class StreamFusionWindowTableFunctionParityTest {
    private static final long[] TIMES = {-1, 0, 1, 3_999, 4_000, 9_000};
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
            assertThat(runStreamFusion(testCase))
                    .as(testCase.kind.name())
                    .containsExactlyElementsOf(runFlink(testCase.assigner));
        }
    }

    @Test
    void attributesNativeMetricsToTheMatchingProtobufPlanNode() throws Exception {
        byte[] windowBytes = StreamFusionWindowTableFunctionPlan.create(
                INPUT_TYPE,
                1,
                new int[0],
                false,
                "UTC",
                new StreamFusionWindowTableFunctionTranslator.WindowParameters(
                        WindowKind.WINDOW_KIND_TUMBLE, 5_000, 0, 0));
        NativePlan windowPlan = NativePlan.parseFrom(windowBytes);
        Operator window = windowPlan.getRoot().toBuilder()
                .setPlanNodeId(12)
                .setWindowTableFunction(windowPlan.getRoot().getWindowTableFunction().toBuilder()
                        .setInput(windowPlan.getRoot().getWindowTableFunction().getInput().toBuilder()
                                .setPlanNodeId(13)))
                .build();
        Calc.Builder calc = Calc.newBuilder().setInput(window);
        for (int index = 0; index <= OUTPUT_TYPE.getFieldCount(); index++) {
            calc.addProjections(Expression.newBuilder()
                    .setInputReference(InputReference.newBuilder()
                            .setIndex(index)
                            .setType(FlinkLogicalTypeProto.serialize(
                                    index == OUTPUT_TYPE.getFieldCount()
                                            ? new IntType(false)
                                            : OUTPUT_TYPE.getTypeAt(index)))));
        }
        byte[] plan = windowPlan.toBuilder()
                .setRoot(Operator.newBuilder().setPlanNodeId(11).setCalc(calc))
                .build()
                .toByteArray();
        NativeMemoryManager memoryManager = tech.streamfusion.flink.TestingNativeMemoryManager.create();
        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                NativeExecutionContext context = new NativeExecutionContext(plan, memoryManager);
                ArrowRowDataBatch input =
                        ArrowRowDataBatch.transpose(List.of(GenericRowData.of(1, null)), INPUT_TYPE, allocator);
                NativeCalcResult ignored = new ArrowCDataBridge.ReusableExecution(context, OUTPUT_TYPE, allocator)
                        .executeWithSelection(input)) {
            assertThat(context.metricValue(11, "numNullRowTimeRecordsDropped")).isZero();
            assertThat(context.metricValue(12, "numNullRowTimeRecordsDropped")).isEqualTo(1);
            assertThat(context.metricValue(13, "numNullRowTimeRecordsDropped")).isZero();
        }
    }

    private static List<String> runStreamFusion(Case testCase) {
        List<RowData> rows = inputs();
        RowKind[] kinds = new RowKind[rows.size()];
        boolean[] hasTimestamps = new boolean[rows.size()];
        long[] timestamps = new long[rows.size()];
        for (int row = 0; row < rows.size(); row++) {
            kinds[row] = rows.get(row).getRowKind();
            hasTimestamps[row] = row < TIMES.length;
            timestamps[row] = hasTimestamps[row] ? TIMES[row] + 100 : 0;
        }
        byte[] plan = StreamFusionWindowTableFunctionPlan.create(
                INPUT_TYPE,
                1,
                new int[0],
                false,
                "UTC",
                new StreamFusionWindowTableFunctionTranslator.WindowParameters(
                        testCase.kind, testCase.size, testCase.slideOrStep, testCase.offset));
        NativeMemoryManager memoryManager = tech.streamfusion.flink.TestingNativeMemoryManager.create();
        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                NativeExecutionContext context = new NativeExecutionContext(plan, memoryManager);
                ArrowRowDataBatch input = ArrowRowDataBatch.transpose(rows, INPUT_TYPE, allocator)
                        .withEnvelope(kinds, hasTimestamps, timestamps);
                NativeCalcResult result = new ArrowCDataBridge.ReusableExecution(context, OUTPUT_TYPE, allocator)
                        .executeWithSelection(input)) {
            ArrowRowDataBatch output = result.selectEnvelopeFrom(input).withoutTimestamps();
            List<String> formatted = format(output);
            formatted.add("watermark:20000");
            assertThat(input.root().getVector(1).getNullCount()).isEqualTo(1);
            assertThat(context.metricValue("numNullRowTimeRecordsDropped")).isEqualTo(1);
            return formatted;
        }
    }

    private static List<String> runFlink(GroupWindowAssigner<?> assigner) throws Exception {
        @SuppressWarnings("unchecked")
        GroupWindowAssigner<org.apache.flink.table.runtime.operators.window.TimeWindow> timeAssigner =
                (GroupWindowAssigner<org.apache.flink.table.runtime.operators.window.TimeWindow>) assigner;
        AlignedWindowTableFunctionOperator operator =
                new AlignedWindowTableFunctionOperator(timeAssigner, 1, 3, ZoneOffset.UTC);
        try (OneInputStreamOperatorTestHarness<RowData, RowData> harness =
                new OneInputStreamOperatorTestHarness<>(operator)) {
            harness.setup(new RowDataSerializer(OUTPUT_TYPE));
            harness.open();
            List<RowData> rows = inputs();
            for (int row = 0; row < TIMES.length; row++) {
                harness.processElement(new StreamRecord<>(rows.get(row), TIMES[row] + 100));
            }
            harness.processElement(new StreamRecord<>(rows.get(rows.size() - 1)));
            harness.processWatermark(new Watermark(20_000));
            List<String> output = new ArrayList<>();
            for (Object event : harness.getOutput()) {
                if (event instanceof Watermark) {
                    output.add("watermark:" + ((Watermark) event).getTimestamp());
                } else {
                    @SuppressWarnings("unchecked")
                    StreamRecord<RowData> record = (StreamRecord<RowData>) event;
                    output.add(format(record.getValue(), record.hasTimestamp(), record.getTimestamp()));
                }
            }
            assertThat(operator.getNumNullRowTimeRecordsDropped().getCount()).isEqualTo(1);
            return output;
        }
    }

    private static List<RowData> inputs() {
        List<RowData> rows = new ArrayList<>();
        int id = 0;
        for (long timestamp : TIMES) {
            GenericRowData row = GenericRowData.of(id++, TimestampData.fromEpochMillis(timestamp));
            row.setRowKind(id % 2 == 0 ? RowKind.UPDATE_AFTER : RowKind.INSERT);
            rows.add(row);
        }
        rows.add(GenericRowData.of(id, null));
        return rows;
    }

    private static List<String> format(ArrowRowDataBatch batch) {
        List<String> output = new ArrayList<>();
        for (int row = 0; row < batch.size(); row++) {
            RowData view = batch.rowView(row);
            view.setRowKind(batch.rowKind(row));
            output.add(format(view, batch.hasTimestamp(row), batch.timestamp(row)));
        }
        return output;
    }

    private static String format(RowData row, boolean hasTimestamp, long timestamp) {
        return row.getRowKind().shortString()
                + ":"
                + row.getInt(0)
                + ":"
                + row.getTimestamp(2, 3).getMillisecond()
                + ":"
                + row.getTimestamp(3, 3).getMillisecond()
                + ":"
                + row.getTimestamp(4, 3).getMillisecond()
                + ":record-ts="
                + (hasTimestamp ? timestamp : "none");
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
