/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.arrow.memory.RootAllocator;
import org.apache.calcite.rex.RexBuilder;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.arrow.ArrowNativePlanDispatcher;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.calc.StreamFusionCalcTranslator;
import tech.streamfusion.nativebridge.NativeExecutionContext;
import tech.streamfusion.proto.plan.v1.NativePlan;

/** Complete ordered changelog comparison for the SQL-generated physical PROCTIME placeholder Calc. */
class ProcessingTimeAttributeParityTest {
    @Test
    @SuppressWarnings("unchecked")
    void datafusionNullPlaceholderMatchesFlinkWithoutReadingAClock() throws Exception {
        String sql = "WITH B AS (SELECT k, PROCTIME() AS pt FROM local_window_input) "
                + "SELECT k, COUNT(*), window_start, window_end FROM TABLE("
                + "TUMBLE(TABLE B, DESCRIPTOR(pt), INTERVAL '10' SECOND)) GROUP BY k, window_start, window_end";
        for (int seed : List.of(3, 19, 71)) {
            var stage = SlicingWindowFlinkPlan.stage("Calc", sql);
            var inputType = ((InternalTypeInfo<RowData>) stage.getInputType()).toRowType();
            var outputType = ((InternalTypeInfo<RowData>) stage.getOutputType()).toRowType();
            var types = new FlinkTypeFactory(getClass().getClassLoader(), FlinkTypeSystem.INSTANCE);
            var rex = new RexBuilder(types);
            var projections = List.of(
                    rex.makeInputRef(types.createFieldTypeFromLogicalType(inputType.getTypeAt(0)), 0),
                    rex.makeCall(FlinkSqlOperatorTable.PROCTIME));
            assertThat(StreamFusionCalcTranslator.unsupportedReason(inputType, outputType, projections, null))
                    .isNull();
            var plan = NativePlan.parseFrom(
                    StreamFusionCalcTranslator.createStagePlan(inputType, outputType, projections, null));
            assertThat(plan.getRoot().getCalc().getProjections(1).hasNullLiteral())
                    .isTrue();
            var root = plan.getRoot().toBuilder().setPlanNodeId(2);
            root.setCalc(root.getCalc().toBuilder()
                    .setInput(root.getCalc().getInput().toBuilder().setPlanNodeId(1)));
            var memory = new SharedAggregateRegionParityTest.Memory();
            try (var flink = new OneInputStreamOperatorTestHarness<RowData, RowData>(
                            (StreamOperatorFactory<RowData>) stage.getOperatorFactory(), 1, 1, 0);
                    var allocator = new RootAllocator(64L << 20);
                    var context = new NativeExecutionContext(
                            plan.toBuilder().setRoot(root).build().toByteArray(), memory);
                    var nativePlan =
                            new ArrowNativePlanDispatcher(context, List.of(inputType), outputType, allocator)) {
                var inputSerializer = new RowDataSerializer(inputType);
                var outputSerializer = new RowDataSerializer(outputType);
                flink.setup(outputSerializer);
                flink.open();
                var random = new Random(seed);
                for (int size : List.of(0, 1, 7, 257)) {
                    var rows = new ArrayList<RowData>();
                    var kinds = new RowKind[size];
                    var present = new boolean[size];
                    var times = new long[size];
                    for (int i = 0; i < size; i++) {
                        var row = new GenericRowData(inputType.getFieldCount());
                        row.setField(0, i % 7 == 0 ? null : random.nextLong());
                        row.setRowKind(RowKind.values()[i % 4]);
                        rows.add(row);
                        kinds[i] = row.getRowKind();
                        present[i] = true;
                        times[i] = i;
                        flink.setProcessingTime(i + 10000L * size);
                        flink.processElement(new StreamRecord<>(inputSerializer.toBinaryRow(row), i));
                    }
                    var expected = new DataOutputSerializer(128);
                    for (var record : flink.extractOutputStreamRecords()) {
                        assertThat(record.hasTimestamp()).isFalse();
                        assertThat(record.getValue().isNullAt(1)).isTrue();
                        outputSerializer.serialize(record.getValue(), expected);
                    }
                    flink.getOutput().clear();
                    var actual = new DataOutputSerializer(128);
                    try (var batch = ArrowRowDataBatch.transpose(rows, inputType, allocator)
                            .withEnvelope(kinds, present, times)) {
                        nativePlan.process(0, batch, output -> {
                            try {
                                for (int i = 0; i < output.size(); i++) {
                                    assertThat(output.hasTimestamp(i)).isFalse();
                                    var row = output.rowView(i);
                                    row.setRowKind(output.rowKind(i));
                                    outputSerializer.serialize(row, actual);
                                }
                            } catch (java.io.IOException failure) {
                                throw new RuntimeException(failure);
                            }
                        });
                    }
                    assertThat(actual.getCopyOfBuffer()).containsExactly(expected.getCopyOfBuffer());
                }
            }
            assertThat(memory.available()).isEqualTo(memory.limit());
        }
    }
}
