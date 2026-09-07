/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.operator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.CollectorOutput;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchSerializer;
import tech.streamfusion.flink.calc.StreamFusionCalcTranslator;

class NativeRegionMetricOwnershipTest {
    private static final RowType TYPE = RowType.of(new IntType(false));
    private static final long ROOT = (1L << 32) | 22;

    @Test
    void unaryRuntimeBoundaryAndRootStageCountTheirOwnRecordsIncludingSinkFailure() throws Exception {
        for (boolean fail : List.of(false, true)) {
            var operator = StreamFusionArrowNativeOperator.forRegion(TYPE, TYPE, plan(), "metric-test-region");
            var failure = new IllegalStateException("injected test sink failure");
            var output = new ArrayList<Integer>();
            try (var allocator = new RootAllocator(64L << 20);
                    var harness =
                            new OneInputStreamOperatorTestHarness<ArrowRowDataBatch, ArrowRowDataBatch>(operator)) {
                harness.setOutputCreator(ignored -> new CollectorOutput<ArrowRowDataBatch>(new ArrayList<>()) {
                    @Override
                    public void collect(StreamRecord<ArrowRowDataBatch> record) {
                        operator.getMetricGroup()
                                .getIOMetricGroup()
                                .getNumRecordsOutCounter()
                                .inc();
                        if (fail) throw failure;
                        for (int row = 0; row < record.getValue().size(); row++)
                            output.add(record.getValue().rowView(row).getInt(0));
                    }
                });
                harness.setup(ArrowRowDataBatchSerializer.INSTANCE);
                harness.open();
                try (var input = input(allocator)) {
                    operator.getMetricGroup()
                            .getIOMetricGroup()
                            .getNumRecordsInCounter()
                            .inc();
                    if (fail)
                        assertThatThrownBy(() -> harness.processElement(new StreamRecord<>(input)))
                                .isSameAs(failure);
                    else harness.processElement(new StreamRecord<>(input));
                }
                assertThat(operator.getMetricGroup()
                                .getIOMetricGroup()
                                .getNumRecordsInCounter()
                                .getCount())
                        .isEqualTo(12);
                assertThat(operator.getMetricGroup()
                                .getIOMetricGroup()
                                .getNumRecordsOutCounter()
                                .getCount())
                        .isEqualTo(5);
                var root = NativeRegionTestHarness.stageMetrics(operator, ROOT).getIOMetricGroup();
                assertThat(root.getNumRecordsInCounter().getCount()).isEqualTo(8);
                assertThat(root.getNumRecordsOutCounter().getCount()).isEqualTo(5);
                if (!fail) assertThat(output).containsExactly(4, 5, 6, 7, 8);
                else assertThat(failure.getSuppressed()).isEmpty();
            }
        }
    }

    @Test
    void multiInputRuntimePreservesStageAndBoundaryCountsWhenSinkThrows() throws Exception {
        var failure = new IllegalStateException("injected multi-input sink failure");
        try (var allocator = new RootAllocator(64L << 20);
                var harness = new NativeRegionTestHarness(plan(), List.of(TYPE), TYPE)) {
            harness.sinkFailure = failure;
            harness.open();
            try (var input = input(allocator)) {
                assertThatThrownBy(() -> harness.accept(0, input)).isSameAs(failure);
            }
            assertThat(harness.metrics()
                            .getIOMetricGroup()
                            .getNumRecordsInCounter()
                            .getCount())
                    .isEqualTo(12);
            assertThat(harness.metrics()
                            .getIOMetricGroup()
                            .getNumRecordsOutCounter()
                            .getCount())
                    .isEqualTo(5);
            assertThat(harness.stageMetrics(ROOT)
                            .getIOMetricGroup()
                            .getNumRecordsInCounter()
                            .getCount())
                    .isEqualTo(8);
            assertThat(harness.stageMetrics(ROOT)
                            .getIOMetricGroup()
                            .getNumRecordsOutCounter()
                            .getCount())
                    .isEqualTo(5);
            assertThat(failure.getSuppressed()).isEmpty();
        }
    }

    private static ArrowRowDataBatch input(RootAllocator allocator) {
        List<GenericRowData> rows = new ArrayList<>();
        for (int i = 0; i < 12; i++) rows.add(GenericRowData.of(i));
        return ArrowRowDataBatch.transpose(rows, TYPE, allocator);
    }

    private static byte[] plan() {
        var types =
                new FlinkTypeFactory(NativeRegionMetricOwnershipTest.class.getClassLoader(), RelDataTypeSystem.DEFAULT);
        var rex = new RexBuilder(types);
        var value = rex.makeInputRef(types.createFieldTypeFromLogicalType(TYPE.getTypeAt(0)), 0);
        return StreamFusionNativeRegionTranslator.compose(List.of(
                StreamFusionNativeRegionTranslator.identifyStage(
                        StreamFusionCalcTranslator.createStagePlan(
                                TYPE,
                                TYPE,
                                List.of(value),
                                rex.makeCall(
                                        SqlStdOperatorTable.GREATER_THAN,
                                        value,
                                        rex.makeExactLiteral(BigDecimal.valueOf(3)))),
                        11),
                StreamFusionNativeRegionTranslator.identifyStage(
                        StreamFusionCalcTranslator.createStagePlan(
                                TYPE,
                                TYPE,
                                List.of(value),
                                rex.makeCall(
                                        SqlStdOperatorTable.LESS_THAN,
                                        value,
                                        rex.makeExactLiteral(BigDecimal.valueOf(9)))),
                        22)));
    }
}
