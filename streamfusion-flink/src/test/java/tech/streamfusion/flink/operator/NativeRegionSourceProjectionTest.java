/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.operator;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.CollectorOutput;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.planner.codegen.CalcCodeGenerator;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchSerializer;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchTypeInfo;
import tech.streamfusion.flink.calc.StreamFusionCalcTranslator;
import tech.streamfusion.proto.plan.v1.NativePlan;

/** Exercises the production projected source boundary and common native owner against Flink codegen. */
class NativeRegionSourceProjectionTest {
    private static final RowType NESTED = RowType.of(
            new org.apache.flink.table.types.logical.LogicalType[] {
                new IntType(false), new VarCharType(), new VarCharType()
            },
            new String[] {"key", "label", "unused"});
    private static final RowType INPUT = RowType.of(new VarCharType(), NESTED, new VarCharType());
    private static final RowType OUTPUT = RowType.of(new VarCharType(), new IntType());

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    @SuppressWarnings("unchecked")
    void projectedNullableNestedInputsMatchGeneratedFlinkChangelog(boolean constants) throws Exception {
        var types = new FlinkTypeFactory(getClass().getClassLoader(), FlinkTypeSystem.INSTANCE);
        var rex = new RexBuilder(types);
        RexNode nested = rex.makeInputRef(types.createFieldTypeFromLogicalType(NESTED), 1);
        RexNode key = rex.makeFieldAccess(nested, "key", true);
        RexNode label = rex.makeFieldAccess(nested, "label", true);
        List<RexNode> projections = constants
                ? List.of(rex.makeLiteral("constant"), rex.makeExactLiteral(java.math.BigDecimal.ONE))
                : List.of(label, key);
        RexNode condition = constants ? null : rex.makeCall(SqlStdOperatorTable.IS_NOT_NULL, label);
        byte[] stage = StreamFusionNativeRegionTranslator.identifyStage(
                StreamFusionCalcTranslator.createStagePlan(INPUT, OUTPUT, projections, condition),
                27,
                "Calc[27]",
                "27_calc");
        var source = source();
        var owner = (OneInputTransformation<ArrowRowDataBatch, ArrowRowDataBatch>)
                (Transformation<?>) StreamFusionNativeRegionTranslator.translate(source, INPUT, OUTPUT, List.of(stage));
        var boundary = (OneInputTransformation<RowData, ArrowRowDataBatch>)
                owner.getInputs().get(0);
        assertThat(owner.getInputType()).isSameAs(ArrowRowDataBatchTypeInfo.INSTANCE);
        assertThat(owner.getOutputType()).isSameAs(ArrowRowDataBatchTypeInfo.INSTANCE);
        assertThat(boundary.getInputs()).containsExactly(source);
        assertThat(owner.getTransitivePredecessors()).hasSize(3);
        var field = StreamFusionArrowNativeOperator.class.getDeclaredField("serializedPlan");
        field.setAccessible(true);
        var rewritten =
                NativePlan.parseFrom((byte[]) field.get(owner.getOperator())).getRoot();
        var original = NativePlan.parseFrom(stage).getRoot();
        assertThat(rewritten.toBuilder().clearCalc().build())
                .isEqualTo(original.toBuilder().clearCalc().build());
        assertThat(rewritten.getCalc().getInput()).isEqualTo(original.getCalc().getInput());

        var factory = CalcCodeGenerator.generateCalcOperator(
                new CodeGeneratorContext(new Configuration(), getClass().getClassLoader()),
                source,
                OUTPUT,
                scala.collection.JavaConverters.asScalaBufferConverter(projections)
                        .asScala()
                        .toSeq(),
                scala.Option.apply(condition),
                true,
                "ProjectedSourceParity");
        var expected = new ArrayList<RowData>();
        var actual = new ArrayList<RowData>();
        var serializer = new RowDataSerializer(OUTPUT);
        try (var flink = new OneInputStreamOperatorTestHarness<RowData, RowData>(factory, 1, 1, 0);
                var nativeOwner = new OneInputStreamOperatorTestHarness<ArrowRowDataBatch, ArrowRowDataBatch>(
                        owner.getOperatorFactory(), 1, 1, 0);
                var input = new OneInputStreamOperatorTestHarness<RowData, ArrowRowDataBatch>(
                        boundary.getOperatorFactory(), 1, 1, 0)) {
            nativeOwner.setOutputCreator(ignored -> new CollectorOutput<ArrowRowDataBatch>(new ArrayList<>()) {
                @Override
                public void collect(StreamRecord<ArrowRowDataBatch> record) {
                    var batch = record.getValue();
                    for (int index = 0; index < batch.size(); index++) {
                        RowData row = serializer.copy(batch.rowView(index));
                        row.setRowKind(batch.rowKind(index));
                        actual.add(row);
                        assertThat(batch.hasTimestamp(index)).isFalse();
                    }
                }
            });
            input.setOutputCreator(ignored -> new CollectorOutput<ArrowRowDataBatch>(new ArrayList<>()) {
                @Override
                public void collect(StreamRecord<ArrowRowDataBatch> record) {
                    assertThat(record.getValue().root().getFieldVectors()).hasSize(constants ? 1 : 2);
                    try {
                        nativeOwner.processElement(record);
                    } catch (Exception failure) {
                        throw new RuntimeException(failure);
                    }
                }
            });
            flink.setup(serializer);
            nativeOwner.setup(ArrowRowDataBatchSerializer.INSTANCE);
            input.setup(ArrowRowDataBatchSerializer.INSTANCE);
            flink.open();
            nativeOwner.open();
            input.open();
            var random = new Random(68157);
            for (int index = 0; index < 513; index++) {
                GenericRowData row = GenericRowData.of(
                        StringData.fromString("unused source " + random.nextLong()),
                        index % 7 == 0
                                ? null
                                : GenericRowData.of(
                                        random.nextInt(),
                                        index % 5 == 0 ? null : StringData.fromString("é-" + random.nextInt(100)),
                                        StringData.fromString("unused nested " + random.nextLong())),
                        StringData.fromString("unused tail"));
                row.setRowKind(RowKind.values()[index % 4]);
                flink.processElement(new StreamRecord<>(row, index));
                input.processElement(new StreamRecord<>(row, index));
                if (index % 31 == 0) input.processWatermark(new Watermark(index));
            }
            input.processWatermark(Watermark.MAX_WATERMARK);
            expected.addAll(flink.extractOutputValues());
            var metrics = NativeRegionTestHarness.stageMetrics(nativeOwner.getOperator(), (1L << 32) | 27);
            assertThat(metrics.getIOMetricGroup().getNumRecordsInCounter().getCount())
                    .isEqualTo(513);
            assertThat(metrics.getIOMetricGroup().getNumRecordsOutCounter().getCount())
                    .isEqualTo(expected.size());
        }
        assertThat(bytes(actual)).containsExactly(bytes(expected));
    }

    private static byte[] bytes(List<RowData> rows) throws Exception {
        var bytes = new DataOutputSerializer(1024);
        var serializer = new RowDataSerializer(OUTPUT);
        for (RowData row : rows) serializer.serialize(row, bytes);
        return bytes.getCopyOfBuffer();
    }

    private static Transformation<RowData> source() {
        return new Transformation<RowData>("source", InternalTypeInfo.of(INPUT), 1) {
            @Override
            protected List<Transformation<?>> getTransitivePredecessorsInternal() {
                return List.of(this);
            }

            @Override
            public List<Transformation<?>> getInputs() {
                return List.of();
            }
        };
    }
}
