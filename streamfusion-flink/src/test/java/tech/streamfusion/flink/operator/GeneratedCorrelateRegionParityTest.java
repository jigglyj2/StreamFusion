/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.operator;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import org.apache.arrow.memory.RootAllocator;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.SpecializedFunction;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.codegen.CalcCodeGenerator;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.ExpandCodeGenerator;
import org.apache.flink.table.runtime.functions.table.UnnestRowsFunction;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.TestingNativeMemoryManager;
import tech.streamfusion.flink.arrow.ArrowCDataBridge;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.NativeCalcResult;
import tech.streamfusion.flink.calc.StreamFusionCalcTranslator;
import tech.streamfusion.flink.expand.StreamFusionExpandTranslator;
import tech.streamfusion.flink.unnest.StreamFusionArrayUnnestTranslator;
import tech.streamfusion.nativebridge.NativeExecutionContext;

/** Mixed region parity against Flink's actual Calc/Expand codegen and collection UNNEST kernel. */
class GeneratedCorrelateRegionParityTest {
    private static final RowType INPUT = RowType.of(new IntType(false), new ArrayType(new IntType()));
    private static final RowType EXPANDED = RowType.of(INPUT.getTypeAt(0), INPUT.getTypeAt(1), new IntType());
    private static final RowType OUTPUT = RowType.of(new IntType(false), new IntType());
    private final FlinkTypeFactory types = new FlinkTypeFactory(getClass().getClassLoader(), RelDataTypeSystem.DEFAULT);
    private final RexBuilder rex = new RexBuilder(types);
    private final List<RexNode> inputProjection = refs(INPUT);
    private final List<RexNode> expandedProjection = refs(EXPANDED);
    private final List<RexNode> outputProjection = List.of(expandedProjection.get(0), expandedProjection.get(2));
    private final RexNode inputFilter = nonnegative(inputProjection.get(0));
    private final RexNode outputFilter = nonnegative(expandedProjection.get(2));

    @Test
    void calcUnnestExpandCalcHasByteExactChangelogAndStageCountsAcrossGeneratedBatches() throws Exception {
        var invocation = rex.makeCall(
                types.createFieldTypeFromLogicalType(new IntType()),
                new SqlFunction(
                        "$UNNEST_ROWS$1",
                        SqlKind.OTHER_FUNCTION,
                        null,
                        null,
                        null,
                        SqlFunctionCategory.USER_DEFINED_TABLE_FUNCTION),
                List.of(inputProjection.get(1)));
        List<byte[]> stages = List.of(
                StreamFusionCalcTranslator.createStagePlan(INPUT, INPUT, inputProjection, inputFilter),
                StreamFusionArrayUnnestTranslator.createStagePlan(INPUT, EXPANDED, FlinkJoinType.INNER, invocation),
                StreamFusionExpandTranslator.createStagePlan(
                        EXPANDED, EXPANDED, List.of(expandedProjection, expandedProjection)),
                StreamFusionCalcTranslator.createStagePlan(EXPANDED, OUTPUT, outputProjection, outputFilter));
        for (int seed = 0; seed < 3; seed++) {
            List<RowData> inputs = rows(seed);
            List<RowData> filtered = flinkCalc(inputs, INPUT, INPUT, inputProjection, inputFilter);
            List<RowData> unnested = flinkUnnest(filtered);
            List<RowData> expanded = flinkExpand(unnested);
            List<RowData> expected = flinkCalc(expanded, EXPANDED, OUTPUT, outputProjection, outputFilter);
            var manager = TestingNativeMemoryManager.create();
            long available = manager.available();
            try (RootAllocator allocator = new RootAllocator(128L << 20);
                    NativeExecutionContext context =
                            new NativeExecutionContext(StreamFusionNativeRegionTranslator.compose(stages), manager)) {
                var execution = new ArrowCDataBridge.ReusableExecution(context, OUTPUT, allocator);
                List<RowData> actual = new ArrayList<>();
                int batchSize = new int[] {1, 7, 64}[seed];
                for (int start = 0; start < inputs.size(); start += batchSize) {
                    List<RowData> rows = inputs.subList(start, Math.min(start + batchSize, inputs.size()));
                    RowKind[] kinds = rows.stream().map(RowData::getRowKind).toArray(RowKind[]::new);
                    try (ArrowRowDataBatch input = ArrowRowDataBatch.transpose(rows, INPUT, allocator)
                                    .withEnvelope(kinds, new boolean[rows.size()], new long[rows.size()]);
                            var stream = execution.executeStream(input)) {
                        NativeCalcResult result;
                        while ((result = stream.nextWithSelection()) != null) {
                            try (NativeCalcResult owned = result) {
                                ArrowRowDataBatch output = owned.selectEnvelopeFrom(input);
                                for (int row = 0; row < output.size(); row++) {
                                    RowData copy = new RowDataSerializer(OUTPUT).copy(output.rowView(row));
                                    copy.setRowKind(output.rowKind(row));
                                    actual.add(copy);
                                }
                            }
                        }
                    }
                }
                assertThat(bytes(actual)).as("seed %s", seed).containsExactly(bytes(expected));
                assertThat(context.metricSnapshot())
                        .containsExactly(
                                1,
                                expanded.size(),
                                expected.size(),
                                2,
                                unnested.size(),
                                expanded.size(),
                                3,
                                filtered.size(),
                                unnested.size(),
                                4,
                                inputs.size(),
                                filtered.size(),
                                5,
                                0,
                                inputs.size());
            }
            assertThat(manager.available()).isEqualTo(available);
        }
    }

    private List<RowData> flinkCalc(
            List<RowData> rows, RowType input, RowType output, List<RexNode> projections, RexNode filter)
            throws Exception {
        var factory = CalcCodeGenerator.generateCalcOperator(
                new CodeGeneratorContext(new Configuration(), getClass().getClassLoader()),
                input(input),
                output,
                scala.collection.JavaConverters.asScalaBufferConverter(projections)
                        .asScala()
                        .toSeq(),
                scala.Option.apply(filter),
                true,
                "MixedRegionCalc");
        try (var harness = new OneInputStreamOperatorTestHarness<RowData, RowData>(factory, 1, 1, 0)) {
            harness.setup(new RowDataSerializer(output));
            harness.open();
            for (RowData row : rows) {
                harness.processElement(new StreamRecord<>(row));
            }
            return new ArrayList<>(harness.extractOutputValues());
        }
    }

    private List<RowData> flinkExpand(List<RowData> rows) throws Exception {
        var factory = ExpandCodeGenerator.generateExpandOperator(
                new CodeGeneratorContext(new Configuration(), getClass().getClassLoader()),
                EXPANDED,
                EXPANDED,
                List.of(expandedProjection, expandedProjection),
                true,
                "MixedRegionExpand");
        try (var harness = new OneInputStreamOperatorTestHarness<RowData, RowData>(factory, 1, 1, 0)) {
            harness.setup(new RowDataSerializer(EXPANDED));
            harness.open();
            for (RowData row : rows) {
                harness.processElement(new StreamRecord<>(row));
            }
            return new ArrayList<>(harness.extractOutputValues());
        }
    }

    private List<RowData> flinkUnnest(List<RowData> rows) {
        ClassLoader loader = getClass().getClassLoader();
        CallContext call = (CallContext)
                Proxy.newProxyInstance(loader, new Class<?>[] {CallContext.class}, (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "getArgumentDataTypes":
                            return List.of(DataTypes.ARRAY(DataTypes.INT()));
                        case "getOutputDataType":
                            return Optional.of(DataTypes.INT());
                        default:
                            throw new UnsupportedOperationException(method.getName());
                    }
                });
        var context = (SpecializedFunction.SpecializedContext) Proxy.newProxyInstance(
                loader, new Class<?>[] {SpecializedFunction.SpecializedContext.class}, (proxy, method, args) -> {
                    if (method.getName().equals("getCallContext")) {
                        return call;
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
        var function = new UnnestRowsFunction.CollectionUnnestFunction(
                context, new IntType(), ArrayData.createElementGetter(new IntType()));
        List<RowData> result = new ArrayList<>();
        for (RowData input : rows) {
            function.setCollector(new Collector<>() {
                @Override
                public void collect(Object element) {
                    GenericRowData row =
                            GenericRowData.of(input.getInt(0), input.isNullAt(1) ? null : input.getArray(1), element);
                    row.setRowKind(input.getRowKind());
                    result.add(new RowDataSerializer(EXPANDED).copy(row));
                }

                @Override
                public void close() {}
            });
            function.eval(input.isNullAt(1) ? (ArrayData) null : input.getArray(1));
        }
        return result;
    }

    private static Transformation<RowData> input(RowType type) {
        return new Transformation<RowData>("source", InternalTypeInfo.of(type), 1) {
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

    private List<RexNode> refs(RowType type) {
        List<RexNode> refs = new ArrayList<>();
        for (int index = 0; index < type.getFieldCount(); index++) {
            refs.add(rex.makeInputRef(types.createFieldTypeFromLogicalType(type.getTypeAt(index)), index));
        }
        return refs;
    }

    private RexNode nonnegative(RexNode value) {
        return rex.makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, value, rex.makeExactLiteral(BigDecimal.ZERO));
    }

    private static List<RowData> rows(int seed) {
        Random random = new Random(32011 + seed);
        List<RowData> rows = new ArrayList<>();
        for (int index = 0; index < 131; index++) {
            Integer[] array = new Integer[random.nextInt(5)];
            for (int item = 0; item < array.length; item++) {
                array[item] = random.nextInt(3) == 0 ? null : random.nextInt(8) - 3;
            }
            GenericRowData row =
                    GenericRowData.of(random.nextInt(6) - 2, index % 11 == 0 ? null : new GenericArrayData(array));
            row.setRowKind(RowKind.values()[index % 4]);
            rows.add(row);
        }
        return rows;
    }

    private static byte[] bytes(List<RowData> rows) throws Exception {
        DataOutputSerializer output = new DataOutputSerializer(256);
        RowDataSerializer serializer = new RowDataSerializer(OUTPUT);
        for (RowData row : rows) {
            serializer.serialize(row, output);
        }
        return output.getCopyOfBuffer();
    }
}
