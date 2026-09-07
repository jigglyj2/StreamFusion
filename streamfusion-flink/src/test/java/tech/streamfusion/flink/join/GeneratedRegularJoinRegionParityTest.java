/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.join;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.codegen.CalcCodeGenerator;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.stream.StreamingJoinOperator;
import org.apache.flink.table.runtime.operators.join.stream.utils.JoinInputSideSpec;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import tech.streamfusion.flink.TestingNativeMemoryManager;
import tech.streamfusion.flink.arrow.ArrowRegularJoinOutputStream;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.calc.StreamFusionCalcTranslator;
import tech.streamfusion.flink.exchange.ArrowExchangeInputBatch;
import tech.streamfusion.flink.operator.StreamFusionNativeRegionTranslator;
import tech.streamfusion.nativebridge.NativeRegularJoinBridge;

/** Real Flink join and generated Calc operators are the oracle, including the complete changelog. */
class GeneratedRegularJoinRegionParityTest {
    private static final RowType INPUT = RowType.of(new BigIntType(false), new VarCharType());
    private static final RowType OUTPUT =
            RowType.of(new BigIntType(), new VarCharType(), new BigIntType(), new VarCharType());
    private final FlinkTypeFactory types = new FlinkTypeFactory(getClass().getClassLoader(), RelDataTypeSystem.DEFAULT);
    private final RexBuilder rex = new RexBuilder(types);
    private final List<RexNode> identity = List.of(ref(0), ref(1), ref(2), ref(3));
    private final List<RexNode> reorder = List.of(ref(2), ref(3), ref(0), ref(1));
    private final RexNode condition =
            rex.makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, ref(0), rex.makeExactLiteral(BigDecimal.ZERO));

    @Test
    void sharedContextBindingsCarryJoinRetractionsAndNativeStageCounts(@TempDir Path temporary) throws Exception {
        for (boolean outer : List.of(false, true)) {
            for (int seed = 1; seed <= 2; seed++) {
                List<Event> events = events(seed);
                List<List<RowData>> joined = flinkJoin(events, outer);
                List<List<RowData>> expected = flinkCalc(joined, reorder, null);
                byte[] plan = StreamFusionNativeRegionTranslator.composeAbove(
                        StreamFusionNativeRegionTranslator.identifyStage(
                                StreamFusionRegularJoinPlan.create(
                                        INPUT,
                                        INPUT,
                                        new int[] {0},
                                        new int[] {0},
                                        new boolean[] {true},
                                        outer ? FlinkJoinType.FULL : FlinkJoinType.INNER,
                                        null),
                                2),
                        List.of(StreamFusionNativeRegionTranslator.identifyStage(
                                StreamFusionCalcTranslator.createStagePlan(OUTPUT, OUTPUT, reorder, null), 1)));
                long stateId = tech.streamfusion.proto.plan.v1.NativePlan.parseFrom(plan)
                        .getRoot()
                        .getCalc()
                        .getInput()
                        .getPlanNodeId();
                for (boolean rocks : List.of(false, true)) {
                    var memory = TestingNativeMemoryManager.create();
                    var binding = rocks
                            ? tech.streamfusion.nativebridge.NativeStateResources.rocksDb(
                                    stateId, 128, 0, 127, temporary.resolve("shared-" + outer + "-" + seed), 32L << 20)
                            : tech.streamfusion.nativebridge.NativeStateResources.memory(stateId, 128, 0, 127);
                    try (var allocator = new RootAllocator(128L << 20);
                            var context = new tech.streamfusion.nativebridge.NativeExecutionContext(
                                    plan,
                                    memory,
                                    tech.streamfusion.nativebridge.NativeStateResources.serialize(List.of(binding)));
                            var empty = ArrowRowDataBatch.empty(INPUT, allocator)) {
                        var edge = new tech.streamfusion.flink.arrow.ArrowNativePlanBridge(context, OUTPUT, allocator);
                        for (int index = 0; index < events.size(); index++) {
                            var event = events.get(index);
                            List<byte[]> actual = new ArrayList<>();
                            try (var input = ArrowRowDataBatch.transpose(event.rows, INPUT, allocator)
                                            .withRowKinds(event.rows.stream()
                                                    .map(RowData::getRowKind)
                                                    .toArray(RowKind[]::new));
                                    var stream = edge.executeStream(
                                            event.side == 0 ? List.of(input, empty) : List.of(empty, input))) {
                                ArrowRowDataBatch next;
                                while ((next = stream.next()) != null) {
                                    try (var output = next) {
                                        assertThat(output.size()).isLessThanOrEqualTo(4096);
                                        for (int row = 0; row < output.size(); row++) {
                                            var value = output.rowView(row);
                                            value.setRowKind(output.rowKind(row));
                                            actual.add(rowBytes(value));
                                        }
                                    }
                                }
                            }
                            assertThat(canonical(actual))
                                    .as("shared outer=%s seed=%s rocks=%s event=%s", outer, seed, rocks, index)
                                    .isEqualTo(bytes(expected.get(index)));
                        }
                        long left = events.stream()
                                .filter(e -> e.side == 0)
                                .mapToLong(e -> e.rows.size())
                                .sum();
                        long right = events.stream()
                                .filter(e -> e.side == 1)
                                .mapToLong(e -> e.rows.size())
                                .sum();
                        assertThat(context.metricSnapshot())
                                .containsExactly(
                                        (1L << 32) + 1,
                                        count(joined),
                                        count(expected),
                                        stateId,
                                        left + right,
                                        count(joined),
                                        1,
                                        0,
                                        left,
                                        2,
                                        0,
                                        right);
                    }
                    assertThat(memory.available()).isEqualTo(memory.limit());
                }
            }
        }
    }

    @Test
    void generatedJoinExpandCalcCalcChangelogAndStageCountsMatchFlinkOnBothNativeBackends(@TempDir Path temporary)
            throws Exception {
        for (boolean outer : List.of(false, true)) {
            for (int seed = 0; seed < 3; seed++) {
                List<Event> events = events(seed);
                List<List<RowData>> joined = flinkJoin(events, outer);
                List<List<RowData>> expanded = flinkExpand(joined);
                List<List<RowData>> filtered = flinkCalc(expanded, identity, condition);
                List<List<RowData>> expected = flinkCalc(filtered, reorder, null);
                byte[] joinPlan = StreamFusionRegularJoinPlan.create(
                        INPUT,
                        INPUT,
                        new int[] {0},
                        new int[] {0},
                        new boolean[] {true},
                        outer ? FlinkJoinType.FULL : FlinkJoinType.INNER,
                        null);
                byte[] plan = StreamFusionNativeRegionTranslator.composeAbove(
                        joinPlan,
                        List.of(
                                tech.streamfusion.flink.expand.StreamFusionExpandTranslator.createStagePlan(
                                        OUTPUT, OUTPUT, List.of(new ArrayList<>(identity), new ArrayList<>(identity))),
                                StreamFusionCalcTranslator.createStagePlan(OUTPUT, OUTPUT, identity, condition),
                                StreamFusionCalcTranslator.createStagePlan(OUTPUT, OUTPUT, reorder, null)));
                assertThat(plan).isNotNull();
                for (boolean rocks : List.of(false, true)) {
                    var memory = TestingNativeMemoryManager.create();
                    long handle = rocks
                            ? NativeRegularJoinBridge.createRocksDb(
                                    plan,
                                    128,
                                    0,
                                    127,
                                    temporary.resolve("join-" + outer + "-" + seed),
                                    32L << 20,
                                    memory)
                            : NativeRegularJoinBridge.create(plan, 128, 0, 127, memory);
                    try (RootAllocator allocator = new RootAllocator(128L << 20)) {
                        for (int eventIndex = 0; eventIndex < events.size(); eventIndex++) {
                            Event event = events.get(eventIndex);
                            List<byte[]> actual = new ArrayList<>();
                            try (ArrowExchangeInputBatch input = input(allocator, event.rows);
                                    ArrowRegularJoinOutputStream stream = new ArrowRegularJoinOutputStream(
                                            handle, event.side, input, null, OUTPUT, allocator, memory)) {
                                while (true) {
                                    try (ArrowRowDataBatch output = stream.next()) {
                                        if (output == null) {
                                            break;
                                        }
                                        for (int index = 0; index < output.size(); index++) {
                                            RowData row = output.rowView(index);
                                            row.setRowKind(output.rowKind(index));
                                            actual.add(rowBytes(row));
                                        }
                                    }
                                }
                            }
                            // MapState iteration order is backend-specific. Compare the complete
                            // serialized changelog multiset per input batch, never a final table or
                            // a sum that could hide a missing UPDATE_BEFORE/DELETE or duplicate.
                            assertThat(canonical(actual))
                                    .as("outer=%s seed=%s rocks=%s event=%s", outer, seed, rocks, eventIndex)
                                    .isEqualTo(bytes(expected.get(eventIndex)));
                        }
                        long left = events.stream()
                                .filter(event -> event.side == 0)
                                .mapToLong(event -> event.rows.size())
                                .sum();
                        long right = events.stream()
                                .filter(event -> event.side == 1)
                                .mapToLong(event -> event.rows.size())
                                .sum();
                        assertThat(NativeRegularJoinBridge.metricSnapshot(handle))
                                .containsExactly(
                                        1,
                                        count(filtered),
                                        count(expected),
                                        2,
                                        count(expanded),
                                        count(filtered),
                                        3,
                                        count(joined),
                                        count(expanded),
                                        4,
                                        left + right,
                                        count(joined),
                                        5,
                                        0,
                                        left,
                                        6,
                                        0,
                                        right);
                        // A Calc invocation still counts when it filters its whole input away.
                        long calcInputs =
                                joined.stream().filter(rows -> !rows.isEmpty()).count()
                                        + filtered.stream()
                                                .filter(rows -> !rows.isEmpty())
                                                .count();
                        assertThat(NativeRegularJoinBridge.statistics(handle)[2])
                                .isEqualTo(calcInputs);
                    } finally {
                        NativeRegularJoinBridge.destroy(handle);
                    }
                    assertThat(memory.available()).isEqualTo(memory.limit());
                }
            }
        }
    }

    private List<List<RowData>> flinkJoin(List<Event> events, boolean outer) throws Exception {
        String code =
                "public class RegionJoinCondition extends org.apache.flink.api.common.functions.AbstractRichFunction "
                        + "implements org.apache.flink.table.runtime.generated.JoinCondition { public RegionJoinCondition(Object[] refs) {} "
                        + "public boolean apply(org.apache.flink.table.data.RowData left, org.apache.flink.table.data.RowData right) { return left.getLong(0) == right.getLong(0); }}";
        var join = new StreamingJoinOperator(
                InternalTypeInfo.of(INPUT),
                InternalTypeInfo.of(INPUT),
                new GeneratedJoinCondition("RegionJoinCondition", code, new Object[0]),
                JoinInputSideSpec.withoutUniqueKey(),
                JoinInputSideSpec.withoutUniqueKey(),
                outer,
                outer,
                new boolean[] {true},
                0,
                0);
        var keySelector = org.apache.flink.table.planner.plan.utils.KeySelectorUtil.getRowDataSelector(
                getClass().getClassLoader(), new int[] {0}, InternalTypeInfo.of(INPUT));
        try (var harness = new KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData>(
                join, keySelector, keySelector, keySelector.getProducedType(), 128, 1, 0)) {
            harness.setup(new RowDataSerializer(OUTPUT));
            harness.open();
            List<List<RowData>> result = new ArrayList<>();
            for (Event event : events) {
                for (RowData row : event.rows) {
                    // Flink may mutate the input header while emitting joined rows.
                    StreamRecord<RowData> record = new StreamRecord<>(new RowDataSerializer(INPUT).copy(row));
                    if (event.side == 0) {
                        harness.processElement1(record);
                    } else {
                        harness.processElement2(record);
                    }
                }
                result.add(new ArrayList<>(harness.extractOutputValues()));
                harness.getOutput().clear();
            }
            return result;
        }
    }

    private List<List<RowData>> flinkCalc(List<List<RowData>> events, List<RexNode> projections, RexNode filter)
            throws Exception {
        Transformation<RowData> input = new Transformation<RowData>("join", InternalTypeInfo.of(OUTPUT), 1) {
            @Override
            protected List<Transformation<?>> getTransitivePredecessorsInternal() {
                return List.of(this);
            }

            @Override
            public List<Transformation<?>> getInputs() {
                return List.of();
            }
        };
        var factory = CalcCodeGenerator.generateCalcOperator(
                new CodeGeneratorContext(new Configuration(), getClass().getClassLoader()),
                input,
                OUTPUT,
                scala.collection.JavaConverters.asScalaBufferConverter(projections)
                        .asScala()
                        .toSeq(),
                scala.Option.apply(filter),
                true,
                "JoinRegionCalc");
        try (var harness = new OneInputStreamOperatorTestHarness<RowData, RowData>(factory, 1, 1, 0)) {
            harness.setup(new RowDataSerializer(OUTPUT));
            harness.open();
            List<List<RowData>> result = new ArrayList<>();
            for (List<RowData> rows : events) {
                for (RowData row : rows) {
                    harness.processElement(new StreamRecord<>(row));
                }
                result.add(new ArrayList<>(harness.extractOutputValues()));
                harness.getOutput().clear();
            }
            return result;
        }
    }

    private List<List<RowData>> flinkExpand(List<List<RowData>> events) throws Exception {
        var factory = org.apache.flink.table.planner.codegen.ExpandCodeGenerator.generateExpandOperator(
                new CodeGeneratorContext(new Configuration(), getClass().getClassLoader()),
                OUTPUT,
                OUTPUT,
                List.of(identity, identity),
                true,
                "JoinRegionExpand");
        try (var harness = new OneInputStreamOperatorTestHarness<RowData, RowData>(factory, 1, 1, 0)) {
            harness.setup(new RowDataSerializer(OUTPUT));
            harness.open();
            List<List<RowData>> result = new ArrayList<>();
            for (List<RowData> rows : events) {
                for (RowData row : rows) harness.processElement(new StreamRecord<>(row));
                result.add(new ArrayList<>(harness.extractOutputValues()));
                harness.getOutput().clear();
            }
            return result;
        }
    }

    private RexNode ref(int index) {
        return rex.makeInputRef(types.createFieldTypeFromLogicalType(OUTPUT.getTypeAt(index)), index);
    }

    private static List<Event> events(int seed) {
        Random random = new Random(9127 + seed);
        List<Event> events = new ArrayList<>();
        for (int batch = 0; batch < 6; batch++) {
            List<RowData> rows = new ArrayList<>();
            for (int index = 0; index < 7 + seed; index++) {
                GenericRowData row = GenericRowData.of(
                        (long) random.nextInt(4) - 1,
                        index % 3 == 0 ? null : StringData.fromString("é-" + random.nextInt(4)));
                row.setRowKind(index % 2 == 0 ? RowKind.INSERT : RowKind.UPDATE_AFTER);
                rows.add(row);
            }
            events.add(new Event(batch % 2, rows));
        }
        for (Event inserted : new ArrayList<>(events)) {
            List<RowData> rows = new ArrayList<>();
            for (RowData row : inserted.rows) {
                RowData copy = new RowDataSerializer(INPUT).copy(row);
                copy.setRowKind(row.getRowKind() == RowKind.INSERT ? RowKind.DELETE : RowKind.UPDATE_BEFORE);
                rows.add(copy);
            }
            events.add(new Event(inserted.side, rows));
        }
        if (seed == 0) {
            List<Event> individual = new ArrayList<>();
            for (Event event : events) {
                for (RowData row : event.rows) {
                    individual.add(new Event(event.side, List.of(row)));
                }
            }
            return individual;
        }
        return events;
    }

    private static ArrowExchangeInputBatch input(RootAllocator allocator, List<RowData> rows) {
        BigIntVector keys = new BigIntVector("key", allocator);
        VarCharVector values = new VarCharVector("payload", allocator);
        TinyIntVector kinds = new TinyIntVector("__streamfusion_row_kind", allocator);
        BigIntVector timestamps = new BigIntVector("__streamfusion_timestamp", allocator);
        VectorSchemaRoot root = new VectorSchemaRoot(List.of(keys, values, kinds, timestamps));
        root.allocateNew();
        for (int index = 0; index < rows.size(); index++) {
            RowData row = rows.get(index);
            keys.setSafe(index, row.getLong(0));
            if (row.isNullAt(1)) {
                values.setNull(index);
            } else {
                values.setSafe(index, row.getString(1).toBytes());
            }
            kinds.setSafe(index, row.getRowKind().toByteValue());
            timestamps.setNull(index);
        }
        root.setRowCount(rows.size());
        return new ArrowExchangeInputBatch(root, INPUT);
    }

    private static byte[] bytes(List<RowData> rows) throws Exception {
        List<byte[]> encoded = new ArrayList<>();
        for (RowData row : rows) {
            encoded.add(rowBytes(row));
        }
        return canonical(encoded);
    }

    private static byte[] rowBytes(RowData row) throws Exception {
        DataOutputSerializer bytes = new DataOutputSerializer(64);
        new RowDataSerializer(OUTPUT).serialize(row, bytes);
        return bytes.getCopyOfBuffer();
    }

    private static byte[] canonical(List<byte[]> records) throws Exception {
        records.sort(java.util.Arrays::compareUnsigned);
        DataOutputSerializer bytes = new DataOutputSerializer(1024);
        for (byte[] record : records) {
            bytes.write(record);
        }
        return bytes.getCopyOfBuffer();
    }

    private static long count(List<List<RowData>> events) {
        return events.stream().mapToLong(List::size).sum();
    }

    private static final class Event {
        private final int side;
        private final List<RowData> rows;

        private Event(int side, List<RowData> rows) {
            this.side = side;
            this.rows = rows;
        }
    }
}
