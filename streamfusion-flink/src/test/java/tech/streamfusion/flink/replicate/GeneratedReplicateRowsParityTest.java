/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.replicate;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.SpecializedFunction;
import org.apache.flink.table.runtime.functions.table.ReplicateRowsFunction;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.TestingNativeMemoryManager;
import tech.streamfusion.flink.arrow.ArrowCDataBridge;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.NativeCalcResult;
import tech.streamfusion.flink.proto.FlinkLogicalTypeProto;
import tech.streamfusion.nativebridge.NativeExecutionContext;
import tech.streamfusion.proto.plan.v1.Expression;
import tech.streamfusion.proto.plan.v1.InputReference;

/** Changelog parity against Flink's actual built-in set-operation table function. */
class GeneratedReplicateRowsParityTest {
    private static final RowType INPUT =
            RowType.of(new BigIntType(false), new VarCharType(), new ArrayType(new IntType()));
    private static final RowType OUTPUT = RowType.of(
            INPUT.getTypeAt(0), INPUT.getTypeAt(1), INPUT.getTypeAt(2), INPUT.getTypeAt(1), INPUT.getTypeAt(2));

    @Test
    void generatedSkewedNestedRowsMatchFlinkAcrossChunksAndAllRowKinds() throws Exception {
        for (int seed = 0; seed < 3; seed++) {
            List<RowData> inputs = rows(seed);
            List<RowData> expected = flink(inputs);
            DataOutputSerializer actual = new DataOutputSerializer(1024);
            RowDataSerializer serializer = new RowDataSerializer(OUTPUT);
            var manager = TestingNativeMemoryManager.create();
            long available = manager.available();
            int wideChunks = 0;
            byte[] plan = StreamFusionReplicateRowsPlan.create(reference(0), List.of(reference(1), reference(2)));
            try (RootAllocator allocator = new RootAllocator(128L << 20);
                    NativeExecutionContext context = new NativeExecutionContext(plan, manager)) {
                var execution = new ArrowCDataBridge.ReusableExecution(context, OUTPUT, allocator);
                int batchSize = new int[] {7, 53, 83}[seed];
                for (int start = 0; start < inputs.size(); start += batchSize) {
                    List<RowData> rows = inputs.subList(start, Math.min(start + batchSize, inputs.size()));
                    try (ArrowRowDataBatch input = ArrowRowDataBatch.transpose(rows, INPUT, allocator)
                                    .withEnvelope(
                                            rows.stream()
                                                    .map(RowData::getRowKind)
                                                    .toArray(RowKind[]::new),
                                            new boolean[rows.size()],
                                            new long[rows.size()]);
                            var stream = execution.executeStream(input)) {
                        NativeCalcResult next;
                        while ((next = stream.nextWithSelection()) != null) {
                            try (NativeCalcResult result = next) {
                                ArrowRowDataBatch output = result.selectEnvelopeFrom(input);
                                assertThat(output.size()).isLessThanOrEqualTo(16_384);
                                boolean wide = false;
                                for (int row = 0; row < output.size(); row++) {
                                    RowData view = output.rowView(row);
                                    view.setRowKind(output.rowKind(row));
                                    serializer.serialize(view, actual);
                                    wide |= view.getLong(0) == 67;
                                }
                                if (wide) {
                                    wideChunks++;
                                }
                            }
                        }
                    }
                }
                assertThat(context.metricSnapshot())
                        .containsExactly(1, inputs.size(), expected.size(), 2, 0, inputs.size());
            }
            DataOutputSerializer expectedBytes = new DataOutputSerializer(1024);
            for (RowData row : expected) {
                serializer.serialize(row, expectedBytes);
            }
            assertThat(actual.getCopyOfBuffer()).containsExactly(expectedBytes.getCopyOfBuffer());
            assertThat(wideChunks).isGreaterThan(1);
            assertThat(manager.available()).isEqualTo(available);
        }
    }

    private List<RowData> flink(List<RowData> inputs) {
        ClassLoader loader = getClass().getClassLoader();
        CallContext call = (CallContext)
                Proxy.newProxyInstance(loader, new Class<?>[] {CallContext.class}, (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "getArgumentDataTypes":
                            return List.of(DataTypes.BIGINT(), DataTypes.STRING(), DataTypes.ARRAY(DataTypes.INT()));
                        case "getOutputDataType":
                            return Optional.of(DataTypes.ROW(
                                    DataTypes.FIELD("text", DataTypes.STRING()),
                                    DataTypes.FIELD("items", DataTypes.ARRAY(DataTypes.INT()))));
                        default:
                            throw new UnsupportedOperationException(method.getName());
                    }
                });
        var specialized = (SpecializedFunction.SpecializedContext) Proxy.newProxyInstance(
                loader, new Class<?>[] {SpecializedFunction.SpecializedContext.class}, (proxy, method, args) -> {
                    if (method.getName().equals("getCallContext")) {
                        return call;
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
        var function = new ReplicateRowsFunction(specialized);
        List<RowData> expected = new ArrayList<>();
        for (RowData input : inputs) {
            function.setCollector(new Collector<>() {
                @Override
                public void collect(RowData values) {
                    GenericRowData joined = GenericRowData.of(
                            input.getLong(0),
                            input.isNullAt(1) ? null : input.getString(1),
                            input.isNullAt(2) ? null : input.getArray(2),
                            values.isNullAt(0) ? null : values.getString(0),
                            values.isNullAt(1) ? null : values.getArray(1));
                    joined.setRowKind(input.getRowKind());
                    expected.add(new RowDataSerializer(OUTPUT).copy(joined));
                }

                @Override
                public void close() {}
            });
            function.eval(
                    input.getLong(0),
                    input.isNullAt(1) ? null : input.getString(1),
                    input.isNullAt(2) ? null : input.getArray(2));
        }
        return expected;
    }

    private static List<RowData> rows(int seed) {
        Random random = new Random(seed);
        List<RowData> rows = new ArrayList<>();
        for (int row = 0; row < 83; row++) {
            boolean wide = row == 17;
            Integer[] items = new Integer[wide ? 1024 : random.nextInt(5)];
            for (int item = 0; item < items.length; item++) {
                items[item] = item % 3 == 0 ? null : random.nextInt();
            }
            GenericRowData input = GenericRowData.of(
                    wide ? 67L : (long) random.nextInt(6) - 1,
                    row % 7 == 0 ? null : StringData.fromString(wide ? "é".repeat(32 * 1024) : "尾-" + row),
                    row % 5 == 0 ? null : new GenericArrayData(items));
            input.setRowKind(RowKind.values()[(row + seed) % 4]);
            rows.add(input);
        }
        return rows;
    }

    private static Expression reference(int index) {
        return Expression.newBuilder()
                .setInputReference(InputReference.newBuilder()
                        .setIndex(index)
                        .setType(FlinkLogicalTypeProto.serialize(INPUT.getTypeAt(index))))
                .build();
    }
}
