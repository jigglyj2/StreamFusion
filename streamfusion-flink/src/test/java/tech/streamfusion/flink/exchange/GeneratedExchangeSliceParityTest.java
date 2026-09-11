/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.exchange;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.TestingNativeMemoryManager;
import tech.streamfusion.flink.arrow.ArrowExchangeCDataBridge;
import tech.streamfusion.flink.arrow.ArrowExchangeInputCDataBridge;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.nativebridge.NativeExchangeRouter;

class GeneratedExchangeSliceParityTest {
    private static final RowType TYPE = RowType.of(
            new IntType(false),
            new IntType(false),
            new VarCharType(),
            new ArrayType(new IntType()),
            new DecimalType(20, 3));

    @Test
    void contiguousAndScatteredFramesPreserveFlinkBytesAndPerKeyOrder() throws IOException {
        for (int seed = 0; seed < 12; seed++) {
            for (boolean preserveKeyGroups : new boolean[] {false, true}) {
                verify(seed, preserveKeyGroups);
            }
        }
    }

    private static void verify(int seed, boolean preserveKeyGroups) throws IOException {
        Random random = new Random(seed);
        List<GenericRowData> rows = new ArrayList<>();
        RowKind[] kinds = new RowKind[64];
        boolean[] hasTimestamps = new boolean[64];
        long[] timestamps = new long[64];
        Map<Integer, List<byte[]>> expected = new HashMap<>();
        RowDataSerializer serializer = new RowDataSerializer(TYPE);
        for (int index = 0; index < kinds.length; index++) {
            int key = seed % 2 == 0 ? index / 16 : index % 4;
            GenericRowData row = GenericRowData.of(
                    key,
                    index,
                    index % 3 == 0 ? null : StringData.fromString("é-" + random.nextLong()),
                    index % 5 == 0 ? null : new GenericArrayData(new Integer[] {index, null, -index}),
                    index % 7 == 0
                            ? null
                            : DecimalData.fromBigDecimal(BigDecimal.valueOf(random.nextLong(), 3), 20, 3));
            kinds[index] = RowKind.values()[index % 4];
            row.setRowKind(kinds[index]);
            rows.add(row);
            hasTimestamps[index] = index % 2 == 0;
            timestamps[index] = 1000L + index;
            if (index > 0 && index < kinds.length - 1) {
                expected.computeIfAbsent(key, ignored -> new ArrayList<>()).add(bytes(serializer, row));
            }
        }
        byte[] plan = NativeExchangePlanSerializer.hash(TYPE, new int[] {0}, 128, 4, preserveKeyGroups);
        var memory = TestingNativeMemoryManager.create();
        Map<Integer, List<byte[]>> actual = new HashMap<>();
        try (RootAllocator allocator = new RootAllocator();
                ArrowRowDataBatch original = ArrowRowDataBatch.transpose(rows, TYPE, allocator)
                        .withEnvelope(kinds, hasTimestamps, timestamps);
                ArrowRowDataBatch sliced = original.slice(1, rows.size() - 2);
                ArrowExchangeBatch.EnvelopeBatch envelope = ArrowExchangeBatch.withEnvelope(sliced, TYPE);
                NativeExchangeRouter router = new NativeExchangeRouter(plan, memory)) {
            for (NativeExchangeFrame frame : ArrowExchangeCDataBridge.route(router, envelope.batch())) {
                try (ArrowExchangeInputBatch decoded =
                        ArrowExchangeInputCDataBridge.decode(plan, frame, TYPE, allocator, memory)) {
                    for (int index = 0; index < decoded.size(); index++) {
                        RowData row = decoded.rowView(index);
                        int originalIndex = row.getInt(1);
                        assertThat(decoded.hasTimestamp(index)).isEqualTo(hasTimestamps[originalIndex]);
                        assertThat(decoded.timestamp(index))
                                .isEqualTo(hasTimestamps[originalIndex] ? timestamps[originalIndex] : Long.MIN_VALUE);
                        actual.computeIfAbsent(row.getInt(0), ignored -> new ArrayList<>())
                                .add(bytes(serializer, row));
                    }
                }
            }
        }
        assertThat(memory.available()).isEqualTo(memory.limit());
        assertThat(actual.keySet()).isEqualTo(expected.keySet());
        for (var entry : expected.entrySet()) {
            List<byte[]> output = actual.get(entry.getKey());
            assertThat(output).hasSize(entry.getValue().size());
            for (int row = 0; row < output.size(); row++) {
                assertThat(output.get(row))
                        .as("seed %s, key %s, row %s", seed, entry.getKey(), row)
                        .isEqualTo(entry.getValue().get(row));
            }
        }
    }

    private static byte[] bytes(RowDataSerializer serializer, RowData row) throws IOException {
        DataOutputSerializer output = new DataOutputSerializer(128);
        serializer.serialize(row, output);
        return output.getCopyOfBuffer();
    }
}
