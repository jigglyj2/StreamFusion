/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.arrow;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static tech.streamfusion.flink.arrow.NativeRegionArrowFixtures.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.TestingNativeMemoryManager;
import tech.streamfusion.flink.exchange.ArrowExchangeBatch;
import tech.streamfusion.nativebridge.NativeExecutionContext;
import tech.streamfusion.nativebridge.NativeRegionStream;
import tech.streamfusion.proto.plan.v1.*;

class ArrowNativeRegionStreamTest {
    @Test
    void generated_changelogs_keep_distinct_schemas_zero_copy_payloads_and_release_owners() throws Exception {
        for (int seed : List.of(3, 19, 71)) {
            var memory = TestingNativeMemoryManager.create();
            long available = memory.available();
            try (var allocator = new RootAllocator(64L << 20);
                    var input = input(allocator, seed);
                    var context = NativeExecutionContext.region(plan(), memory, null, null)) {
                var held = new ArrayList<ArrowRowDataBatch>();
                try {
                    for (int invocation = 0; invocation < 3; invocation++) {
                        try (var envelope = ArrowExchangeBatch.withEnvelope(input, TYPE);
                                var array = ArrowArray.allocateNew(allocator);
                                var schema = ArrowSchema.allocateNew(allocator);
                                var dictionaries = new CDataDictionaryProvider()) {
                            Data.exportVectorSchemaRoot(
                                    allocator, envelope.batch().root(), null, array, invocation == 0 ? schema : null);
                            try (var stream = NativeRegionStream.open(
                                    context,
                                    new long[] {array.memoryAddress()},
                                    new long[] {invocation == 0 ? schema.memoryAddress() : 0},
                                    null)) {
                                assertThat(array.snapshot().release).isZero();
                                Schema[] negotiated = new Schema[2];
                                boolean[] seen = new boolean[2];
                                while (true) {
                                    try (var outputArray = ArrowArray.allocateNew(allocator);
                                            var outputSchema = ArrowSchema.allocateNew(allocator)) {
                                        int port =
                                                stream.next(outputArray.memoryAddress(), outputSchema.memoryAddress());
                                        if (port == -1) {
                                            assertThat(outputArray.snapshot().release)
                                                    .isZero();
                                            break;
                                        }
                                        assertThat(port).isBetween(0, 1);
                                        if (negotiated[port] == null) {
                                            assertThat(outputSchema.snapshot().release)
                                                    .isNotZero();
                                            negotiated[port] = Data.importSchema(allocator, outputSchema, dictionaries);
                                        } else
                                            assertThat(outputSchema.snapshot().release)
                                                    .isZero();
                                        var root = VectorSchemaRoot.create(negotiated[port], allocator);
                                        int count = Math.toIntExact(outputArray.snapshot().length);
                                        try {
                                            Data.importIntoVectorSchemaRoot(allocator, outputArray, root, dictionaries);
                                            root.setRowCount(count);
                                        } catch (RuntimeException | Error failure) {
                                            root.close();
                                            throw failure;
                                        }
                                        RowType type = port == 0 ? TYPE : TEXT;
                                        var output = NativePlanOutputEnvelope.read(root, type, allocator)
                                                .batch();
                                        held.add(output);
                                        assertThat(output.size()).isEqualTo(input.size());
                                        assertThat(output.root()
                                                        .getVector(0)
                                                        .getDataBuffer()
                                                        .memoryAddress())
                                                .isEqualTo(input.root()
                                                        .getVector(port == 0 ? 0 : 1)
                                                        .getDataBuffer()
                                                        .memoryAddress());
                                        var serializer = new RowDataSerializer(type);
                                        for (int row = 0; row < input.size(); row++) {
                                            RowData expected = input.rowView(row);
                                            if (port == 1) {
                                                var projected = GenericRowData.of(
                                                        expected.isNullAt(1) ? null : expected.getString(1));
                                                projected.setRowKind(input.rowKind(row));
                                                expected = projected;
                                            }
                                            // Arrow row views expose payload; RowKind lives in the batch envelope.
                                            var actualBytes = serializer
                                                    .toBinaryRow(output.rowView(row))
                                                    .copy();
                                            actualBytes.setRowKind(output.rowKind(row));
                                            var expectedBytes = serializer
                                                    .toBinaryRow(expected)
                                                    .copy();
                                            expectedBytes.setRowKind(input.rowKind(row));
                                            assertThat(actualBytes)
                                                    .as(
                                                            "seed=%s invocation=%s port=%s row=%s",
                                                            seed, invocation, port, row)
                                                    .isEqualTo(expectedBytes);
                                            assertThat(output.rowKind(row)).isEqualTo(input.rowKind(row));
                                            assertThat(output.hasTimestamp(row)).isEqualTo(input.hasTimestamp(row));
                                            if (output.hasTimestamp(row))
                                                assertThat(output.timestamp(row))
                                                        .isEqualTo(input.timestamp(row));
                                        }
                                        seen[port] = true;
                                    }
                                }
                                assertThat(seen).containsExactly(true, true);
                                assertThat(negotiated[0]).isNotEqualTo(negotiated[1]);
                            } finally {
                                if (array.snapshot().release != 0) array.release();
                                if (schema.snapshot().release != 0) schema.release();
                            }
                        }
                        long count = 37L * (invocation + 1);
                        assertThat(context.metricSnapshot())
                                .containsExactly(11, count, count, 12, count, count, 13, count, count);
                    }
                    context.close();
                    for (var output : held) assertThat(output.size()).isEqualTo(37);
                } finally {
                    for (var output : held) output.close();
                }
            }
            assertThat(memory.available()).isEqualTo(available);
        }
    }

    @Test
    void cancellation_and_partial_import_do_not_leak_c_data_handles() throws Exception {
        var memory = TestingNativeMemoryManager.create();
        long available = memory.available();
        try (var allocator = new RootAllocator(64L << 20);
                var input = input(allocator, 3);
                var context = NativeExecutionContext.region(plan(), memory, null, null)) {
            for (int attempt = 0; attempt < 2; attempt++) {
                try (var envelope = ArrowExchangeBatch.withEnvelope(input, TYPE);
                        var array = ArrowArray.allocateNew(allocator);
                        var schema = ArrowSchema.allocateNew(allocator)) {
                    Data.exportVectorSchemaRoot(allocator, envelope.batch().root(), null, array, schema);
                    try {
                        if (attempt == 0) {
                            assertThatThrownBy(() -> NativeRegionStream.open(
                                            context,
                                            new long[] {array.memoryAddress(), 0},
                                            new long[] {schema.memoryAddress(), 0},
                                            null))
                                    .hasMessageContaining("arity");
                            assertThat(array.snapshot().release).isNotZero();
                            assertThat(schema.snapshot().release).isNotZero();
                            NativeRegionStream.open(
                                            context,
                                            new long[] {array.memoryAddress()},
                                            new long[] {schema.memoryAddress()},
                                            null)
                                    .close();
                        } else
                            assertThatThrownBy(() -> NativeRegionStream.open(
                                            context,
                                            new long[] {array.memoryAddress()},
                                            new long[] {schema.memoryAddress()},
                                            null))
                                    .hasMessageContaining("failed");
                    } finally {
                        if (array.snapshot().release != 0) array.release();
                        if (schema.snapshot().release != 0) schema.release();
                    }
                }
            }
        }
        assertThat(memory.available()).isEqualTo(available);
    }
}
