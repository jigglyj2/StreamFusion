/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.exchange;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.flatbuffers.FlatBufferBuilder;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.flatbuf.Message;
import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.flatbuf.MetadataVersion;
import org.apache.arrow.flatbuf.RecordBatch;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.TestingNativeMemoryManager;
import tech.streamfusion.flink.arrow.ArrowExchangeCDataBridge;
import tech.streamfusion.flink.arrow.ArrowExchangeInputCDataBridge;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;

class NativeExchangeRowCountTest {
    @Test
    void nativeFramesExposeTheDecodedRowCountWithoutCopyingOrDecodingPayload() {
        var type = RowType.of(new IntType(false), new VarCharType(), new ArrayType(new IntType()));
        var plan = NativeExchangePlanSerializer.hash(type, new int[] {0}, 128);
        for (int count : new int[] {1, 7, 257}) {
            var rows = new ArrayList<GenericRowData>();
            for (int row = 0; row < count; row++)
                rows.add(GenericRowData.of(
                        row % 5,
                        row % 3 == 0 ? null : StringData.fromString("é-" + row),
                        new GenericArrayData(new Integer[] {row, null, -row})));
            var memory = TestingNativeMemoryManager.create();
            try (var allocator = new RootAllocator(8 << 20)) {
                try (var input = ArrowRowDataBatch.transpose(rows, type, allocator);
                        var envelope = ArrowExchangeBatch.withEnvelope(input, type)) {
                    List<NativeExchangeFrame> frames =
                            ArrowExchangeCDataBridge.route(plan, envelope.batch(), allocator, memory);
                    int total = 0;
                    for (var frame : frames) {
                        int logicalRows = frame.logicalRowCount();
                        total += logicalRows;
                        try (var decoded = ArrowExchangeInputCDataBridge.decode(plan, frame, type, allocator, memory)) {
                            assertThat(logicalRows)
                                    .isEqualTo(decoded.arrowBatch().size());
                        }
                        // NativeExchangeFrames uses non-zero offsets into one JNI response.
                        // A detached frame must describe exactly the same rows.
                        var detached = new NativeExchangeFrame(frame.keyGroup(), frame.metadata(), frame.body());
                        assertThat(detached.logicalRowCount()).isEqualTo(logicalRows);
                    }
                    assertThat(total).isEqualTo(count);
                }
                assertThat(allocator.getAllocatedMemory()).isZero();
            }
            assertThat(memory.available()).isEqualTo(memory.limit());
        }
    }

    @Test
    void acceptsZeroRowsAndRejectsInvalidHeaderCountsAndBodyLengths() {
        for (long count : new long[] {0, 1, Integer.MAX_VALUE}) {
            var frame = new NativeExchangeFrame(0, header(count, MessageHeader.RecordBatch, 0), new byte[0]);
            assertThat(frame.logicalRowCount()).isEqualTo((int) count);
        }
        for (long count : new long[] {-1, (long) Integer.MAX_VALUE + 1, Long.MAX_VALUE}) {
            var frame = new NativeExchangeFrame(0, header(count, MessageHeader.RecordBatch, 0), new byte[0]);
            assertThatThrownBy(frame::logicalRowCount).hasMessageContaining("Invalid Arrow IPC");
        }
        for (var frame : List.of(
                new NativeExchangeFrame(0, header(1, MessageHeader.Schema, 0), new byte[0]),
                new NativeExchangeFrame(0, header(1, MessageHeader.RecordBatch, 8), new byte[0]),
                new NativeExchangeFrame(0, header(1, MessageHeader.RecordBatch, -1), new byte[0]),
                new NativeExchangeFrame(0, new byte[0], new byte[0]),
                new NativeExchangeFrame(0, new byte[] {127, 127, 127, 127}, new byte[0]))) {
            assertThatThrownBy(frame::logicalRowCount).hasMessageContaining("Invalid Arrow IPC");
        }
    }

    private static byte[] header(long rows, byte kind, long bodyLength) {
        var builder = new FlatBufferBuilder();
        int batch = RecordBatch.createRecordBatch(builder, rows, 0, 0, 0, 0);
        int message = Message.createMessage(builder, MetadataVersion.V5, kind, batch, bodyLength, 0);
        builder.finish(message);
        return builder.sizedByteArray();
    }
}
