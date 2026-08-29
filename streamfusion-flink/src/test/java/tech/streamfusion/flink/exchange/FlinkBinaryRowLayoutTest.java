/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0.
 */
package tech.streamfusion.flink.exchange;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.binary.BinarySegmentUtils;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;

/** Independently generates the BinaryRow byte fixtures asserted by the Rust encoder. */
class FlinkBinaryRowLayoutTest {
    @Test
    void fixedAndInlineStringLayoutMatchesNativeFixture() {
        BinaryRowData row = new BinaryRowData(2);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.reset();
        writer.writeRowKind(RowKind.INSERT);
        writer.writeInt(0, 42);
        writer.writeString(1, StringData.fromString("abc"));
        writer.complete();

        assertThat(BinarySegmentUtils.copyToBytes(row.getSegments(), row.getOffset(), row.getSizeInBytes()))
                .containsExactly(0, 0, 0, 0, 0, 0, 0, 0, 42, 0, 0, 0, 0, 0, 0, 0, 'a', 'b', 'c', 0, 0, 0, 0, 0x83);
    }

    @Test
    void integerKeysMatchNativeKeyGroupVectors() {
        int[] values = {-1, 0, 1, 42};
        int[] groups = {106, 102, 94, 69};
        for (int index = 0; index < values.length; index++) {
            BinaryRowData row = new BinaryRowData(1);
            BinaryRowWriter writer = new BinaryRowWriter(row);
            writer.reset();
            writer.writeRowKind(RowKind.INSERT);
            writer.writeInt(0, values[index]);
            writer.complete();

            assertThat(KeyGroupRangeAssignment.assignToKeyGroup(row, 128)).isEqualTo(groups[index]);
        }
    }

    @Test
    void temporalAndDecimalLayoutMatchesNativeFixture() {
        BinaryRowData row = new BinaryRowData(4);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.reset();
        writer.writeRowKind(RowKind.INSERT);
        writer.writeInt(0, 20_000);
        writer.writeInt(1, 45_678);
        writer.writeTimestamp(2, TimestampData.fromEpochMillis(-2, 999_000), 6);
        writer.writeDecimal(
                3, DecimalData.fromUnscaledBytes(new byte[] {0, (byte) 0x80, 0, 0, 0, 0, 0, 0, 0}, 25, 0), 25);
        writer.complete();

        byte[] bytes = BinarySegmentUtils.copyToBytes(row.getSegments(), row.getOffset(), row.getSizeInBytes());
        assertThat(bytes).hasSize(64);
        assertThat(littleEndianInt(bytes, 8)).isEqualTo(20_000);
        assertThat(littleEndianInt(bytes, 16)).isEqualTo(45_678);
        assertThat(littleEndianLong(bytes, 24)).isEqualTo((40L << 32) | 999_000);
        assertThat(littleEndianLong(bytes, 40)).isEqualTo(-2);
        assertThat(littleEndianLong(bytes, 32)).isEqualTo((48L << 32) | 9);
        assertThat(bytes).startsWith(new byte[] {0, 0, 0, 0, 0, 0, 0, 0, 0x20, 0x4e, 0, 0});
        assertThat(java.util.Arrays.copyOfRange(bytes, 48, 57)).containsExactly(0, (byte) 0x80, 0, 0, 0, 0, 0, 0, 0);
    }

    private static int littleEndianInt(byte[] bytes, int offset) {
        return java.nio.ByteBuffer.wrap(bytes, offset, Integer.BYTES)
                .order(java.nio.ByteOrder.LITTLE_ENDIAN)
                .getInt();
    }

    private static long littleEndianLong(byte[] bytes, int offset) {
        return java.nio.ByteBuffer.wrap(bytes, offset, Long.BYTES)
                .order(java.nio.ByteOrder.LITTLE_ENDIAN)
                .getLong();
    }
}
