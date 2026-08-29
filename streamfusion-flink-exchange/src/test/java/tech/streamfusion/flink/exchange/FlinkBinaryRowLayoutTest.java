/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.exchange;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.table.data.StringData;
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
}
