/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.exchange;

import static org.assertj.core.api.Assertions.assertThat;

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
}
